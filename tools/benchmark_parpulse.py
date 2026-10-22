"""Real HTTP cache plus a Parquet consumer. Generates bounded files outside timing."""
import argparse
import contextlib
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

ROOT = Path(__file__).resolve().parents[1]


def percentile(values, fraction):
    return sorted(values)[int((len(values)-1)*fraction)] if values else None


def resources(process):
    pid = process if isinstance(process,int) else process.pid
    try:
        base = Path(f"/proc/{pid}")
        fields = (base / "stat").read_text().split(")", 1)[1].split()
        return {"pid":pid,"user_ticks":int(fields[11]),"sys_ticks":int(fields[12]),"rss_pages":int(fields[21]),"io":(base/"io").read_text(),"status":(base/"status").read_text()}
    except (FileNotFoundError, PermissionError):
        return {"pid":pid,"exit":None if isinstance(process,int) else process.poll()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--profile", choices=["server", "consumer"])
    parser.add_argument("--binary-dir", type=Path, default=ROOT / "target/release/examples")
    args = parser.parse_args()
    if args.profile and (args.smoke or os.name != "posix"):
        parser.error("separate syscall profiles require Linux and cannot use --smoke")
    if shutil.disk_usage(ROOT).free < 10 * 1024**3:
        raise RuntimeError("10 GiB disk reserve required")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    suffix = ".exe" if os.name == "nt" else ""
    server = args.binary_dir.resolve() / f"verify_server{suffix}"
    consumer = args.binary_dir.resolve() / f"benchmark{suffix}"
    result = {"source_sha":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),"platform":platform.platform(),"cpu_count":os.cpu_count(),"cpu":Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else platform.processor(),"load":os.getloadavg() if hasattr(os,"getloadavg") else None,"binary_sha256":{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [server,consumer]},"samples":[],"passed":False,"policy":"512 MiB disk cache; new process and disposable empty cache per side. Cold is cache-empty at offered-load start, not OS-page-cache eviction. Warm is primed before timing. Full object HTTP download then projected/row-group Parquet decode. The public checked client still exposes full-column decode; the consumer uses the real /file API and the same reqwest/tempfile/parquet libraries. file baseline decodes the identical on-disk object with no HTTP/cache. All decoded values validated. Projection is client-side and does not save HTTP bytes."}
    result["profile_target"] = args.profile
    if args.profile:
        result["strace_version"] = subprocess.check_output(["strace", "--version"], text=True).splitlines()[0]
        result["diagnostic_policy"] = "One frozen 100k/65536/16-column/all-groups/eight-permit/50-per-second/10s case per side. Instrumented times are not measurements. strace PID resource samples describe the tracer, not target high-water marks. --kill-on-exit prevents orphaned tracees; -I2 allows owned shutdown."
    owned, logs = [], []
    origin = None
    origin_thread = None
    origin_lock = threading.Lock()
    origin_counts = {"head":0,"get":0,"bytes":0}
    active_fixture = None

    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    @contextlib.contextmanager
    def workspace():
        directory = tempfile.TemporaryDirectory(prefix="parpulse-benchmark-")
        try:
            yield directory.name
        finally:
            for process in owned:
                stop(process)
            directory.cleanup()

    class Origin(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body):
            if self.path.split("?")[0] != "/fixture/bench.parquet" or active_fixture is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(active_fixture.stat().st_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            with origin_lock:
                origin_counts["get" if body else "head"] += 1
            if body:
                with active_fixture.open("rb") as source:
                    while chunk := source.read(65536):
                        self.wfile.write(chunk)
                        with origin_lock:
                            origin_counts["bytes"] += len(chunk)
    try:
        with workspace() as scratch:
            work = Path(scratch)
            fixtures = work / "tests/parquet"
            fixtures.mkdir(parents=True)
            env = {k:v for k,v in os.environ.items() if not k.startswith("AWS_")}
            origin = ThreadingHTTPServer(("127.0.0.1",0),Origin)
            origin_thread = threading.Thread(target=origin.serve_forever,daemon=True)
            origin_thread.start()
            env.update(AWS_EC2_METADATA_DISABLED="true",AWS_ACCESS_KEY_ID="owned-fixture",AWS_SECRET_ACCESS_KEY="owned-fixture",AWS_REGION="us-east-1",AWS_ENDPOINT_URL_S3=f"http://127.0.0.1:{origin.server_port}",AWS_MAX_ATTEMPTS="1")
            sizes = [100000] if args.profile else [1000] if args.smoke else [1000,100000,1000000]
            for rows in sizes:
                for row_group in ([65536] if args.profile else [1024] if args.smoke else [1024,65536]):
                    fixture = fixtures / "bench.parquet"
                    subprocess.run([str(consumer),"generate",str(fixture),str(rows),str(row_group)],env=env,check=True,timeout=120)
                    fixture_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
                    if args.profile:
                        assert fixture_hash == "6045a6913d1adae6b4d634a4129be1db7ec487b0849416d03a7585223e08f527", "original profiled fixture changed"
                    active_fixture = fixture
                    for columns in ([16] if args.profile else [1] if args.smoke else [1,16]):
                        for group in (["all"] if args.smoke or args.profile else ["all","first"]):
                            for concurrency in ([8] if args.profile else [1] if args.smoke else [1,8]):
                                # shortcut: bursts cover the full matrix; sustained rates below cover one mapped large-file case.
                                trials = [(1,1)] if args.smoke else [(8,1)]
                                if not args.smoke and rows == 100000 and row_group == 65536 and columns == 16 and group == "all" and concurrency == 8:
                                    trials += [(50,10),(500,10),(50,60)]
                                if args.profile:
                                    trials = [(50,10)]
                                for rate,duration in trials:
                                    for repeat in range(1 if args.smoke or args.profile or duration == 60 else 5):
                                        sides = ["cold","warm","file"]
                                        if repeat % 2:
                                            sides.reverse()
                                        for side in sides:
                                            label = f"n{rows}-rg{row_group}-c{columns}-{group}-q{concurrency}-rate{rate}-s{duration}-rep{repeat}-{side}"
                                            process = None
                                            trial = tempfile.TemporaryDirectory(prefix="trial-", dir=work)
                                            trial_path = Path(trial.name)
                                            if side == "file":
                                                source = "file://" + str(fixture)
                                            else:
                                                with socket.socket() as reservation:
                                                    reservation.bind(("127.0.0.1",0))
                                                    port = reservation.getsockname()[1]
                                                log = (args.output / f"{label}.server.log").open("wb")
                                                logs.append(log)
                                                server_command = [str(server),str(port),str(512*1024*1024)]
                                                if args.profile == "server":
                                                    server_command = ["strace","-f","-c","-I2","--kill-on-exit","-o",str(args.output/f"{label}.server.strace"),*server_command]
                                                process = subprocess.Popen(server_command,cwd=trial_path,env=env,stdout=log,stderr=log)
                                                owned.append(process)
                                                endpoint = f"http://127.0.0.1:{port}"
                                                deadline = time.monotonic()+15
                                                while True:
                                                    if process.poll() is not None:
                                                        raise RuntimeError("owned storage server exited before readiness")
                                                    try:
                                                        with urllib.request.urlopen(endpoint+"/heartbeat",timeout=1) as response:
                                                            assert response.status == 200
                                                        break
                                                    except OSError:
                                                        if time.monotonic() >= deadline:
                                                            raise TimeoutError("storage server readiness failed")
                                                        time.sleep(.1)
                                                source = endpoint+"/file?bucket=fixture&keys=bench.parquet"
                                                if side == "warm":
                                                    with urllib.request.urlopen(source,timeout=30) as response:
                                                        assert hashlib.sha256(response.read()).hexdigest() == fixture_hash
                                            raw = args.output / f"{label}.csv"
                                            with origin_lock:
                                                source_before = origin_counts.copy()
                                            command = [str(consumer),"read",source,str(rows),str(row_group),str(columns),group,str(concurrency),str(rate),str(duration),"--out",str(raw)]
                                            if args.profile == "consumer":
                                                command = ["strace","-f","-c","-I2","--kill-on-exit","-o",str(args.output/f"{label}.consumer.strace"),*command]
                                            log = (args.output / f"{label}.client.log").open("wb")
                                            logs.append(log)
                                            client = subprocess.Popen(command,cwd=work,env=env,stdout=log,stderr=log)
                                            owned.append(client)
                                            samples = []
                                            deadline = time.monotonic()+duration+45
                                            while client.poll() is None:
                                                if time.monotonic() >= deadline:
                                                    raise TimeoutError(f"client exceeded bounded deadline {label}")
                                                samples.append({"time":time.monotonic(),"server":resources(process) if process else None,"generator":resources(client),"origin":resources(os.getpid())})
                                                time.sleep(.25)
                                            log.flush()
                                            if client.returncode:
                                                raise RuntimeError(f"client/decode failed {label}")
                                            values = list(csv.DictReader(raw.open()))
                                            ok = [s for s in values if s["status"] == "ok"]
                                            rejected = sum(s["status"] == "rejected" for s in values)
                                            assert len(values) == rate*duration and len(ok)+rejected == len(values)
                                            assert ok, "all requests rejected"
                                            elapsed = [int(s["elapsed_us"])/1000 for s in ok]
                                            lag = [(int(s["started_us"])-int(s["scheduled_us"]))/1000 for s in values]
                                            summary = re.search(r"wall_us=(\d+)", (args.output/f"{label}.client.log").read_text())
                                            wall = int(summary[1])/1e6
                                            sample = {"label":label,"side":side,"rows":rows,"row_group":row_group,"columns":columns,"group":group,"concurrency":concurrency,"rate":rate,"duration":duration,"repeat":repeat,"offered":len(values),"successful":len(ok),"rejected":rejected,"errors":0,"wall_s":wall,"successful_per_s":len(ok)/wall,"http_bytes":sum(int(s["http_bytes"]) for s in ok),"p50_ms":percentile(elapsed,.5),"p95_ms":percentile(elapsed,.95),"p99_ms":percentile(elapsed,.99),"download_p50_ms":percentile([int(s["download_us"])/1000 for s in ok],.5),"decode_p50_ms":percentile([int(s["decode_us"])/1000 for s in ok],.5),"generator_lag_p99_ms":percentile(lag,.99),"fixture_sha256":fixture_hash,"fixture_bytes":fixture.stat().st_size,"command":command,"resources":samples}
                                            result["samples"].append(sample)
                                            with origin_lock:
                                                sample["origin"] = {key:origin_counts[key]-source_before[key] for key in origin_counts}
                                            if side in ["warm","file"]:
                                                assert sample["origin"]["get"] == 0, "warmed/direct read fetched origin data"
                                            else:
                                                assert sample["origin"]["get"] >= 1, "cold read did not reach actual SDK origin"
                                            sample["final_server_resources"] = resources(process) if process else None
                                            (args.output/"result.json").write_text(json.dumps(result,indent=2))
                                            if process:
                                                stop(process)
                                            trial.cleanup()
                                            print(label,flush=True)
            result["passed"] = True
    finally:
        for process in owned:
            stop(process)
        for log in logs:
            log.close()
        if origin is not None:
            origin.shutdown()
            origin.server_close()
        if origin_thread is not None:
            origin_thread.join(timeout=5)
        result["origin_totals"] = origin_counts
        result["cleanup"] = {"owned_processes_stopped":all(p.poll() is not None for p in owned),"scratch_removed":not Path(scratch).exists() if "scratch" in locals() else True}
        (args.output/"result.json").write_text(json.dumps(result,indent=2)+"\n")


if __name__ == "__main__":
    main()
