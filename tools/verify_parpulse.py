from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--cargo", default=os.environ.get("CARGO", "cargo"))
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.name):
        parser.error("name must be a simple unique run identifier")
    evidence = ROOT / ".audit" / "verification" / args.name
    evidence.mkdir(parents=True, exist_ok=False)
    result = {"passed": False, "actions": [], "commands": [], "cleanup": {}}
    process = None
    server_log = None
    scratch = None
    try:
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        env.update(
            AWS_EC2_METADATA_DISABLED="true",
            CARGO_BUILD_JOBS="2",
            CARGO_PROFILE_DEV_DEBUG="0",
            CARGO_PROFILE_TEST_DEBUG="0",
        )
        result["sourceCommit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        command = [args.cargo, "build", "--locked", "-p", "storage-node", "--example", "verify_server"]
        result["commands"].append(command)
        with (evidence / "build.log").open("w", encoding="utf-8") as log:
            subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=log,
                           timeout=1800, check=True)
            for arguments in (
                ["-p", "storage-node", "--test", "cache_safety", "--test", "s3_fixture"],
                ["-p", "parpulse-client"],
                ["-p", "storage-node", "--lib", "disk::"],
                ["-p", "storage-node", "--lib", "cache::replacer"],
                ["-p", "storage-node", "--lib", "cache::data_store_cache::sqlite::tests::test_sqlite_store_cache"],
            ):
                command = [args.cargo, "test", "--locked", *arguments]
                result["commands"].append(command)
                subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=log,
                               timeout=1800, check=True)
            command = [args.cargo, "build", "--locked", "-p", "tests", "--example", "decode_probe"]
            result["commands"].append(command)
            subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=log,
                           timeout=1800, check=True)
        target = Path(env.get("CARGO_TARGET_DIR", "target"))
        if not target.is_absolute():
            target = ROOT / target
        suffix = ".exe" if os.name == "nt" else ""
        binary = target / "debug" / "examples" / f"verify_server{suffix}"
        decoder = target / "debug" / "examples" / f"decode_probe{suffix}"
        result["binarySha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        scratch = tempfile.TemporaryDirectory(prefix="parpulse-", dir=ROOT / ".audit")
        scratch_path = Path(scratch.name)
        work = scratch_path / "storage-node"
        fixtures = work / "tests" / "parquet"
        fixtures.mkdir(parents=True)
        source = ROOT / "storage-node" / "tests" / "parquet" / "userdata1.parquet"
        shutil.copyfile(source, fixtures / source.name)
        expected = hashlib.sha256(source.read_bytes()).hexdigest()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        server_log = (evidence / "server.log").open("w", encoding="utf-8")
        command = [str(binary), str(port)]
        result["commands"].append(command)
        process = subprocess.Popen(command, cwd=work, env=env, stdout=server_log,
                                   stderr=server_log)
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                raise RuntimeError("owned server exited before becoming ready")
            try:
                with urllib.request.urlopen(endpoint + "/heartbeat", timeout=1) as response:
                    assert response.status == 200
                break
            except (urllib.error.URLError, TimeoutError):
                if time.monotonic() >= deadline:
                    raise TimeoutError("owned server did not become ready")
                time.sleep(0.1)
        result["actions"].append({"action": "doctor", "pid": process.pid, "loopbackPort": port})
        query = urllib.parse.urlencode(
            {"bucket": "tests-parquet", "keys": source.name, "is_test": "true"}
        )
        for attempt in (1, 2):
            with urllib.request.urlopen(endpoint + "/file?" + query, timeout=20) as response:
                payload = response.read()
                status = response.status
            actual = hashlib.sha256(payload).hexdigest()
            assert status == 200 and actual == expected, "cache response differs from source"
            result["actions"].append({"action": "http-read", "attempt": attempt,
                                      "status": status, "bytes": len(payload), "sha256": actual})
            if attempt == 1:
                (fixtures / source.name).unlink()
                result["actions"].append({"action": "remove-owned-source-fixture", "exists": False})
        for attempt in (1, 2):
            missing = urllib.parse.urlencode({"bucket": "tests-parquet", "keys": "missing.parquet", "is_test": "true"})
            try:
                urllib.request.urlopen(endpoint + "/file?" + missing, timeout=3)
                raise AssertionError("missing source returned success")
            except urllib.error.HTTPError as error:
                assert error.code == 500
                error.close()
            result["actions"].append({"action": "missing-source-retry", "attempt": attempt, "status": 500})
        command = [str(decoder), endpoint]
        result["commands"].append(command)
        decoded = subprocess.run(command, cwd=work, env=env, capture_output=True,
                                 text=True, encoding="utf-8", timeout=30, check=True)
        (evidence / "decode.log").write_text(decoded.stdout + decoded.stderr, encoding="utf-8")
        match = re.search(r"PARQUET_ROWS=(\d+) COLUMNS=13 BATCHES=(\d+)", decoded.stdout)
        assert match and int(match[1]) == 1000 and int(match[2]) > 0, "fixture lost decoded rows"
        result["actions"].append({"action": "parquet-decode", "rows": int(match[1]),
                                  "columns": 13, "batches": int(match[2])})
        result["passed"] = True
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            result["cleanup"]["ownedProcessStopped"] = process is None or process.poll() is not None
            if "port" in locals():
                with socket.socket() as probe:
                    probe.settimeout(1)
                    closed = probe.connect_ex(("127.0.0.1", port)) != 0
                result["cleanup"]["ownedPortClosed"] = closed
                assert closed, "verification port remains occupied after stopping the child"
            if server_log is not None:
                server_log.close()
            if scratch is not None:
                scratch.cleanup()
                assert not Path(scratch.name).exists()
            result["cleanup"]["scratchRemoved"] = True
        except Exception as error:
            result["passed"] = False
            result["cleanup"]["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            result["cleanup"]["evidenceRetained"] = True
            destination = evidence / "result.json"
            destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            assert destination.is_file()
            print(destination)


if __name__ == "__main__":
    main()
