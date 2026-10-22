---
name: verify-parpulse
description: Build and drive Parpulse's real loopback HTTP cache and Rust Parquet client with retained byte, decode and cleanup evidence.
---

# Verify Parpulse

Status: proven on Windows MSVC with the baseline-bundled run. It fetched matching 113,629-byte responses twice, decoded 1,000 rows and 13 columns, and retained evidence after process, port and scratch cleanup. Existing AWS-dependent tests require external infrastructure and must not use ambient credentials.

## Launch

Run `python tools/verify_parpulse.py <unique-run-name>` from the repository root with Rust 1.99.1 and its MSVC prerequisites on PATH. The maintained driver builds `storage-node/examples/verify_server.rs` and `tests/examples/decode_probe.rs`, then launches the actual storage service on an unused loopback port in an owned temporary directory. It copies only the shipped `userdata1.parquet` fixture into that directory's source tree. No public cloud connection is made.

## Doctor

Require the owned child to remain alive, `/heartbeat` to return 200, and the compiled executable to have a retained SHA-256. Refuse an existing evidence directory. The driver disables EC2 metadata access and removes ambient AWS credentials from the child environment.

## Drive

Run the maintained offline tests, including the actual AWS SDK against an owned loopback fixture with dummy credentials. Request the actual `/file` fixture API twice, removing the owned source copy after the first response. Require both complete response streams to match the source hash, proving a cache hit. Request a missing object twice and require prompt failures rather than a stuck pending entry. Run the checked Rust Parquet client and require 1,000 rows with 13 columns. These checks prove local cache/HTTP/decode behavior and SDK HEAD/GET/stream/error handling. Live AWS, throughput and durable cache reuse after restart remain unverified.

## Evidence
For performance work, build `cargo build --release --locked --jobs 2 -p storage-node --example verify_server -p parpulse-client --example benchmark`, then run `python3 tools/benchmark_parpulse.py --output .audit/benchmark/run` on an isolated Linux host. The workflow `.github/workflows/benchmark.yml` preserves raw request CSVs, exact fixture hashes, commands, response/decode counts, bytes and resource samples. The actual AWS SDK reads an owned loopback HTTP origin with dummy credentials. Origin HEAD/GET/byte counters prove cold fetches and zero-origin warm reads. The artificial-delay MockS3 path is excluded. `--smoke` is setup validation only. Cache-empty means a new owned disk cache, not host page-cache eviction. The consumer projects columns and row groups after downloading the whole object, so this does not claim server-side pushdown or lower network bytes. No live AWS is used. Profile actual server and consumer separately before any performance fix.


Retain commands, source commit, binary hash, server log, request status and byte hashes, decoded row output, and cleanup state in `.audit/verification/<run-name>/`. Timing fields are diagnostics, not a performance comparison. Source fixtures and credentials are not copied into evidence.

## Cleanup

Stop only the child created by this driver, await its exit, and remove only its temporary working directory. Keep the evidence directory and require `result.json` to survive cleanup. Never use a process-name kill or the app's default cache path.

See [the feature map](features/README.md).
