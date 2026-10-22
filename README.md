# Parquet I/O service

An asynchronous object-to-Arrow I/O service based on the full MIT-licensed CMU 15-721 Spring 2024 Parpulse capstone. It fetches remote objects through the AWS S3 SDK, caches bytes locally, serves an HTTP stream, and decodes Parquet into Arrow batches for the original OLAP client interface.

The [official showcase](https://15721.courses.cs.cmu.edu/spring2024/project-showcase.html) links [cmu-db/15721-s24-cache1](https://github.com/cmu-db/15721-s24-cache1). This repository retains its complete history, authors and [MIT license](LICENSE), starting at `b4e2bc8f2c2fc3ab7a9b9fa3f8e864e25e9c8c40`. [README.upstream.md](README.upstream.md) preserves the original README. This is an adaptation of that project, not an original-authorship or course-completion claim.

## Run locally

Requires Rust 1.99.1, Python 3.11 or later, and a C/C++ toolchain. Windows needs the MSVC Build Tools. SQLite is built from its bundled source. The application lockfile pins dependencies, including Chrono 0.4.34 for Arrow 50 compatibility.

```powershell
cargo build --locked --workspace --jobs 2
python tools/verify_parpulse.py local
```

The verification command is the self-contained demonstration. It starts the actual service on an unused loopback port, serves the shipped Parquet fixture, proves a cache hit after removing the owned source copy, and runs the actual Rust client. The expected decoded result is 1,000 rows and 13 columns. It also drives missing-source retries, offline regression tests, and the real AWS SDK against an owned local HTTP fixture. It uses dummy fixture credentials and makes no AWS request.

For an interactive local service, run from the storage-node directory so the inherited fixture layout resolves correctly:

```powershell
cd storage-node
cargo run --locked --bin storage_node -- --data-store disk
```

The binary binds `127.0.0.1:3030`. `GET /heartbeat` returns 200. `GET /file?bucket=tests-parquet&keys=userdata1.parquet&is_test=true` serves the local fixture. Without `is_test=true`, the same endpoint reads one exact object key through the S3 SDK's normal credential and region configuration. The HTTP endpoint accepts one object, not a comma-separated object list. The lower-level S3 reader supports ordered multi-object streams.

Use only empty, disposable cache paths. The inherited disk and SQLite stores delete their cache paths on drop. They do not reconstruct cache indexes after restart. This service has no authentication and is not a public deployment configuration.

## Changes in this adaptation

- Cache identity hashes the source kind, bucket, object count and length-delimited keys. Delimiter collisions, mock/remote collisions and request-derived cache filenames are eliminated.
- Disk publication writes an owned same-directory temporary file, flushes it and publishes without replacing an existing winner. A failed source leaves no partial final object; concurrent publication has one winner. Replacement writes truncate an old tail.
- Cancelled disk streams stop sending and release their cache pin. Stream errors terminate after one error. S3 failures no longer poll a completed SDK future again.
- Local fixture access resolves canonical paths inside the shipped Parquet or text fixture directories. Traversal to project source is rejected, and failed source initialization clears pending-cache state.
- The client adds `request_object_checked(RequestParams)`, returning a bounded `Receiver<Result<RecordBatch>>`. Decoder failures reach the consumer; dropping the receiver stops decoding. Blocking Parquet work runs on Tokio's blocking pool.
- The unchanged external `StorageClient` trait cannot express late stream errors. Its adapter validates all batches before returning a successful stream, which uses memory proportional to the decoded object. Its collecting method is implemented; unknown table IDs return errors.
- Normal startup binds loopback. The real-product driver owns and cleans up its processes, sockets and scratch files. Hosted CI uses no cloud credentials or coverage token.

The upstream cache policies, memory/disk and SQLite stores, AWS adapter, request types and OLAP integration remain available. `S3Reader::with_client` allows a configured SDK client, including a local test endpoint, without changing the default AWS constructor.

## Checked client

```rust
use parpulse_client::{client::StorageClientImpl, RequestParams};

let client = StorageClientImpl::new("http://127.0.0.1:3030", "http://127.0.0.1:3031")?;
let request = RequestParams::MockS3(("tests-parquet".into(), vec!["userdata1.parquet".into()]));
let mut batches = client.request_object_checked(request).await?;
while let Some(batch) = batches.recv().await {
    let batch = batch?;
    println!("{} rows", batch.num_rows());
}
```

The object downloads into a temporary file before decoding because Parquet metadata is at the end of the file. The checked API bounds its decoded-batch channel to 32 batches. It does not impose a total object-size limit. The catalog endpoint remains a placeholder and the legacy table API uses the original fixed table map.

## Verification and limits

```powershell
cargo fmt --all -- --check
cargo test --locked --workspace --no-run
cargo clippy --locked --all-targets --all-features
python tools/verify_parpulse.py verification
```

The [verification skill](.agents/skills/verify-parpulse/SKILL.md) and feature map define the real-product checks. Each run needs a new name. Logs, commands, executable hash, HTTP hashes, decoded rows and observed cleanup remain under ignored `.audit/verification/<name>/`. Maintained tooling is under `tools/` and the two Rust examples.

Regression evidence covers ambiguous cache keys, failed downloads, concurrent disk publication, replacement truncation, cancellation, fixture traversal, corrupt Parquet pages, HTTP errors, unknown tables, and SDK HEAD/GET/stream/error behavior on loopback. Existing disk, cache-replacement, SQLite and client tests run without AWS. The rest of the inherited workspace is compiled; its cloud and timing-sensitive tests are not run by this offline driver.

Live AWS compatibility, credentialed access, durable cache restart, crash/power-loss durability and multi-machine OLAP deployment remain unverified. The loopback throughput results below are separate from upstream capstone numbers. The inherited synchronous disk helper and broader cache replacement/concurrency design remain teaching code. Clippy reports inherited warnings visibly. The old public source history includes a coverage-service credential; the active workflow replaces that configuration and never uses it.

## Reproduce cache and Parquet measurements

The [complete measured baseline](docs/benchmarks/2026-10-22-baseline.md) covers 753 trials on a four-logical-CPU AMD EPYC 7763 Ubuntu runner with Rust 1.99.1 release builds. For a 16 MB, 100k-row object at 50 offers/s and eight consumer permits, warm-cache throughput was 32.69 successful/s [32.35, 33.10], with 169 [164, 172] of 500 offers rejected; successful-response p99 was 259.12 ms [257.94, 264.68]. Direct-file decode completed 49.99/s [49.99, 50.00] with no rejection. Values are medians and ranges of five alternating trials. All 97,260 offers were retained: 33,551 successes, 63,709 consumer-permit rejections and zero errors. The full matrix, 60-second stability results, sampled resources, commands and raw artifact link are in the report. Download/temp-file work dominates this path; a separate profile is pending before any buffering fix.

Build `cargo build --release --locked --jobs 2 -p storage-node --example verify_server -p parpulse-client --example benchmark`, then run `python3 tools/benchmark_parpulse.py --output .audit/benchmark/run` on an isolated Linux host. The manual [benchmark workflow](.github/workflows/benchmark.yml) retains raw request CSVs, fixture/binary hashes, commands, resource samples and outcomes.

The driver uses the actual AWS SDK against an owned loopback HTTP object origin with dummy credentials and counted HEAD/GET bytes. It compares a new empty disk cache, a primed cache and a direct-file consumer on identical generated Parquet objects. It sweeps 1k/100k/1m rows, physical row groups, projected columns and concurrency, with five alternating comparisons plus selected sustained offered-rate and stability trials. Every decoded cell must match its deterministic source value. This measures a loopback origin, not AWS latency or bandwidth. It never uses the artificial-delay MockS3 reader for timing.

Projection and row-group selection occur in the benchmark consumer after a full HTTP download. The public checked client still decodes all columns. These runs therefore measure that limitation and cannot claim server pushdown or network-byte savings. Cache-empty means an empty application cache, not an evicted host page cache. Setup smoke timings are not benchmark results.
