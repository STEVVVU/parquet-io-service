# Matched Parquet write-buffer comparison

[Hosted run 39740738996](https://github.com/STEVVVU/parquet-io-service/actions/runs/39740738996) compared frozen baseline `9a263808c1a8e6f46b6305881fe8a74a31744f01` with candidate `6a222067f7b53553cf192efdf322f2343ce4a71d`. Both release consumers used Rust 1.99.1, the same dependency lockfile, fixtures, loopback origin and server binary on a four-logical-CPU AMD EPYC 7763 runner. The candidate adds a 256 KiB Tokio `BufWriter` to the checked client's HTTP spool and the benchmark consumer's HTTP spool, then flushes before decoding.

For the 100k-row, 16-column, all-row-group workload at eight consumer permits and 50 offers/s, warm-cache median throughput rose from **31.93 [31.61, 32.60] to 43.82 [43.57, 44.20] successful requests/s** across five runs. Successful-response p99 fell from **270.47 [262.83, 272.34] ms to 189.27 [187.71, 208.80] ms**. Each run offered 500 requests. Baseline accepted 321–331 and rejected 169–179; candidate accepted 440–446 and rejected 54–60. The five per-trial ranges do not overlap. Cold-cache throughput rose from 32.61 [29.78, 33.14] to 43.75 [43.03, 44.93] requests/s, with p99 falling from 262.46 [259.42, 326.11] to 193.63 [181.74, 222.59] ms.

At 500 offers/s, the same warm workload completed 32.45 [32.09, 32.89] baseline requests/s and 44.81 [43.92, 44.98] candidate requests/s. The eight-permit cap rejected most offers in both builds: 4,666–4,675 of 5,000 for baseline and 4,545–4,557 for candidate. This is a saturated consumer path, not a service-capacity estimate. The 60-second 50 offers/s stability runs were single samples. They recorded 32.97 baseline and 44.04 candidate warm-cache requests/s, with p99 of 261.63 and 190.68 ms. Those singles have no run-to-run range.

## Coverage and correctness

The result contains 1,506 unique CSV trials: 753 per build, with 251 each for cold cache, warm cache and direct-file decode. Each side includes the 48 original one-second matrix settings and two ten-second sustained settings, each repeated five times in reversed order, plus the original 60-second single. Warm-cache setup fetched and hashed the object before the timed request. Cold cache means an empty application cache; the runner's operating-system page cache was not evicted.

The 1,506 CSVs contain 70,059 successful requests, 124,461 concurrency rejections and zero request errors. All 753 matched trial pairs retained the same expected workload and fixture. The consumer checks each decoded value against its deterministic formula before writing a successful CSV row. I independently checked every CSV's offered count, unique indices, status counts, expected selected row count, expected value sum, and HTTP byte count. All 23,104 successful request indices shared by both builds produced identical rows, sums and byte counts. Different requests can finish before the eight-permit cap in each build, so 111 of 753 matched trial pairs accepted different offer indices; rejected offers remain counted and have no latency values.

The hosted offline verifier passed before measurement. It exercised the valid 113,629-byte Parquet response, which is smaller than the buffer, along with cache reuse, missing-source retries and corrupt/error cases. Every measured cold-cache request reached the owned loopback origin; warm-cache and direct-file requests fetched no origin data. The result records 1,004 origin GETs and 55,783,936,124 origin bytes across both builds. All decoded values passed the consumer's oracle.

## Regressions and limits

The direct-file arm bypasses the HTTP spool, so it is a control for the buffering change. Its five-run median decode p50 was unchanged for the representative 100k-row workload: 12.310 [9.420, 13.060] ms baseline and 12.354 [8.164, 13.046] ms candidate. One direct-file cell regressed with non-overlapping five-run ranges: at one million rows, a 65,536-row group, one column, all groups, one permit and eight offers/s, decode p50 was 4.876 [4.823, 4.919] ms baseline and 4.970 [4.928, 5.030] ms candidate, a 0.094 ms increase. The direct-file path does not use the new buffer, and this small difference does not explain or offset the HTTP result. The 60-second direct-file single also measured 10.133 ms baseline and 12.858 ms candidate p50; one sample cannot establish a repeatable regression.

Across the 50 five-run cells per side, candidate median p50 was higher in two cold-cache and three warm-cache HTTP cells; candidate median p99 was higher in four cold-cache and two warm-cache cells. Every HTTP cell in those groups had overlapping five-run ranges. Direct-file median p50 was higher in 23 cells and median p99 in 29 cells, but only the one million-row p50 cell above had non-overlapping ranges. These counts include the one-second burst matrix, where most cells offer only eight requests per trial.

The separate strace profile of the frozen baseline counted 229,415 consumer `write` calls for 111 successful warm responses, about 2,066 writes per response. It supports the small-write bottleneck hypothesis. The trace was instrumented and is not timing evidence. The matched run shows that the candidate moves the end-to-end loopback workload, but it does not isolate local disk throughput. The 16 MB object fits in the 512 MiB application cache, and Linux page caching remains active; local writes may complete to page cache. Do not treat these rates as AWS, network or physical-disk performance. The direct-file arm also confirms that buffering did not improve Parquet decode itself.

The run retained periodic `/proc` resource snapshots, not process high-water marks. Across the full campaign, the largest sampled consumer RSS was 282.38 MiB for baseline and 197.04 MiB for candidate; the server maximum was 53.58 MiB and the origin maximum was 188.15 MiB. These values span different trial workloads and do not establish a memory reduction. The original single-build baseline remains in [2026-10-22-baseline.md](2026-10-22-baseline.md), and the separate syscall profiles are documented there.

## Artifact and build identity

GitHub reports artifact `parquet-compare-39740738996` at 6,341,113 compressed bytes with digest `sha256:75efbe7d21fcc8b137cf4b174729deba5a2a86269bdd6c48a6003be75843bfa0`. The extracted artifact has 4,022 files totaling 77,848,277 bytes. Its paths remain under the extraction directory. The result manifest SHA-256 is `4361438116f0f8b9453c7ec0454b4a33804388a64bd04db7e155b10d8a09fbd1`. The retained files are under `C:\Users\steve\.codex\orchestrate\metrics30-20261022\evidence\P12-matched-39740738996`.

The baseline archive SHA-256 is `afc0b888ac12ee1f73be36a573b79c21e992359e32c558c300176e4f18db8dcc`. The independently built consumer hashes differ: baseline `a59e3cf29eefb4a39ef1225ce732046efa947f4cfbc4ce9da56ca513a5e2e161`; candidate `c7cc12da3a1a839428c59c4d906128903d90cb2c640491167fa687024bca7b51`. The artifact's eight source hashes were checked against their Git blobs. Only the two buffering sources differ; manifests, lockfile and server sources match.

| Fixture rows | Row-group limit | Bytes | SHA-256 |
|---:|---:|---:|---|
| 1,000 | 1,024 | 153,911 | `1f579c89986b803ac10fb32cfa1a17befc08e4a12e8ff8e59b5703a4f74c142f` |
| 1,000 | 65,536 | 153,911 | `1f579c89986b803ac10fb32cfa1a17befc08e4a12e8ff8e59b5703a4f74c142f` |
| 100,000 | 1,024 | 15,257,364 | `e2b0f533a3a82835a5aed27de7334857c279dbb5408f94c2d7b0f4097ec1229d` |
| 100,000 | 65,536 | 16,014,061 | `6045a6913d1adae6b4d634a4129be1db7ec487b0849416d03a7585223e08f527` |
| 1,000,000 | 1,024 | 152,590,689 | `b1979730ae73002d59f6c8891c5626be5686a139f3339ba127cfadaae1d6faa7` |
| 1,000,000 | 65,536 | 160,075,798 | `20cdbebf1488f0b0f65655b157d7d46a24a4f45f80c56d94b0d075a8ce7d6ced` |

| Source | Baseline SHA-256 | Candidate SHA-256 |
|---|---|---|
| `storage-client/src/client.rs` | `37dbd49d3eada7158c8b712753f9baa0ab6b723a0d426f8071b01fcaba449ef6` | `1abd77ca7706dcff60a2eb023b27309a2ab0f1d6fc2f475d159f22543bee4064` |
| `storage-client/examples/benchmark.rs` | `cf7992cf278c5e92893ded888bb6f3f05ac7264dd9b61e58f788880f520612a9` | `3e6e4bb3f47956229e578abc2ddef39c814e6eeb934757b62a81304652ecd8df` |
| `Cargo.toml` | `6b6a9dd7dc7e4a35e886701f07eff604808f22f2ff69c8eb46e94215805313b5` | same |
| `Cargo.lock` | `c6c2a620edbdb70639f755ae41bad570d45a48fdd52e6c7fe37b2b911b65bb39` | same |
| `storage-client/Cargo.toml` | `1db632eb09ae24a58810b5d504c09d6bc90bd3ce03d903c90835f074a9651cbd` | same |
| `storage-node/Cargo.toml` | `d260ffe7975f3d94ec962bdb474bad657d17b3fab9ca5a07d52208d78f40e51d` | same |
| `storage-node/examples/verify_server.rs` | `d980d96c23ccea383fce7edad2263b4fc8a0136e4d1800a4d3021b1484d7c6b7` | same |
| `storage-node/src/server.rs` | `8e512c1b5b92bc1d5fbe2bab701120f3d86edde7b3cd3b4480daccf3f0dddecc` | same |
