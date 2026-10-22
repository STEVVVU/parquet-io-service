# Feature map

| Feature | Real action and expected state |
| --- | --- |
| HTTP cache | Two actual `/file` requests return matching bytes even after the owned source copy is removed. Missing-source retries each fail promptly. |
| Parquet decoding | The real Rust client consumes the HTTP stream and emits nonzero rows with 13 columns. |
| Isolation and cleanup | An owned loopback child uses a private working directory, exits during cleanup, and leaves retained evidence. |
| Cache safety | Real disk regressions prove atomic no-clobber publication, failed-source cleanup, replacement truncation, pin release and fixture boundaries. |
| Client errors | Corrupt Parquet pages fail the legacy adapter and reach the checked stream as an error. HTTP errors and unknown tables fail explicitly. |
| S3 adapter | The actual AWS SDK performs HEAD, GET and ordered streaming against an owned loopback fixture with dummy credentials; an SDK failure emits one error then EOF. Live AWS remains unverified. |
| Benchmarked HTTP consumer | `python3 tools/benchmark_parpulse.py --output .audit/benchmark/run` uses release `verify_server` and `parpulse-client/examples/benchmark`. It generates deterministic 16-column files with 1k, 100k and 1m rows, row groups 1024/65536, one/all projected columns, first/all row groups and concurrency 1/8. Five rotated cache-empty-start, primed-cache and direct-file sides validate every decoded value, count HTTP bytes, outcomes, tails and generator/server resources. The larger selected case also has offered-rate and 60-second stability trials. Projection happens after full HTTP transfer in this consumer; the public checked client still decodes all columns. |
