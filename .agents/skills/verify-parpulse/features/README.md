# Feature map

| Feature | Real action and expected state |
| --- | --- |
| HTTP cache | Two actual `/file` requests return matching bytes even after the owned source copy is removed. Missing-source retries each fail promptly. |
| Parquet decoding | The real Rust client consumes the HTTP stream and emits nonzero rows with 13 columns. |
| Isolation and cleanup | An owned loopback child uses a private working directory, exits during cleanup, and leaves retained evidence. |
| Cache safety | Real disk regressions prove atomic no-clobber publication, failed-source cleanup, replacement truncation, pin release and fixture boundaries. |
| Client errors | Corrupt Parquet pages fail the legacy adapter and reach the checked stream as an error. HTTP errors and unknown tables fail explicitly. |
| S3 adapter | The actual AWS SDK performs HEAD, GET and ordered streaming against an owned loopback fixture with dummy credentials; an SDK failure emits one error then EOF. Live AWS remains unverified. |
