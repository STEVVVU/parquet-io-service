# Parpulse I/O fork

Retain the full CMU 15-721 Spring 2024 Parpulse source, history and MIT notices. The root README must distinguish upstream mechanisms from this fork's changes. Do not copy upstream performance claims into local results.

Use Rust 1.99.1. Commands are `cargo test --workspace --no-run`, focused unit tests, `cargo fmt --all -- --check`, and `python tools/verify_parpulse.py <run-name>`. See `.agents/skills/verify-parpulse/SKILL.md` before product changes. Build with two jobs and debug information disabled on this workstation. Keep the application lockfile checked in after compatibility fixes.

Existing tests include real AWS-dependent cases and timing assertions. Never run cloud cases with ambient credentials. Verification uses owned loopback services, isolated working directories and the shipped sample Parquet file. It preserves logs and response hashes under ignored `.audit/` after cleaning up only its owned process and scratch directory.

Cache identity includes source kind, bucket and every object key without ambiguous concatenation. Cache files must stay inside the owned cache directory. Keep the legacy client API compatible, and expose errors explicitly in new checked streaming APIs. Do not weaken errors, boundary checks or inherited tests to make a check pass.
