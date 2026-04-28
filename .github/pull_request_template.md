## Summary

- 

## Professionalization QA

- [ ] I followed `docs/contributor-style-guide.md` for ownership, naming, and complexity.
- [ ] I completed `docs/professionalization-qa.md` or marked non-applicable items with a reason.
- [ ] New and touched Rust code passes the configured Clippy complexity guardrails without broad lint exceptions.
- [ ] SQL-visible behavior has targeted `pg_test` coverage or a documented reason existing coverage is sufficient.

## Verification

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --all-targets --no-default-features --features pg17,pg_test --locked -- -D warnings`
- [ ] `cargo test --lib --locked --no-default-features --features pg17`
- [ ] Targeted `cargo pgrx test pg17 ... -F pg_test`
- [ ] `./scripts/check_extension_upgrade.sh 17`
