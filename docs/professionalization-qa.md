# Professionalization QA Checklist

Use this checklist before merging milestone work or release-gate cleanup. Every item should be answered in the pull request, either checked or marked not applicable with a short reason.

## Source Quality

- [ ] New or touched modules have a clear owner and do not duplicate behavior from another module.
- [ ] SQL wrappers delegate behavior to owner modules instead of reimplementing runtime, catalog, or policy rules.
- [ ] Shared SPI/query patterns use `src/sql.rs` helpers where practical.
- [ ] New and touched functions pass the configured complexity guardrails without broad lint exceptions.
- [ ] Any `#[expect(...)]` added in the change has a local reason and cannot be removed by a simple split.

## Behavior

- [ ] User-facing errors include the failing argument or setting and a concrete fix path.
- [ ] Permission, secret, runtime, and network-policy behavior stays centralized in the owning modules.
- [ ] SQL-visible behavior has a targeted test or an explicit reason why existing coverage is sufficient.
- [ ] Extension upgrade behavior is unaffected or covered by the upgrade smoke check.

## Documentation

- [ ] New SQL behavior is reflected in `docs/reference.md` or the relevant workflow doc.
- [ ] New contributor rules are reflected in `docs/contributor-style-guide.md` or `docs/code-quality-rubric.md`.
- [ ] Operational changes are reflected in `docs/operations.md`.
- [ ] The documentation index still points contributors to the right starting pages.

## Verification

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --all-targets --no-default-features --features pg17,pg_test --locked -- -D warnings`
- [ ] `cargo test --lib --locked --no-default-features --features pg17`
- [ ] Targeted `cargo pgrx test pg17 ... -F pg_test` checks for SQL-visible changes
- [ ] `./scripts/check_extension_upgrade.sh 17` for extension DDL, catalog, or migration changes
