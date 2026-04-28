# Contributor Style Guide

This guide is the review contract for new and touched code. The cleanup rubric covers narrow release-gate cleanup passes; this page covers everyday contribution expectations.

## Ownership Boundaries

- SQL wrappers in `src/api/` should normalize SQL arguments, call one owner module, and format SQL-facing results.
- Runtime behavior belongs in `backend`, `candle`, `client`, and execution modules, not in SQL wrappers.
- Operator controls belong in `operator_policy`, `permissions`, `catalog`, `secrets`, and `guc`.
- Shared SPI helpers belong in `sql`; do not add one-off `Spi::get_one_with_args(...)` wrappers in feature modules when the helper can express the pattern.
- Keep visibility at `pub(crate)` or narrower unless pgrx export machinery requires broader visibility.

## Naming

- Name modules after the behavior they own, not the current caller.
- Name parser functions with `parse`, validation functions with `validate` or `require`, and normalization functions with `normalize`.
- SQL-facing functions should mirror their SQL name or the owning SQL family.
- Error helpers should name the user-visible argument, setting, or policy object they report.
- Avoid names that describe implementation mechanics only, such as `handle`, `process`, or `do_*`, unless the surrounding domain makes them precise.

## Complexity

- New and touched functions must pass the Clippy thresholds in `clippy.toml`.
- Treat `too_many_lines`, `too_many_arguments`, `type_complexity`, and `cognitive_complexity` as design feedback before adding an exception.
- Use `#[expect(clippy::..., reason = "...")]` only when pgrx SQL export shapes or a deliberate boundary make the complexity clearer than the split.
- If a function needs an exception, keep the exception local and explain why the shape belongs there.
- Prefer small helpers with domain names over comments that narrate a long function.

## Error Handling

- Return `crate::error::Result<T>` from fallible internal code.
- Use `?` for propagation and keep recovery logic explicit at the boundary that owns the decision.
- Do not use `unwrap`, `expect`, `panic`, `todo`, or `unimplemented` outside test code.
- User-facing errors should include the failing argument or setting and a concrete fix path.

## Comments And Documentation

- Comments should explain non-obvious pgrx, PostgreSQL, runtime, policy, or safety constraints.
- Delete comments that restate the next line of code.
- Add docs when introducing a new public SQL behavior, operator workflow, or contributor-facing ownership rule.
- Link new contributor docs from `docs/README.md` so the reading path stays discoverable.

## Tests

- Add focused unit tests for pure parsing, validation, and serialization behavior.
- Add `pg_test` coverage when the change depends on PostgreSQL catalogs, GUCs, SPI, permissions, extension upgrade behavior, or SQL-visible output.
- Prefer shared builders and fixtures over repeated setup blocks.
- Keep test names sentence-like and behavior-specific.

## Required Checks

Run the same checks CI runs for the touched area:

```bash
cargo fmt --all --check
env PGRX_HOME=/tmp/postllm-pgrx-home cargo clippy --all-targets --no-default-features --features pg17,pg_test --locked -- -D warnings
cargo test --lib --locked --no-default-features --features pg17
```

Run targeted `cargo pgrx test pg17 ... -F pg_test` checks when SQL-visible behavior changes.
