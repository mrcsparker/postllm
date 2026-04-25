# Code Quality Cleanup Rubric

Use this rubric for release-gate cleanup passes that touch naming, comments, module boundaries, or dead code. It is deliberately narrower than a contributor style guide: it exists to make cleanup reviews repeatable without expanding the public API.

## Naming

- SQL-facing wrapper names should mirror the SQL function they expose.
- Internal helpers should name the domain behavior they own, not the call site that happens to use them.
- Parser and validator names should state whether they normalize, validate, or format diagnostics.
- Error helpers should name the user-visible argument or setting they report.

## Comments

- Keep comments when they explain a non-obvious `pgrx`, `PostgreSQL`, runtime, or safety constraint.
- Remove or rewrite comments that restate a function name, describe mechanics visible in the next line, or drift from the code.
- Use module-level comments only when they clarify ownership boundaries.
- Prefer actionable doc comments on crate-visible helpers because `README.md` is compiled as crate documentation.

## Module Boundaries

- SQL wrapper modules in `src/api/` should only normalize SQL arguments and format results.
- Runtime, policy, and catalog modules should own behavior; wrappers should not duplicate those decisions.
- Use the narrowest visibility that still supports the SQL router and sibling modules.
- Keep helper modules domain-shaped instead of dumping unrelated utilities into a shared file.

## Dead Code And Drift

- Remove helpers with no non-test caller unless the next planned call site is in the same change.
- Remove stale lint exceptions when the lint no longer fires.
- Prefer shared parser/formatter helpers when enum diagnostics must stay consistent.
- Run `cargo clippy --all-targets --features pg17,pg_test --locked -- -D warnings` before marking a cleanup slice complete.
