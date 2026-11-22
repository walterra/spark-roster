---
name: go-review
description: Review SparkRoster Go diffs, pull requests, and packages for correctness, idiomatic style, security, API compatibility, and test gaps. Use for Go code reviews and pre-merge checks.
---

# Review SparkRoster Go code

1. Identify the requested diff or package. If no base is specified, inspect staged and unstaged changes separately, including relevant untracked Go files. State the scope reviewed. Read changed code together with callers, tests, and affected README and schema contracts.
2. Review against the checklist below. Follow actual control and data flow before reporting an issue. Prefer actionable regressions introduced by the change over unrelated cleanup.
3. Run applicable local checks when safe. Re-read findings against the code and remove duplicates or unsupported claims.

Stay read-only: do not edit, stage, or commit. Do not install tools, run third-party scripts, contact managed hosts, or change services during review. Do not execute untrusted code or repository scripts outside an appropriately isolated environment.

## Review checklist

- **Style and API design:** Use `gofmt` for formatting. Check names, package boundaries, exported documentation, simple control flow, and interfaces that serve an actual caller. Prefer repository conventions over personal taste; style-only nits are non-blocking.
- **Correctness and reliability:** Check boundary conditions, integer conversions and overflow, error propagation, partial failures, context cancellation, resource cleanup, and goroutine ownership. Inspect synchronization and shutdown paths where concurrency is introduced. Check compatibility with the Go version in `go.mod`.
- **Security:** Trace untrusted input to commands, paths, network requests, and queries. Check input validation, shell-free argument passing, authorization and least privilege where relevant, bounded I/O, and redaction of credentials and inference payloads. Keep findings focused on risk and remediation; do not build or run exploits.
- **Planner:** Verify that `plan` stays read-only and never proposes stopping a protected service. Check byte units and checked arithmetic, reserve, evidence freshness, startup and overlap peaks, and additional growth of retained workloads. Missing evidence is not zero; do not double-count resident allocations, count swap as usable model capacity, or credit stop reclaim on an overlap path. Keep planned reclaim distinct from observed headroom.
- **CLI contracts:** Check versioned JSON and schema compatibility, stable verdicts and exit codes, and JSON-only stdout in JSON mode. Diagnostics belong on stderr. For future systemd changes, verify explicit authorization for mutations and pass arguments directly without shell interpolation.
- **Tests:** Look for regression tests that fail for the right reason. Prefer deterministic fixtures and explicit clocks for planner tests. Check relevant negative cases such as stale evidence, malformed input, protected-service conflicts, and exact-capacity boundaries. Keep offline tests independent of systemd, GPUs, and live services.

## Checks

Prefer documented repository checks. Otherwise, use `gofmt -l` on changed Go files, `go vet ./...`, and `go test ./...` from the module root. Run `go test -race ./...` when relevant and supported. Use `govulncheck ./...` if installed and network policy permits; it checks known vulnerabilities and does not replace manual review. Report which checks passed, failed, or were skipped and why.

## Output

List verified findings first, grouped by severity (`must fix`, `should fix`, `nit`). Each finding needs `file:line`, the condition that triggers the problem, concrete impact, and a suggested fix. Separate open questions and unverified risks from findings. If nothing is actionable, say so, then list checks and remaining coverage gaps.

## References

- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) and [Google Go Style Decisions](https://google.github.io/styleguide/go/decisions)
- [Go Security](https://go.dev/security/) and [Go Vulnerability Management](https://go.dev/doc/security/vuln/)
- [samber Go skills](https://github.com/samber/cc-skills-golang) for deeper style, concurrency, testing, and security guidance
