# SparkRoster

Manage the models that share your Spark.

SparkRoster is an early-stage project for managing inference services on NVIDIA DGX Spark and other GB10 systems, including the ASUS Ascent GX10. The goal is to run chat and coding LLMs, decision models, and embedding services together when unified memory allows, without replacing the runtimes or observability tools already in use.

**Status:** Design only. No CLI or deployment automation is implemented yet.

## Planned workflow

1. Define model profiles for existing and experimental services. Pin the model and runtime versions, and record context, concurrency, memory requirements, readiness checks, and telemetry endpoints.
2. Group profiles into a deployment set. Mark services that must stay online, such as an embedding endpoint used for indexing.
3. Run a read-only plan before deploying or updating. Check live unified-memory headroom against expected demand during startup, warm-up, and steady state.
4. Apply an approved plan, verify readiness, and restore the previous deployment if the candidate fails. Keep observability running during the change.

## Memory planning

A plan should report **fit**, **stop-first**, **insufficient**, or **unknown**. An update that fits after the old model stops may still exceed memory if both versions load at once. The planner should show that difference, account for growth of services that stay online, and recheck headroom before loading new weights.

Measured peaks for the exact profile are preferred. Estimates should show their uncertainty. GPU and CPU share the same memory pool; swap is not model capacity. A fit forecast reduces out-of-memory risk but cannot guarantee success under unmeasured load.

## Observability

Expose per-service readiness and structured lifecycle events, including planned stops, failures, and rollbacks. Record predicted versus observed memory peaks without logging prompts, tool payloads, or credentials. Elastic/Kibana is a planned reference integration, not a deployment dependency.

## License

[Apache License 2.0](LICENSE) covers SparkRoster. Model weights, third-party runtimes, and container images retain their own licenses.
