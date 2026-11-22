# SparkRoster

Manage the models that share your Spark.

SparkRoster is an early-stage project for managing inference services on NVIDIA DGX Spark and other GB10 systems, including the ASUS Ascent GX10. The goal is to run chat and coding LLMs, decision models, and embedding services together when unified memory allows, without replacing the runtimes or observability tools already in use.

**Status:** Design only. No CLI or deployment automation is implemented yet.

## Planned workflow

1. Define model profiles for existing and experimental services. Pin the model and runtime versions, and record context, concurrency, memory requirements, readiness checks, and telemetry endpoints.
2. Group profiles into a deployment set. Mark services that must stay online, such as an embedding endpoint used for indexing.
3. Run a read-only plan before deploying or updating. Check live unified-memory headroom against expected demand during startup, warm-up, and steady state.
4. Apply an approved plan, verify readiness, and restore the previous deployment if the candidate fails. Keep observability running during the change.

## Adoption: capture an existing setup

A planned read-only workflow will inspect a selected systemd workload and draft a portable profile from what is already deployed. Capture model and runtime pins, launch settings, memory limits, and readiness and telemetry references. Keep host-specific paths configurable, omit credentials, and flag anything that needs manual input or measurement rather than guessing. Review the draft before adding it to Git. Inspection must not restart or take ownership of the service. A profile is not verified reproducible until its pins and configuration have been validated in a separate deployment test.

## Planner input example

The [v1 input schema](schema/planning-v1.schema.json) describes the catalog, desired deployment set, and host snapshot. The [coding replacement fixture](examples/coding-replacement/) uses synthetic measurements to show how embeddings and decisions can stay online while the coding model is replaced. After reserve, the example leaves **−24 GiB** during overlap and **+10 GiB** if the old coding model stops first. These are fixture calculations, not live GB10 results or CLI output.

All memory values are integer bytes. Additional growth is relative to the snapshot, not the service's full resident footprint. A stop-reclaim lower bound estimates how much *additional available memory* a stop may yield; recheck after stopping before trusting it. Missing memory evidence is unknown, not zero. Change the profile ID when changing settings or pins that affect the measured budget.

## Memory planning

A plan should report **fit**, **stop-first**, **insufficient**, or **unknown**. An update that fits after the old model stops may still exceed memory if both versions load at once. The planner should show that difference, account for growth of services that stay online, and recheck headroom before loading new weights.

Measured peaks for the exact profile are preferred. Estimates should show their uncertainty. GPU and CPU share the same memory pool; swap is not model capacity. A fit forecast reduces out-of-memory risk but cannot guarantee success under unmeasured load.

## Observability

Expose per-service readiness and structured lifecycle events, including planned stops, failures, and rollbacks. Record predicted versus observed memory peaks without logging prompts, tool payloads, or credentials. Elastic/Kibana is a planned reference integration, not a deployment dependency.

## License

[Apache License 2.0](LICENSE) covers SparkRoster. Model weights, third-party runtimes, and container images retain their own licenses.
