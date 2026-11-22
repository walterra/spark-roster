# SparkRoster

Manage the models that share your Spark.

SparkRoster is an early-stage project for managing inference services on NVIDIA DGX Spark and other GB10 systems, including the ASUS Ascent GX10. The goal is to run chat and coding LLMs, decision models, and embedding services together when unified memory allows, without replacing the runtimes or observability tools already in use.

**Status:** The offline memory planner is implemented. Read-only Linux memory and systemd inspection prototypes have been tested with synthetic inputs. Live GX10 validation and deployment automation are pending.

## Planned workflow

1. Define model profiles for existing and experimental services. Pin the model and runtime versions, and record context, concurrency, memory requirements, readiness checks, and telemetry endpoints.
2. Group profiles into a deployment set. Mark services that must stay online, such as an embedding endpoint used for indexing.
3. Run a read-only plan before deploying or updating. Check live unified-memory headroom against expected demand during startup, warm-up, and steady state.
4. Apply an approved plan, verify readiness, and restore the previous deployment if the candidate fails. Keep observability running during the change.

## Adoption: capture an existing setup

A planned read-only workflow will inspect a selected systemd workload and draft a portable profile from what is already deployed. Capture model and runtime pins, launch settings, memory limits, and readiness and telemetry references. Keep host-specific paths configurable, omit credentials, and flag anything that needs manual input or measurement rather than guessing. Review the draft before adding it to Git. Inspection must not restart or take ownership of the service. A profile is not verified reproducible until its pins and configuration have been validated in a separate deployment test.

Try the prototypes without Linux or systemd using the synthetic fixtures:

```bash
go run ./cmd/spark-roster memory --meminfo examples/host-observation/meminfo.txt --json
go run ./cmd/spark-roster inspect --unit example.service --from-show examples/host-observation/unit.show --json
go run ./cmd/spark-roster profile draft --unit example.service --from-show examples/host-observation/unit.show --json
```

Omit `--meminfo` to read local Linux `/proc/meminfo`; omit `--from-show` to query the selected local unit through `systemctl show`. Neither command starts or stops anything. Memory observations include `MemAvailable` and separate swap counters. They are **not** planner snapshots: the running service inventory and its memory bounds still need to be collected. A file replay reports the read time, not proof of the original capture time.

The systemd prototype only collects allowlisted unit states and cgroup memory fields. It does not export launch commands or environment variables. The draft references the existing unit and lists what still needs review, including pins, runtime configuration, readiness, telemetry, and memory peaks. It is not yet a runnable profile. Cgroup counters are not validated GPU-memory totals; a unit-level unlimited setting does not rule out parent limits.

## Storage preflight

On Linux, `spark-roster storage --path <existing-directory> --json` reports the capacity and available bytes of the filesystem containing that directory. Omit `--json` for GiB output. Check the intended archive filesystem, not just `/`. Available bytes exclude reserved blocks but do not account for quotas or concurrent writes. The command does not create directories, reserve space, or export images.

## Planner input example

The [v1 input schema](schema/planning-v1.schema.json) describes the catalog, desired deployment set, and host snapshot. The [coding replacement fixture](examples/coding-replacement/) uses synthetic measurements to show how embeddings and decisions can stay online while the coding model is replaced. After reserve, the example leaves **−24 GiB** during overlap and **+10 GiB** if the old coding model stops first. These are synthetic inputs, not live GB10 measurements.

Run from the repository root with Go 1.23 or newer:

```bash
go run ./cmd/spark-roster plan \
  --catalog examples/coding-replacement/catalog.json \
  --deployment-set examples/coding-replacement/deployment-set.json \
  --snapshot examples/coding-replacement/host-snapshot.json \
  --as-of 2026-11-22T12:00:00Z
```

Add `--json` for a versioned result on stdout. All completed verdicts exit `0`; invalid input exits `1`, and invalid CLI usage exits `2`. Diagnostics go to stderr. `--as-of` is for replaying historical snapshots. Without it, the planner uses the current time; snapshots older than five minutes and launch measurements older than 30 days are not trusted.

The planner budgets additions and replacements. It leaves unlisted services in place, sums launch peaks conservatively, and does not credit reclaimed memory on the overlap path. For multiple stops, supply non-overlapping reclaim bounds. Rollback budgeting, live probes, runtime-specific limits, and applying plans are outside this offline implementation.

All memory values are integer bytes. Additional growth is relative to the snapshot, not the service's full resident footprint. A stop-reclaim lower bound estimates how much *additional available memory* a stop may yield; recheck after stopping before trusting it. Missing memory evidence is unknown, not zero. Change the profile ID when changing settings or pins that affect the measured budget.

## Memory planning

A plan should report **fit**, **stop-first**, **insufficient**, or **unknown**. An update that fits after the old model stops may still exceed memory if both versions load at once. The planner should show that difference, account for growth of services that stay online, and recheck headroom before loading new weights.

Measured peaks for the exact profile are preferred. Estimates should show their uncertainty. GPU and CPU share the same memory pool; swap is not model capacity. A fit forecast reduces out-of-memory risk but cannot guarantee success under unmeasured load.

## Observability

Expose per-service readiness and structured lifecycle events, including planned stops, failures, and rollbacks. Record predicted versus observed memory peaks without logging prompts, tool payloads, or credentials. Elastic/Kibana is a planned reference integration, not a deployment dependency.

## License

[Apache License 2.0](LICENSE) covers SparkRoster. Model weights, third-party runtimes, and container images retain their own licenses.
