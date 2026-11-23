# SparkRoster

Manage the models that share your Spark.

SparkRoster is an early-stage project for managing inference services on NVIDIA DGX Spark and other GB10 systems, including the ASUS Ascent GX10. The goal is to run chat and coding LLMs, decision models, and embedding services together when unified memory allows, without replacing the runtimes or observability tools already in use.

**Status:** The offline memory planner is implemented. Read-only Linux memory and systemd inspection prototypes have been tested with synthetic inputs. Live GX10 validation and deployment automation are pending.

## Planned workflow

1. Define model profiles for existing and experimental services. Pin the model and runtime versions, and record context, concurrency, memory requirements, readiness checks, and telemetry endpoints.
2. Group profiles into a deployment set. Mark services that must stay online, such as an embedding endpoint used for indexing.
3. Run a read-only plan before deploying or updating. Check live unified-memory headroom against expected demand during startup, warm-up, and steady state.
4. Apply an approved plan, verify readiness, and restore the previous deployment if the candidate fails. Keep observability running during the change.

## Three-profile Pi benchmark

`benchmark stage` builds the locked ARM64 Pi runner image. `benchmark preflight` verifies the staged profiles and stock baseline; `benchmark check` exercises stock streaming and one real Pi task without switching services. A systemd-hosted `benchmark run` runs cold-intended/warm-repeat streaming probes and eight test-graded Pi tasks (three attempts per task) on each of stock Qwen, Pi Q8, and Inkling. Use `benchmark report` for a scorecard or `benchmark status` for per-attempt JSON. Run only with the site runbook's root-owned binary and `ExecStopPost` recovery hook. The hook confirms candidate exit and verifies stock generation; Elastic ingestion requires a separate check. This fixed-profile suite is not a general deployment engine or boot-time recovery service.

## Operator interface roadmap

Build the terminal app before the web console. The proposed `spark-roster tui` runs on the Spark, including over an SSH terminal, and gives operators one place to monitor the host and manage inference workloads.

- Monitor live RAM headroom, disk space, GPU temperature and clocks, and workload readiness. Show stale or unavailable data explicitly; link to Kibana for historical analysis without making Elastic a dependency for local status.
- Preview workload changes and memory forecasts, confirm the exact plan, follow progress, and verify recovery. Keep model operations separate from host policies such as the GPU clock cap.
- Share planning, execution, and inspection code with the existing non-interactive CLI. Preserve structured JSON for scripts and agents; do not require terminal scraping or add a separate control plane for the TUI.

Design reference: EXO’s dark, monospaced console. Borrow its restrained accent color, compact resource gauges with numeric readouts, and clear separation of live hardware, model details, and actions. In SparkRoster, make unified RAM headroom, GPU telemetry, active deployment, and staged models readable at a glance. Keep plan verdicts, switch progress, and restore state visible where operators act. Use explicit status labels so color is never the only signal. The reference sets the bar for polish, not a requirement to add chat or multi-host topology.

First deliverable: view live host and workload status and preview a plan without writes. Add switch and restore controls once they use the same verified lifecycle operations as the CLI. Defer the web console until the terminal workflow is proven.

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

## Plan from a live host observation

On Linux, run `spark-roster snapshot --inventory inventory.json > snapshot.json`, then pass that file to `spark-roster plan --snapshot snapshot.json` with the catalog and desired deployment set. A v1 inventory looks like:

```json
{
  "schema_version": 1,
  "kind": "systemd_inventory",
  "services": {
    "coding": {"profile_id": "stock-coding-v1", "units": ["engine.service", "proxy.service"]}
  }
}
```

List every managed workload expected to be running and all units needed for each workload. Capture checks that those units are loaded and active, then reads host memory. It refuses disagreement rather than dropping a partially active workload. The inventory is an operator-reviewed mapping, not auto-discovery or proof of artifact identity. The snapshot contains no growth or reclaim bounds: unit state and cgroup counters cannot establish them. Expect `unknown` when those bounds are needed. Neither command mutates workloads.

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
