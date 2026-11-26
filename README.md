# SparkRoster

Start, switch and benchmark inference models on an NVIDIA GB10 Spark (DGX Spark, ASUS Ascent GX10 and other GB10 boxes), with memory checks and a restore path.

```text
$ spark-roster list
ID                   MODE    PORT   CONTEXT  MEMORY FLOOR        SOURCE    TITLE
inkling-2-mini       launch  9932   262144   75 GiB (estimated)  built-in  Inkling-2-Mini on SGLang
qwen3-8-27b-dflash2  launch  30000  262144   90 GiB (estimated)  built-in  Qwen3.8-27B NVFP4 + DFlash2 on SGLang
qwen3-8-27b-pi-q8    launch  9931   262144   65 GiB (estimated)  built-in  Pi-adapted Qwen3.8-27B Q8_0 on llama.cpp
```

A **profile** is one pinned, verifiable description of a service: model and drafter revisions, container image digest or source commit, launch flags, port, a real-generation readiness check and a memory floor. Everything SparkRoster runs is a profile. Three ship with it. Add your own, or adopt a service something else already runs.

What it does:

- **Stage** a profile: download and verify its weights by size and SHA-256, pull or build its runtime, and generate its systemd unit. Disk space is checked before anything downloads.
- **Switch** between profiles: stop the running one and confirm it exited, check host memory, start the new one, wait for a real answer. If the new profile fails to come up, the previous one is restored and verified.
- **Recover** from an interrupted switch or benchmark: every change is journaled before it happens, so a killed run leaves a record that `recover` acts on.
- **Benchmark** profiles on the same coding suite, then report and compare runs. Reports carry the exact profile definitions, so someone else can repeat the run.
- **Plan** memory offline for combinations of services before loading anything. See [docs/planning.md](docs/planning.md).

Everything has `--json` output, documented exit codes and `--dry-run`, so scripts and coding agents can drive it. Humans get tables and the next command to run.

**Status: early.** The profile engine, CLI and benchmark have tests against fake hosts, with mutation checks on the safety rules. Parts have run on the author's GX10 and parts have not. [Verification status](#verification-status) says exactly which.

## Requirements

- A GB10 Spark running Linux ARM64 with the NVIDIA driver, Docker (your user in the `docker` group) and systemd.
- Enough free disk. The three built-in profiles download about 72 GB of files and pull about 32 GB of images (compressed), plus a CUDA llama.cpp build. `spark-roster stage` refuses to start a download that does not fit, with a 20 GiB reserve.
- `sudo` for `systemctl start|stop`. SparkRoster never runs sudo silently: it asks you to run `sudo -v` first, and prints exact commands for the steps it will not run itself.
- For Pi Q8: the CUDA toolkit (`nvcc`; set `CUDACXX` if it is not in `/usr/local/cuda/bin`) and `cmake`, `git`, `curl`.
- Only one heavy model fits at a time. Each profile's memory floor is a reviewed estimate, not a measurement. See [Memory](#memory).

## Install

Download the binary from a release and verify it (release assets appear with the first tag):

```bash
tag=v0.1.0   # replace with the release you want
curl -fsSLO "https://github.com/walterra/spark-roster/releases/download/${tag}/spark-roster_${tag}_linux_arm64"
curl -fsSLO "https://github.com/walterra/spark-roster/releases/download/${tag}/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
install -m 0755 "spark-roster_${tag}_linux_arm64" ~/.local/bin/spark-roster
spark-roster version
```

Or build from source with Go 1.23 or newer. The build is reproducible (the release workflow builds twice and compares):

```bash
# install go
sudo apt  install golang-go

# clone repo
git clone https://github.com/walterra/spark-roster && cd spark-roster

# build spark-roster
CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-X main.version=dev" -o ~/.local/bin/spark-roster ./cmd/spark-roster
```

`list`, `plan`, `memory` and `storage` also run on a Mac or x86 laptop. Starting and stopping needs the Spark.

## First run

```bash
spark-roster doctor                    # checks docker, GPU, systemd, sudo, memory and disk; prints a fix for each problem
spark-roster list                      # the available profiles
spark-roster stage inkling-2-mini      # download, verify, pull the image; --dry-run shows the plan and sizes first
spark-roster switch inkling-2-mini --dry-run
spark-roster switch inkling-2-mini     # asks before stopping anything; --yes skips the prompt
spark-roster status                    # what runs, what is staged, which profile is active, and the next step
```

Staging a launched profile ends with one step only you can do, because it needs root. `stage` writes the generated unit to a staging path for you to review and prints the commands, then exits with code 3 until it is installed:

```text
Artifacts are staged. Install the unit (review ~/.local/share/spark-roster/units/spark-roster-inkling-2-mini.service first):
  sudo install -o root -g root -m 0644 '...' /etc/systemd/system/spark-roster-inkling-2-mini.service
  sudo systemctl daemon-reload
Then run: spark-roster switch inkling-2-mini
```

Run `sudo -v` before `switch`, `stop` and `recover` so they do not stop on a password prompt. The model answers on its port, for example `http://127.0.0.1:9932/v1` (OpenAI-compatible). Profiles listen on `127.0.0.1` only, with no API key.

Switching downtime depends on the model. Measured on a GX10 at a 262,144-token window: Pi Q8 serving after about 1 minute, Inkling-2-Mini after about 6 minutes. The Qwen profile's start time has not been measured here; its sources report 3.5 to 9 minutes. Each profile's `ready_timeout` is a watchdog above those numbers, not a measurement.

## The commands

| Command                                                             | What it does                                                                                                                         |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------- | ------------ | ----------------------------- |
| `list`                                                              | Profiles with port, context window, memory floor and where they come from                                                            |
| `status`                                                            | Per-profile state (`running`, `stopped`, `partial`, `missing`), whether staged, the active profile, unsettled work, the next command |
| `doctor`                                                            | Platform, user, docker, GPU, systemd, sudo, memory, disk, journal and profiles, each with a fix                                      |
| `stage PROFILE`                                                     | Download and verify artifacts, generate the unit. `--dry-run` reports sizes and free space                                           |
| `switch PROFILE`                                                    | Replace the running profile. Refuses before stopping anything if the target is unstaged, the port is busy or sudo needs a password   |
| `stop`                                                              | Stop the running profile and confirm its exit                                                                                        |
| `recover`                                                           | Settle an interrupted switch or benchmark: stop everything left running, restore the baseline, start paused units                    |
| `benchmark launch                                                   | worker                                                                                                                               | report FILE | compare A B` | See [Benchmarks](#benchmarks) |
| `plan`, `snapshot`, `memory`, `storage`, `inspect`, `profile draft` | Offline planning and read-only host inspection, in [docs/planning.md](docs/planning.md)                                              |
| `version`                                                           | Version, commit and platform                                                                                                         |

Flags for every command: `--json`, and `--profiles DIR` and `--data DIR` to move where site profiles and state live (defaults `~/.config/spark-roster/profiles` and `~/.local/share/spark-roster`, or `$SPARK_ROSTER_PROFILES` and `$SPARK_ROSTER_DATA`). State-changing commands also take `--dry-run` and `--yes`. Flags may come before or after the profile id.

Without `--yes` and without a terminal, a command that changes services refuses rather than guess. In a terminal it asks, and only `y` or `yes` counts.

### Exit codes

| Code | Meaning                                                                                                                                                                |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0    | Done                                                                                                                                                                   |
| 1    | The operation failed                                                                                                                                                   |
| 2    | Bad arguments                                                                                                                                                          |
| 3    | A precondition was not met and **nothing was changed**: not staged, port busy, sudo needs a password, unsettled earlier work, confirmation missing, an unit to install |
| 4    | A switch failed and the previous profile was **restored and verified**                                                                                                 |
| 5    | Host state is unknown: a restore failed or a stop was not confirmed. Run `spark-roster recover`                                                                        |

## Using it from a script or an agent

- Add `--json`. Stdout is exactly one JSON document with a `kind` and `schema_version`; progress goes to stderr. Kinds: `profile_list`, `status`, `switch`, `stop`, `recover`, `stage`, `doctor`, `benchmark_launch`, `benchmark_compare`.
- Branch on the exit code, not on text. Operations report `outcome` (for example `ready`, `refused`, `failed_restored`, `unsettled`, `needs_confirmation`, `operator_step_required`) and `next`, a list of commands to run.
- Always call with `--dry-run` first when a plan matters, then repeat with `--yes`.
- Never pass credentials. Profiles carry no secrets, and the only privileged calls are `systemctl start` and `stop` through `sudo -n`. When sudo needs a password the command exits 3 and says to run `sudo -v`; it does not hang.
- One operation runs at a time. A second one exits 3 naming the lock.
- A coding agent can run everything except the steps that need your password: `sudo install` of a unit and the `sudo systemd-run` of a benchmark. Both are printed as exact commands.

## Profiles

A profile is a JSON file. Built-ins are compiled in; yours go in `~/.config/spark-roster/profiles/ID.json` (the file name must be the profile id, and an id cannot reuse a built-in). The format, every field and the variables are in [docs/profiles.md](docs/profiles.md). Rules the loader enforces:

- Hugging Face and git artifacts are pinned to a full 40-character commit. Images are pinned to a `sha256:` digest. A branch, tag or `latest` is refused.
- Every Hugging Face artifact lists its files with sizes, and large files with SHA-256, so staging and `status` can check them.
- A profile either **launches** a service (docker or a process, with a unit SparkRoster generates) or **adopts** one that systemd already runs. SparkRoster starts and stops adopted units but never rewrites them.
- Memory evidence is `measured` or `estimated`, with a note and a date.

### Adopt a service you already run

Write a profile with an `adopt` block naming your units, the container to confirm gone on stop, the port and model name, and a key file if the endpoint needs one. Then `switch` can move between it and the built-ins, and `benchmark launch` can use it as the baseline that is restored at the end. Adoption does not need any change to your service.

A worked example, a Qwen deployment behind a keepalive proxy, is in `examples/adopted-service.json`.

## Benchmarks

The benchmark runs eight JavaScript tasks (three repetitions each) through the [Pi coding agent](https://pi.dev) on each profile, one at a time. Each attempt gets a fresh workspace. A separate container loads the submitted function and runs hidden cases, so grading is code, not a model. The runner, tasks and relay are in `cmd/spark-roster/pi-bench/`. It was written by a coding agent, inspired by [kyuz0/pi-bench](https://github.com/kyuz0/pi-bench), not copied from it. Streaming speed probes (three prompt sizes, cold and warm) run alongside.

Settings for every run: Pi 1.5.0 with a 262,144-token window and a 16,384-token output cap, at most 20 inference requests and 10 minutes per attempt, grading in a container with no network.

```bash
spark-roster benchmark stage          # once: build the ARM64 Pi runner image (needs docker)
spark-roster benchmark launch inkling-2-mini qwen3-8-27b-pi-q8 --gate    # one task, one repetition: a quick check
spark-roster benchmark launch inkling-2-mini qwen3-8-27b-pi-q8           # the full run
```

`launch` changes nothing. It treats the profile serving now as the **baseline**, checks that every profile is staged and every port is free, and prints a paste-safe Bash block. The block verifies the binary's SHA-256, installs a root-owned copy and starts a systemd unit with a recovery hook. It also prints the downtime and the ceiling.

What the run does:

1. Writes the report and a journal record before it stops anything.
2. Pauses units a profile lists under `pause_during_benchmark` (a health-check timer, say), and refuses to time anything if one stays active.
3. Runs each profile in order, then always restores the baseline and resumes what it paused, whether the run finished, failed or was cancelled. If the process dies, systemd's recovery hook does the same.

The ceiling is the sum of limits the code enforces: attempts, speed probes, a start allowance per profile that has to be started (its `ready_timeout` plus 5 minutes), restoring the baseline and a margin. It is a watchdog, not a prediction. A full three-profile run took about 2.5 hours at 256K in the author's runs.

Read results with:

```bash
spark-roster benchmark report ~/.local/share/spark-roster/benchmarks/latest.json
spark-roster benchmark compare run-a.json run-b.json
```

`compare` lists differences in suite, window, output cap, limits and repetitions first, because they make pass counts incomparable, then per-profile passes and task changes. Reports (schema 3) hold the profile definitions as written, the Pi runner image, the tool version and the settings.

### Reproducing the three-profile comparison

The author compared Qwen3.8-27B (NVFP4 with a DFlash2 drafter on SGLang), a Pi-adapted Qwen3.8-27B Q8_0 on llama.cpp and Inkling-2-Mini on SGLang. On the same GX10 at a 262,144-token window and a 16,384-token output cap, passes out of 24 were 17, 18 and 21 for the three. That is a deployment comparison on eight small tasks, not a model ranking. It was one run, and earlier runs of the same profiles moved by two or three passes.

To repeat it:

```bash
spark-roster stage qwen3-8-27b-dflash2 && spark-roster stage qwen3-8-27b-pi-q8 && spark-roster stage inkling-2-mini
spark-roster benchmark stage
spark-roster benchmark launch qwen3-8-27b-dflash2 qwen3-8-27b-pi-q8 inkling-2-mini
```

What matches and what does not:

- **Pi Q8 and Inkling-2-Mini:** the built-in profiles carry the same pins and launch flags as the author's runs. Their numbers are comparable.
- **Qwen3.8-27B differs.** The author's run served it from a locally built image (`qwen38-dflash2:v1.2.3`) behind a keepalive proxy on port 30001. The built-in profile uses the public `lmsysorg/sglang` v0.5.22 image by digest, listens on port 30000 and has no proxy. Same model and drafter revisions and the same SGLang flags, but a different image, so its speed and pass count can differ. A repeat on the public image has not been run yet.
- **Variance.** Expect the pass counts to move by a few between runs, and read `compare`'s settings warning before comparing anything.
- Streaming speed at a short prompt, from the same run: about 49 tokens per second for Qwen3.8-27B, 8 for Pi Q8 and 36 for Inkling-2-Mini.

## Memory

A GB10's CPU and GPU share one memory pool, so SparkRoster treats host `MemAvailable` as the budget. A switch checks it twice: after the old profile stops, against the new profile's floor, and while the new one starts, against a 10 GiB reserve. It refuses rather than start something that would run the machine out.

The floors in the built-in profiles (65, 75 and 90 GiB) are **estimates**, taken from the start checks the author's trials used. Measured minimum `MemAvailable` during runs was about 49 to 56 GiB with each model serving. The offline planner treats an estimate as unverified and answers `unknown` rather than `fit`, so measure your own box before relying on a plan.

## Safety model

- Nothing changes before a check. Every state-changing command has `--dry-run`.
- A stop is only believed after its exit is confirmed (unit inactive and container gone). A new profile starts only after that.
- Switches, stops and benchmark runs write a journal first. After a crash, `status` shows it and `recover` finishes the job. A new switch is refused until it is settled.
- Restoring the previous profile is verified by a real generation, not by a unit state.
- Privilege: only `systemctl start|stop` through `sudo -n`, with unit names validated. Generated units run as your user, never root, with `Restart=no`. Arguments are quoted for systemd and control characters are refused.
- Downloads go to `.partial`, are verified by size and SHA-256 and only then renamed. A corrupt file is deleted, never installed. Redirects to plain http are refused.

SparkRoster does not change the GPU clock cap or other host settings, and it leaves observability agents alone.

## Troubleshooting

| Symptom                                                               | Fix                                                                                                                    |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `switch` exits 3: "not staged"                                        | `spark-roster stage PROFILE`, then install the unit it prints                                                          |
| exit 3: "sudo needs a password"                                       | run `sudo -v` in the same terminal and retry                                                                           |
| exit 3: "port N is in use"                                            | something else listens there; stop it or change the profile's port                                                     |
| exit 3: "earlier switch is unsettled"                                 | `spark-roster recover`                                                                                                 |
| exit 5                                                                | host state unknown. Look at `status`, then `recover`. A unit that will not stop needs your hands: `journalctl -u UNIT` |
| `status` says the journal names a profile that is not running         | `spark-roster stop` clears the stale record without touching any service                                               |
| `stage` refuses for disk space                                        | free space at the directory it names, or move the data directory with `--data`                                         |
| a profile never becomes ready                                         | `journalctl -u spark-roster-PROFILE.service`; the switch has already restored the previous profile                     |
| the built-in Qwen profile starts slower or differently than elsewhere | it uses the public image; compare with your own `adopt` profile                                                        |

## Verification status

Be honest about what has run:

- **Run on a real GX10:** staging and switching of Pi Q8 and Inkling through earlier fixed-profile commands (the same pins and flags the built-in profiles use); the three-profile benchmark and its restore; the benchmark relay, selftest and failure reports.
- **Tested against fakes only:** the profile engine, `stage`, `switch`, `stop`, `recover`, `doctor` and `benchmark launch|worker|compare`. That includes real `systemctl`, `sudo`, Docker output, the generated unit under systemd, the recovery hook's quoting and the lock files.
- **Never run:** the public Qwen profile and its image on a fresh machine. This README's first-run flow on a machine that is not the author's.

Until the first live runs are recorded here, treat every step above as untested on hardware, keep the commands' `--dry-run` close, and report what you find.

## Credits and licenses

[Apache License 2.0](LICENSE) covers SparkRoster. Model weights, runtimes and container images keep their own licenses.

The Qwen profile's launch flags follow the SGLang recipes of [Alzeo](https://github.com/hasso5703/alzeo) (MIT) and [MiaAI-Lab](https://github.com/MiaAI-Lab/Qwen3.8-27B-SGLang-DGX-Spark), which also publish measured settings for these models on a Spark. The benchmark is inspired by [kyuz0/pi-bench](https://github.com/kyuz0/pi-bench). SparkRoster does not use their code. Models and runtimes: [Qwen3.8-27B NVFP4](https://huggingface.co/RadixArk/Qwen3.8-27B-NVFP4), its [DFlash2 drafter](https://huggingface.co/maurienne-ai/Qwen3.8-27B-DFlash2-NVFP4-RTNcal), the [Pi-adapted GGUF](https://huggingface.co/harness-hub/Qwen3.8-27B-pi-adapted-GGUF) with [llama.cpp](https://github.com/ggml-org/llama.cpp), [Inkling-2-Mini](https://huggingface.co/thinkingmachines/Inkling-2-Mini) and [SGLang](https://github.com/sgl-project/sglang).

## Contributing

`go vet ./... && go test -race ./...` and `node --test cmd/spark-roster/pi-bench/relay.test.cjs` run in CI on Linux and macOS. Tests for safety rules (restore, confirmation, journal, privilege) come with a mutation check: break the rule and a test must fail.
