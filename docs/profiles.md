# Profile reference

A profile is one JSON file describing a service SparkRoster can start, stop, verify and benchmark. Built-in profiles are in `internal/profile/builtin/`. Put your own in `~/.config/spark-roster/profiles/ID.json`; the file name must be `ID.json` and the id may not reuse a built-in. Unknown fields are rejected, so a typo never silently drops a pin.

```json
{
  "schema_version": 1,
  "kind": "profile",
  "id": "my-model",
  "title": "My model on llama.cpp",
  "artifacts": [ ... ],
  "launch": { ... },            // or "adopt": { ... }, never both
  "serving": { ... },
  "memory": { ... },
  "pause_during_benchmark": ["my-canary.timer"],
  "concurrency": 1,
  "context_tokens": 262144
}
```

## Variables

Paths in artifacts, launch arguments, mounts, environment values and `key_file` may use:

| Variable | Expands to |
|---|---|
| `{home}` | the operator's home directory |
| `{data}` | the data directory (`--data`, default `~/.local/share/spark-roster`) |
| `{hf_cache}` | the Hugging Face hub cache (`$HF_HUB_CACHE`, or `$HF_HOME/hub`, or `~/.cache/huggingface/hub`) |
| `{dir}` | inside a git artifact's `build` commands only: that artifact's checkout directory |

Other braces, such as JSON in an argument, are left alone. Reports store profiles unexpanded, so they hold no machine-specific paths.

## `artifacts`

| `kind` | Required fields | Pinning |
|---|---|---|
| `huggingface` | `name` (repo id), `revision`, `files` | `revision` is a full 40-character commit. `files` lists `path` and `bytes` for every file, and `sha256` for large files (the Hugging Face API publishes it for LFS files). `dest` stores a flat copy instead of the hub cache layout. |
| `image` | `name`, `revision`, optionally `bytes` | `revision` is a `sha256:` digest of the image. `bytes` is the compressed size and only feeds the staging report. |
| `url` | `name` (destination path), `url`, `sha256`, `bytes` | `https` only. |
| `git` | `name` (checkout directory), `url`, `revision`, optionally `build` and `creates` | `revision` is a full 40-character commit. `build` is a list of commands run in the checkout. `creates` names the file that must exist afterwards. |

Optional on any artifact: `role` (`model`, `drafter` or `runtime`), which the offline planner uses to read pins.

## `launch` (SparkRoster runs it)

| Field | Meaning |
|---|---|
| `runtime` | `docker` or `process` |
| `image` | docker only; must be pinned by digest |
| `exe` | process only; the program |
| `args` | arguments after the image (docker) or program (process) |
| `env` | environment variables (docker `-e`) |
| `memory_limit` | `100g` or `512m`; docker `--memory` and `--memory-swap`, or `MemoryMax` for a process |
| `docker_args` | extra `docker run` flags placed before the image, such as `--shm-size` and `16g` as two entries |
| `mounts` | `src:/dst[:ro]` bind mounts, written with `--mount` so a missing source is an error, not a new directory |
| `dirs` | directories created at staging |

SparkRoster generates `spark-roster-ID.service` from this, runs it as your user with `Restart=no`, and prints the `sudo install` commands. The unit is reproducible: environment variables are sorted and arguments are quoted for systemd.

## `adopt` (something else runs it)

```json
"adopt": { "units": ["engine.service", "proxy.service"], "container": "engine" }
```

`units` start in order and stop in reverse. `container`, if set, must be gone after a stop. SparkRoster never edits these units.

## `serving`

| Field | Meaning |
|---|---|
| `port` | where the OpenAI-compatible API answers (`127.0.0.1`) |
| `model` | the model name requests use |
| `key_file` | optional file holding a Bearer token |
| `ready_timeout` | how long a start may take before the switch gives up and restores the previous profile, such as `"20m"`. A watchdog above the real start time. |
| `tool_call_probe` | reserved |

Readiness is a real chat completion that must return the expected answer, not a unit state or a health route.

## `memory`

| Field | Meaning |
|---|---|
| `start_min_available_bytes` | host `MemAvailable` required before this profile starts |
| `steady_bytes` | optional steady-state demand for the planner |
| `evidence` | `measured` or `estimated` |
| `note` | where the number came from |
| `recorded_at` | when it was gathered |

The offline planner only gives a confident verdict for `measured` evidence younger than 30 days.

## `pause_during_benchmark`

Units, usually a health-check timer, that a benchmark stops while it times requests and starts again afterwards. Services and timers are accepted.

## Checking a profile

`spark-roster list` loads and validates every profile and prints the first problem with the field named. `spark-roster stage ID --dry-run` shows what staging would download and whether the disk can hold it.
