# Prefect Liveness Monitor

Sidecar that streams logs from the Prefect background-services pod and restarts
it when the queue processor goes silent or emits too many consecutive errors.

## How it works

1. Opens a `follow=true` HTTP stream against the Kubernetes log API for the
   current pod.
2. Feeds each log line into a queue consumed by the controller loop.
3. The controller restarts the pod (by exiting with code 1) if:
   - No line arrives within `SILENCE_WINDOW` seconds.
   - `MAX_FAILURES` consecutive error-pattern lines are detected.
4. On reconnect, the last batch of lines is deduplicated to avoid false
   positives from replayed history.

## Environment variables

| Variable                | Required | Default | Description                                        |
| ----------------------- | -------- | ------- | -------------------------------------------------- |
| `POD_NAME`              | ✅       | —       | Name of the pod (injected via `fieldRef`)          |
| `POD_NAMESPACE`         | ✅       | —       | Namespace of the pod (injected via `fieldRef`)     |
| `SILENCE_WINDOW`        |          | `1800`  | Seconds of log silence before restart              |
| `MAX_FAILURES`          |          | `3`     | Consecutive error lines before restart             |
| `STARTUP_GRACE_SECONDS` |          | `90`    | Seconds to suppress silence alerts after pod start |
| `STREAM_READ_TIMEOUT`   |          | `120`   | HTTP read timeout for the log stream (seconds)     |
| `K8S_API`               |          | `https://kubernetes.default.svc` | Kubernetes API server base URL    |

## Development

Requires [devenv](https://devenv.sh).

```bash
devenv shell        # enter the dev shell
```

| Task         | Command                 | Description                  |
| ------------ | ----------------------- | ---------------------------- |
| `mon:run`    | `devenv run mon:run`    | Run the monitor locally      |
| `mon:test`   | `devenv run mon:test`   | Run the test suite (verbose) |
| `mon:lint`   | `devenv run mon:lint`   | Lint with ruff               |
| `mon:format` | `devenv run mon:format` | Format with ruff             |
| `mon:check`  | `devenv run mon:check`  | Type-check with basedpyright |

### Running tests manually

```bash
uv run pytest tests/ -v --cov --cov-report=term-missing
```
