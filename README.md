# nats-workflows

A Go library for durable workflows on NATS JetStream. Define workflows in
your app, start Runs, schedule future Runs, and accept durable Hook input.
Use an existing NATS connection or embed a server in your process.

The standalone CLI and HTTP server remain available under `cmd/nats-workflows`.
See [embedding](docs/embedding.md) for integration and lifecycle details,
`CONTEXT.md` for terminology, and `docs/` for the execution model.

## Use in another Go app

Requires Go 1.26.5 or newer. Once this module is published:

```sh
go get github.com/arekbartnik/nats-workflows/engine
```

Import the packages your app needs:

| Package | Purpose |
| --- | --- |
| `github.com/arekbartnik/nats-workflows/engine` | Workflow definitions, execution, Runs, schedules and Hooks |
| `github.com/arekbartnik/nats-workflows/embedded` | Optional in-process NATS server with explicit storage configuration |
| `github.com/arekbartnik/nats-workflows/api` | Optional HTTP handler for an engine |

Construct an engine using your connection, logger and workflows:

```go
eng, err := engine.New(ctx, nc, logger,
    engine.Config{Namespace: "myapp"},
    orderWorkflow(orders),
)
```

Run `eng.Serve(ctx)` in a supervised goroutine and call `eng.Start`,
`eng.Schedule`, `eng.Submit` or `eng.Wait` from your app. Handle the constructor
and worker errors. Cancel the worker context and wait for `Serve` to return
before closing NATS. The engine never closes a connection you supply.

The [complete embedding example](examples/embedded/main.go) defines a workflow,
starts embedded NATS, runs it, reads typed State and shuts down:

```sh
go run ./examples/embedded
```

Sample order, AI and SWAPI workflows live in `examples/workflows`; they are
not registered automatically. The library does not read `.env`, choose ports,
install signal handlers or launch an HTTP server.

## Run examples with Task

With Task installed, run `task` to list the available commands:

```sh
task example:embedded
task example:serve
task example:order -- 750
task example:random
task example:choose
task example:ask -- 'Why is the sky blue?'
```

`task dev` is an alias for `task example:serve`. Both use Portless and expose
Swagger at `https://nats-workflows.localhost/api/docs`. Install Portless with
`npm install -g portless` if needed; it starts its local proxy automatically.
Run the workflow commands in another terminal. They connect to the local NATS
server when available, or start an embedded server themselves. The SWAPI
examples need internet access; `example:ask` also needs `OPENROUTER_API_KEY`
in the environment or `.env`.

Arguments after `--` pass through to the example or CLI:

```sh
task example:embedded -- -data /tmp/my-workflow-example
task cli -- list
task cli -- approve RUN_ID
task cli -- retry RUN_ID
task example:order -- 750 --at '2027-10-01T12:00:00Z'
```

The embedding example uses `data/example` by default. CLI examples share
`data` and honor `NATS_URL`, `NATS_DATA`, `HTTP_ADDR`, and `WORKFLOW_NAMESPACE`.
Portless supplies `PORT`, `HOST`, and `PORTLESS_URL`; its assigned port takes
precedence over `HTTP_ADDR`. Run `task cli -- serve` for direct access without
the proxy, defaulting to `http://localhost:8080`.
Keep `example:serve` running for scheduled Runs to execute.

## Upgrade from the standalone layout

- Run the CLI with `go run ./cmd/nats-workflows` instead of `go run .`.
- Update imports from `00f/nats-workflows` to
  `github.com/arekbartnik/nats-workflows`. Sample workflow imports now end in
  `/examples/workflows`.
- `Workflow.ResumeCmd` and `Engine.ResumeCmd` have been removed. The CLI owns
  its command hints.
- An empty namespace preserves existing stream, bucket and subject names.
  Choosing a new namespace selects separate storage; it does not migrate Runs.
- Hook contracts include the input type's Go package path. The module rename
  and sample relocation change those IDs. Finish existing demo Runs with
  pending Hooks using the old binary before upgrading, including failed Runs
  that may retry a Hook step. This change does not migrate stored Hook contracts.

Workflow registration now rejects invalid definitions before creating storage.
Workers sharing a namespace must use compatible definitions and configuration.

## HTTP API & Swagger

`serve` mode also exposes an HTTP API (default `:8080`, override with
`HTTP_ADDR`). **Swagger UI lives at
[`/api/docs`](http://localhost:8080/api/docs)** — interactive documentation
of every endpoint with try-it-out and per-workflow request examples. The
OpenAPI spec itself is served at `/openapi.yaml` (hand-written, embedded in
the binary; the UI page loads its assets from a CDN, so `/api/docs` needs
internet access — the API does not).

```sh
go run ./cmd/nats-workflows    # then open http://localhost:8080/api/docs
```

The endpoints mirror the CLI verbs:

```sh
# what can I run?
curl localhost:8080/api/v1/workflows

# start a run (state seeds the workflow's initial state)
curl -X POST localhost:8080/api/v1/runs \
  -d '{"workflow":"order","state":{"order":{"item":"kit","qty":1,"amount":750}}}'

# follow it: long-polls until the run parks or ends
curl "localhost:8080/api/v1/runs/<runID>/wait?timeout=60s"

# submit to the specific Hook shown in the Run's hooks array
curl -X POST localhost:8080/api/v1/runs/<runID>/resume \
  -H 'Content-Type: application/json' \
  -d '{"token":"<hook-token>","submission_id":"approval-123","payload":{"decision":"approve"}}'

# retry a failed run, list runs
curl -X POST localhost:8080/api/v1/runs/<runID>/retry
curl "localhost:8080/api/v1/runs?status=parked"

# a run's history: one snapshot per state change, oldest first
curl localhost:8080/api/v1/runs/<runID>/history
```

The API has no auth — it trusts its network, same as the NATS port.

## Docker

### Build

```sh
docker build -t nats-workflows .
```

### Run the service

```sh
docker run -d --name nats-workflows \
  -p 8080:8080 \
  -v wf-data:/data \
  --env-file .env \
  nats-workflows
```

- `-p 8080:8080` — the HTTP API; Swagger UI at http://localhost:8080/api/docs.
- `-v wf-data:/data` — JetStream store (`NATS_DATA=/data` in the image);
  a named volume keeps runs across container restarts. Without it, state
  lives in an anonymous volume and is lost when the container is removed.
- `--env-file .env` — only needed for the `ask` workflow, which requires
  `OPENROUTER_API_KEY`. Omit if you don't use it.

No arguments means `serve`: the container runs the workflow executor, an
embedded NATS server listening on port 4222, and the HTTP API on port 8080.

### Run one-shot commands

While the service container is running, execute the other commands inside it —
they connect to the serving process over `127.0.0.1:4222`:

```sh
docker exec nats-workflows /nats-workflows list
docker exec nats-workflows /nats-workflows random
docker exec nats-workflows /nats-workflows order 600
docker exec nats-workflows /nats-workflows approve <runID>
docker exec nats-workflows /nats-workflows retry <runID>
```

Interactive commands (`choose`, `ask` without a question) need a TTY:

```sh
docker exec -it nats-workflows /nats-workflows choose
```

You can also run a one-shot command as its own container against the same
volume when nothing is serving — the binary embeds NATS in-process:

```sh
docker run --rm -v wf-data:/data nats-workflows list
```

Do **not** run this while the serve container is up: two embedded servers over
the same store directory would see mutually stale data. Use `docker exec`
against the running container instead.

### Environment variables

| Variable             | Default            | Purpose                                        |
| -------------------- | ------------------ | ---------------------------------------------- |
| `NATS_URL`           | (unset)            | Connect to an external NATS instead of embedding a server |
| `NATS_DATA`          | `/data` (in image) | JetStream store directory for the embedded server |
| `HTTP_ADDR`          | `:8080`            | Listen address of the HTTP API (`serve` mode)  |
| `PORT` / `HOST`      | (unset)           | Listen port and host supplied by Portless; `PORT` overrides `HTTP_ADDR` |
| `PORTLESS_URL`       | (unset)           | Public URL used in the startup log when running through Portless |
| `WORKFLOW_NAMESPACE` | (empty)            | Isolates this CLI's Runs, Hooks and queues; use the same value for all commands and workers |
| `OPENROUTER_API_KEY` | (unset)            | Required by the `ask` workflow                 |

### Note on port 4222

The embedded server binds to `127.0.0.1` inside the container, so publishing
the port (`-p 4222:4222`) does not make it reachable from the host. Use
`docker exec` as shown above, or point the app at an external NATS server via
`NATS_URL`. (The HTTP API is different: it binds all interfaces, which is why
`-p 8080:8080` works.)

## Scheduled runs

Add `--at` to a start command to create a Run for a future instant. Supply an RFC3339 timestamp with `Z` or an explicit timezone offset. The command prints the Run ID and returns immediately.

```sh
go run ./cmd/nats-workflows order 750 --at '2027-10-01T14:00:00+02:00'
go run ./cmd/nats-workflows ask --at '2027-10-01T12:00:00Z' 'Why is the sky blue?'
go run ./cmd/nats-workflows random --at '2027-10-01T12:00:00Z'
go run ./cmd/nats-workflows choose --at '2027-10-01T12:00:00Z'
go run ./cmd/nats-workflows reschedule RUN_ID '2027-10-02T12:00:00Z'
go run ./cmd/nats-workflows cancel RUN_ID
```

Keep `serve` running to execute due Runs. With an existing server, set `NATS_URL=nats://127.0.0.1:4222` for CLI commands. If workers are offline, overdue Runs start when they return. A Run never starts before its timestamp, but worker availability can delay it. Cancellation and rescheduling are available only while it is `scheduled`.

The HTTP API accepts `start_at` on creation:

```sh
curl localhost:8080/api/v1/runs -H 'Content-Type: application/json' \
  -d '{"workflow":"order","start_at":"2027-10-01T12:00:00Z","state":{"order":{"item":"model kit","qty":1,"amount":750}}}'
curl -X POST localhost:8080/api/v1/runs/RUN_ID/reschedule \
  -H 'Content-Type: application/json' -d '{"start_at":"2027-10-02T12:00:00Z"}'
curl -X POST localhost:8080/api/v1/runs/RUN_ID/cancel
```

Omit `start_at` for immediate execution. Past timestamps are rejected. Scheduled, running, and parked Runs have no expiry; done, failed, and cancelled Runs are cleaned up seven days after entering that status. Retrying a failed Run clears its expiry. Cleanup catches up when an executor resumes after downtime.

## Durable Hook input

Each Hook accepts one event. Steps declare a typed input contract and call
`engine.HookInput[T]` with a stable name. Register before contacting a service
that can call back. Later waits require distinct names and tokens.

HTTP Resume now requires `token`, `submission_id` and `payload`, and returns
`202` with an acceptance receipt. `POST /api/v1/hooks/submit` accepts the same
body for callbacks. Keep the token and submission ID across retries. Arbitrary
State patches are rejected. Acceptance is separate from Step completion.

The approval and choose commands still work. They resolve the Hook once and
keep a generated submission ID through transport retries. An optional final
argument supplies an ID: `approve <runID> approve <submissionID>` or
`choose <runID> <submissionID>`. A new invocation otherwise creates a new action.
`list` includes Hook tokens, status and acceptance time.

Accepted input survives Step failure and Retry. Terminal Run cleanup removes
payloads and receipts after seven days, but permanently reserves tokens. Cleaned
Hooks return `410`; unknown tokens return `404`. The default payload limit is
64 KiB; Hook storage and Step storage each default to 64 MiB with reject-new
behavior. Configure these through `engine.Config`. Permanent reservations count
toward Hook storage, so long-lived deployments must provision for their growth.

The API assumes trusted local/private access. Hook tokens identify waits and
are not authentication credentials. See [engine control flow](docs/engine-flow.md)
for registration, consumption and crash recovery.

## Webhooks

A Webhook is a Hook that an outside service calls over HTTP. Declare the Step
input with `engine.WebhookType[T](version, verify, accept)`, set
`engine.Config.WebhookURL` and mount `Engine.Webhooks()` at that address. In
the Step, `run.Webhook(ctx, name)` registers the Hook and returns its address;
send that to the service, then wait with `engine.HookInput[T]` under the same
name.

`verify` sees the headers and raw body and rejects the call with `401`.
`accept` declines input the Step does not wait for, such as intermediate
states; the call gets `202` and the Webhook keeps waiting. The body is the
input and its SHA-256 the submission ID. Duplicates, declined calls and calls
to used or cleaned-up Webhooks get `202`, so callers stop retrying. Unknown
tokens and plain Hooks get `404`. See [ADR 0005](docs/adr/0005-webhooks.md).

## Check Hook delivery

Follow the [manual Hook checks](docs/hooks-manual-check.md) to test approvals,
duplicate submissions, late callbacks, restart persistence and Retry through
the HTTP API. Each step includes commands and expected results.
