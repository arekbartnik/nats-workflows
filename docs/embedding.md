# Embedding workflows in a Go app

The library module is `github.com/arekbartnik/nats-workflows`. Import `engine`
for workflow execution. The `embedded` and `api` packages are optional.

See [the runnable example](../examples/embedded/main.go) for a complete app.
Run it with `go run ./examples/embedded -data /path/to/exclusive/store`.

## Connection and storage ownership

Pass your app's `*nats.Conn` to `engine.New`. The connection must reach a server
with JetStream and native message scheduling enabled. The test suite uses the
NATS server version pinned in `go.mod`; use that version when setting up an
external server unless you have verified compatibility with another version.

`New` validates configuration and workflow definitions, then creates or updates
the engine's streams, consumers and KV buckets. Its connection therefore needs
permission to manage those resources. Provisioning can fail partway through;
fix the configuration or connection and call `New` again. Construction does not
launch any worker goroutines.

For an embedded server:

```go
srv, err := embedded.Start(embedded.Config{
    StoreDir: "/var/lib/myapp/workflows",
})
if err != nil {
    return err
}
defer srv.Close()

eng, err := engine.New(ctx, srv.Conn(), logger,
    engine.Config{Namespace: "myapp"}, workflows...)
```

The storage directory is required and must belong exclusively to this server.
The helper does not probe for other servers or choose a default data directory.
It opens no TCP listener by default. Set `ListenAddr: "127.0.0.1:4222"` to let
other processes connect, or use port `0` and read `srv.ClientURL()` for a free
port. `Close` closes its connection and waits for the server to stop. Repeated
calls are safe, and persisted data remains in the directory.

The standalone CLI still reads `NATS_URL` and `NATS_DATA` and can connect to an
existing local server. Those choices belong to the CLI, not the library.

## Define workflows in the host app

Build an `engine.Workflow` from named Steps. Use `engine.Typed` to decode and
persist the workflow's State. Capture app dependencies in the Step function:

```go
func orderWorkflow(orders *OrderService) engine.Workflow {
    return engine.Workflow{
        Name: "order-v1",
        Steps: []engine.Step{{
            Name: "reserve",
            Run: engine.Typed(func(ctx context.Context, run *engine.Run, s *OrderState) error {
                return orders.Reserve(ctx, run.ID, s)
            }),
        }},
    }
}
```

`OrderService` and `OrderState` are types supplied by your app. Pass its HTTP
clients, database handles and other dependencies through closures like this.
Steps must honor context cancellation and tolerate redelivery after a crash.
A Step may run longer than the ack wait: the engine heartbeats its delivery and
cancels its context after `Step.Timeout`, which defaults to the first `BackOff`
delay. `Config.Concurrency` sets how many Steps one process runs at once.
Use a stable idempotency key for external effects, such as the Run ID plus the
Step name. A successful external call can be repeated if the process stops
before the engine persists the next State.

`New` rejects blank or duplicate workflow names, empty workflows, blank or
duplicate Step names, nil Step functions, invalid input contracts and invalid
configuration. It copies Step definitions and input contracts; changing the
original slices or a `Workflows()` result does not change registration.
A nil logger uses `slog.Default()`.

Use `engine.HookType[T]` and `engine.HookInput[T]` for human or callback input.
The engine persists accepted input and reuses it on Retry. See the
[Hook documentation](../README.md#durable-hook-input) for submission semantics.

## Worker lifetime

`New` provisions storage. `Serve` executes Steps, starts due Runs, repairs
missed scheduling and Hook publications, and cleans up terminal Runs.
At least one worker must be serving for Runs to progress.

The host app must supervise the worker's error and own its cancellation:

```go
workerCtx, stopWorker := context.WithCancel(appCtx)
workerDone := make(chan error, 1)
go func() { workerDone <- eng.Serve(workerCtx) }()

// While serving requests, also monitor workerDone for unexpected failure.
// On shutdown, stop accepting new work, then:
stopWorker()
if err := <-workerDone; err != nil {
    return err
}
// Now it is safe to close the NATS connection or embedded server.
```

The example above illustrates ownership; the runnable example also propagates
worker failure to the caller waiting for a Run. If the host consumes the result
while supervising, retain it instead of reading the channel again at shutdown.

Cancellation stops consumption, cancels active Step contexts, and waits for
callbacks to return. `Serve` returns nil after cancellation and an error if
consumer startup fails or a consumer stops unexpectedly. A Step that ignores
cancellation can delay shutdown. Unfinished work remains subject to JetStream
redelivery; stopping a worker does not cancel its Runs.

Only one `Serve` call may run on an Engine at a time; a second call returns
`engine.ErrServing`. After it returns, the Engine may serve again. The host
retains ownership of the NATS connection throughout. No `Engine.Close` is
needed because the engine does not own that connection.

## Namespaces and multiple workers

Set `Config.Namespace` for independent apps sharing a NATS account. It accepts
up to 64 ASCII letters, digits, underscores or hyphens and is case-sensitive.
The empty value preserves the standalone application's original storage names.

| Resource | Empty namespace | Namespace `orders` |
| --- | --- | --- |
| Step stream | `WF_STEPS` | `WF_STEPS_orders` |
| Schedule stream | `WF_SCHEDULES` | `WF_SCHEDULES_orders` |
| Run bucket | `wf_runs` | `wf_runs_orders` |
| Hook bucket | `wf_hooks` | `wf_hooks_orders` |
| Step subjects | `wf.step.>` | `wf_orders.step.>` |
| Schedule subjects | `wf.schedule.>` | `wf_orders.schedule.>` |
| Start subjects | `wf.start.>` | `wf_orders.start.>` |

Durable consumers belong to their namespace's streams. Hook token reservations
are unique within a namespace. Two namespaces may use the same Hook token
without sharing its input. Namespaces isolate resource names; NATS account
permissions and the host's authentication still control access.

Workers sharing a namespace share the queue and must register the same
compatible workflow definitions and use the same engine configuration. Each
worker must be able to execute every workflow in that queue. `New` updates
existing resources, so starting another worker with different limits or retry
settings changes shared configuration.

Persisted Runs refer to a workflow name and Step index. Give incompatible
workflow changes a new name, such as `order-v2`, and retain `order-v1` until its
Runs have finished. Do not reorder Steps under an existing name while old Runs
can still execute or retry. Hook contracts also include their input type's Go
package path and explicit version; keep these stable for existing waits.

Switching a namespace does not move any data. See the
[upgrade notes](../README.md#upgrade-from-the-standalone-layout) for the module
rename and the relocated sample workflows.

## HTTP integration

`api.New(eng, logger).Handler()` returns an `http.Handler`. The host app owns
the HTTP server, routing, authentication middleware and shutdown. The handler
exposes `/api/v1`, `/api/health`, `/api/docs`, and `/openapi.yaml` at their
existing paths. Its bundled docs use root-relative paths.

The library adds no authentication. Hook tokens identify waits and do not
authorize their callers. An app can instead call engine methods directly from
its own authenticated handlers and omit the `api` package.

## Local development before publishing

In a consuming app, point the module at this checkout:

```sh
go mod edit -require=github.com/arekbartnik/nats-workflows@v0.0.0
go mod edit -replace=github.com/arekbartnik/nats-workflows=/absolute/path/to/nats-workflows
go mod tidy
```

Remove the replacement and choose a published version when the repository is
available remotely. The library, optional packages, examples and CLI are in one
Go module. Importing only `engine` does not link the embedded server or HTTP
handler into the consuming executable.
