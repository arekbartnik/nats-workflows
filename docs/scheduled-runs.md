# Scheduled runs

Design agreed during the scheduling interview.

## Agreed behavior

- Scheduling creates a Run immediately with an ID, initial State, and scheduled status. It appears in reads and listings before execution.
- A scheduled Run starts no earlier than its start time, as soon as workers are available. Overdue Runs start after recovery.
- Pending Runs can be cancelled or rescheduled. Both operations reject requests once execution has begun.
- Any future date is allowed. Scheduled, running, and parked Runs have no expiry. Done, failed, and cancelled Runs expire seven days after entering that status. Retry clears the expiry.
- Scheduling is available through the Go engine, HTTP API, and existing CLI start commands. The API and CLI also expose cancellation and rescheduling. Swagger documentation describes these operations.
- API and CLI timestamps require an explicit timezone and are stored as UTC. Creation and rescheduling reject past timestamps. Omitting a start time preserves immediate execution.

## Implementation

The Go engine exposes `Schedule(ctx, workflow, initial, at)`, `Cancel(ctx, id)`, and `Reschedule(ctx, id, at)`. HTTP creation accepts an optional `start_at`; cancellation and rescheduling use `POST /api/v1/runs/{id}/cancel` and `POST /api/v1/runs/{id}/reschedule`.

Native scheduled messages live in `WF_SCHEDULES`, separate from ordinary step deliveries. Each schedule's subject includes its Run revision, so an older publisher cannot replace a newer schedule. The starter checks the revision and atomically changes scheduled to running before queuing Step 0. Its delivery stays unacknowledged until that step is durably queued. Cancellation and rescheduling compete against that transition; a successful cancellation prevents execution, and a losing operation returns a conflict.

Each executor reconciles stored scheduling intent on startup and whenever a Run changes. It repairs missing publications, and every second it publishes overdue starts and removes obsolete schedule records. KV revision checks protect terminal cleanup from concurrent Retry. Cleanup happens while an executor runs; after downtime it catches up on startup. Existing terminal Runs without an explicit expiry use their last update plus seven days.

Steps retain the engine's existing redelivery semantics. A crash during a Step can execute its side effects again; scheduling does not provide exactly-once side effects.
