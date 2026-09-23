# Durable scheduled runs

Scheduling creates the Run immediately, with its initial State and a future start time. NATS native scheduled messages trigger execution, while a KV revision check arbitrates starting, cancellation, and rescheduling. Schedule subjects include that revision so a delayed publisher cannot replace a newer schedule.

NATS cannot atomically commit a Run in KV and publish a schedule in another stream. The Run is therefore the durable intent, and executor maintenance repairs missing publications after crashes. A separate durable starter consumer keeps its delivery unacknowledged until Step 0 is queued, covering the crash between claiming the Run and publishing its first step. This adds reconciliation but preserves pending-run visibility and cancellation up to the execution claim.

The former bucket-wide seven-day TTL would delete distant schedules and long-lived parked Runs. We remove it and purge terminal Runs seven days after their terminal transition, using revision checks so concurrent Retry cannot lose its state. Active Runs have no expiry; maintenance requires a running executor and catches up after downtime.
