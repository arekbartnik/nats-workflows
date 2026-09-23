# Durable hooks proposal

Status: accepted and implemented. See [engine control flow](engine-flow.md) for the Go API, storage limits and recovery protocol.

## Reference behavior

[Workflow SDK hooks](https://workflow-sdk.dev/docs/foundations/hooks) let a workflow receive serializable payloads through a token. Hooks support repeated events, custom token ownership and conflict detection, and disposal. Registration must commit before the hook is ready to receive. Typed hook definitions can validate incoming payloads. Webhooks add generated public URLs, HTTP request serialization, and automatic or workflow-controlled responses.

## Fit with this engine

The engine executes an ordered list of Go steps. It persists State and the current Step, rather than replaying a suspended function. `ErrPark` acknowledges the step delivery; Resume re-enters that step with patched State. ADR 0002 deliberately makes `ErrPark` the only parking mechanism.

A hook can build on Park without adding another Run status. The agreed meaning is a durable, addressable input owned by a Run that accepts one event. This term is recorded in CONTEXT.md.

Repeated hooks would require an explicit durable event cursor and a way to process another event without advancing to the next Step. A blocking Go channel or an in-memory loop would not survive a restart.

## Initial scope

The first release accepts one event per hook. Typed JSON payload validation, durable registration, and asynchronous delivery through application-controlled HTTP handlers or a Go API form the proposed design. Reusable hooks are outside this release; workflow-generated HTTP responses are not proposed. This scope is a subset of the reference feature, not full parity.

Use the existing approval workflow as the first example. Existing human approvals and external callbacks will use the same durable delivery mechanism, preserving the existing CLI and HTTP experience.

## Agreed decisions

- Hooks are single-use. A Run may have separate Hooks for later waits.
- Support both engine-generated tokens and caller-chosen tokens. Tokens are unique across the engine and never assigned to another Hook. Keep a small permanent reservation even after the Run is deleted.
- A stable Hook name within a Run identifies registration retries. Re-registering that name returns the same Hook and token; a distinct wait requires a distinct name.
- Every submission requires an idempotency ID. HTTP callers provide it; the CLI generates one per user action and retains it across delivery retries.
- Compare validated JSON values for duplicate submissions, ignoring whitespace and object field order. Changed values under the same submission ID conflict.
- Use application-controlled handlers initially, with payload validation before delivery. This release assumes trusted local/private access and adds no public authentication scheme. Generated public callback URLs are outside the initial scope.
- Existing human approvals and external callbacks use the same durable delivery mechanism, with typed Hook input replacing arbitrary Resume patches.
- Accept and retain input after durable hook registration, including before the Run parks. Reject unknown hooks; register before contacting a callback-producing service.
- The first valid input wins. The same submission ID and payload returns the original receipt; a different submission or changed payload conflicts.
- Hooks have no deadlines in the first release.
- Retain accepted input for Retry of the failed Step. A new decision requires a new Hook.
- Resume validates input against the current Hook's payload type. Existing approval commands remain; unrelated State changes are rejected. This narrows the generic Resume API.

## Remaining agreed decisions

- Retain payloads and receipts for the Run's lifetime and seven days after its terminal transition. Retry before cleanup preserves them. After cleanup return gone, retaining only the permanent token reservation.
- Allow one open Hook per Run. Interpret this as one outstanding wait, including accepted input not yet consumed; later Steps can register their own Hooks.
- Default the configurable JSON payload limit to 64 KiB. Bound storage and reject new input when acceptance capacity is exhausted, without evicting accepted input.
- Show Hook name, token, status, and acceptance time in existing run details through the API and CLI. Show input acceptance separately from Step completion. No new frontend.
- Scope this release to trusted local/private deployment. The existing API has no authentication middleware. Public exposure and provider-signature verification are outside this release.

## Accepted NATS design

Use JetStream KV for Hook registration and its single accepted input, and the existing `WF_STEPS` stream for execution. A separate incoming-event stream was considered in the initial draft. Single-use Hooks do not need an event queue or cursor per Hook, and a KV revision check can select the winning submission before its handler returns.

`wf_runs` remains authoritative for Run progress, the outstanding wait, and the identity of input consumed by a successful Step. A `wf_hooks` KV bucket holds token ownership, registration identity, lifecycle, accepted payload, and immutable submission receipt. Encode or hash tokens for storage keys; never interpolate caller tokens into NATS subjects or wildcard expressions. Preserve ownership records permanently, stripping payload and receipt only during eligible cleanup.

Use separate revision-checked records as an explicitly recoverable protocol, not an assumed cross-record transaction. The Hook record selects the winning input; the Run record commits workflow progress. Neither record's update can silently replace a concurrent change.

### Register

1. Persist registration intent in the Run, including its stable Hook name, Step identity, and chosen token. Generate a token only for a new intent. Recover the same intent on retry.
2. Reserve the token with KV Create. An existing reservation for this exact registration is a retry; another owner is a conflict. Do not expose the Hook as ready until ownership and the Run's wait agree.
3. Reconcile interrupted registration from the stored intent. Reject attempts to change the token or payload contract of an existing registration. Report a token conflict without invoking the external service.
4. Register before invoking anything that can call back. Such external side effects still require their own idempotency keys because Step execution can repeat.

### Submit

Validate the size and typed payload, then use one Hook-record revision check to store the winning submission ID, normalized payload, receipt, and pending delivery intent. If another writer wins, read its result and return either the original receipt for an equivalent retry or a conflict. This same operation handles human approvals and external callbacks.

The receipt means input was durably accepted, not that the Run resumed or its Step completed. Persistence of this record is the acceptance boundary. If the response is lost, retrying the same submission recovers the receipt. Do not reject or replace an accepted input merely because later step-stream publication is unavailable.

Enforce acceptance capacity at the authoritative Hook store. Bound the step stream separately with reject-new behavior. If that stream fills after acceptance, preserve the input and retry dispatch when space becomes available. This distinguishes rejecting new input from delaying already accepted work.

### Deliver and consume

A KV watch can accelerate delivery, while executor reconciliation provides recovery after missed notifications or downtime. The accepted Hook record remains durable delivery intent until the Run records successful consumption or a terminal disposition.

When the Run is parked at the matching wait, use its revision to transition it to running with a persisted wakeup identity, then publish the Step. If publication fails or the executor crashes, reconciliation republishes using that same identity. Include the wait/wakeup identity in Step messages and reject stale generations; Msg-Id dedup alone is insufficient.

When input arrives while the Step is still running, retain it in the Hook record without overwriting in-flight State. The Step may read it before returning, or park and let reconciliation dispatch it. A successful Step commits its State, progress, and consumed-input identity together in the Run. Reconcile the Hook's consumed status afterward. These orderings cover a callback arriving between the Step's input check and its Park commit.

On Step failure, keep the accepted input. Retry re-enters the same Step with that input; it does not reopen the Hook for another answer. Failed Runs accept no new input until Retry makes them active again. Hooks without input remain recoverable with their Run until cleanup. Successful consumption permits the next distinct wait.

Run revision checks protect committed State. They do not guarantee external side effects occur exactly once if workers overlap or crash. Keep side-effect idempotency requirements explicit, and do not claim that JetStream dedup removes them.

### Cleanup

Base payload/receipt cleanup on the Run's terminal retention deadline. Coordinate cleanup with Retry through a revision-checked Run deletion claim, so cleanup cannot strip input from a concurrently retried Run. Clean the Hook only after the Run is irreversibly removed or through an equivalent fenced protocol. Keep token reservations permanently. Unknown tokens return not found; cleaned-up tokens return gone.

## API and operator behavior

Proposed operations are register a named Hook, submit typed input by token and submission ID, and inspect Hooks on a Run. Exact Go names and HTTP route spelling are implementation choices.

The existing Resume route and approval commands become adapters to Hook submission. They must carry the specific Hook identity as well as the submission ID, rather than resolving a delayed request to whichever wait is current. The CLI can resolve the Hook once when the user starts the action and retain that identity through retries. This deliberately changes the generic Resume contract, while preserving the approval workflow's user-facing commands.

Return an acceptance receipt for a winner or its identical retry. Distinguish validation errors, token or submission conflicts, unknown tokens, gone tokens, and capacity rejection. Expose pending, accepted, and consumed progress in run details; acceptance does not imply successful execution.

An HTTP caller must retain its submission ID across retries. A CLI transport retry retains its generated ID; a new CLI invocation is a new action unless an explicit ID is supplied. Tokens identify Hooks and are not authentication credentials in this design.

## Implementation sequence and verification

1. Add Hook types, validation, persistent registration, ownership, and first-winner acceptance.
2. Add revision-checked wakeup intent, Step-message generation checks, and reconciliation. Reuse the scheduled-start recovery approach without calling the current Resume save-then-enqueue path directly.
3. Convert the order workflow's approval waits and Resume adapters. Update the HTTP contract, OpenAPI documentation, CLI output, and engine-flow documentation.
4. Add payload/receipt cleanup with permanent reservations and storage limits.

Verify concurrent token registration, crash recovery at every cross-record transition, competing decisions, equivalent JSON retries, callbacks before Park, crashes after acceptance and before enqueue, callbacks after later waits begin, delivery beyond the dedup window, failure followed by Retry, capacity exhaustion, and cleanup racing with Retry. Integration tests should use the embedded NATS server and exercise actual persistence and redelivery.

## Design outcome

The accepted design is implemented in `engine/hooks.go`, the Resume adapters and the example workflows. Permanent token reservations grow with the number of Hooks. The bounded Hook bucket retains accepted input until Run cleanup, then keeps only the reservation. Integration tests exercise registration, acceptance, delivery and cleanup against embedded NATS.

Sources: [Workflow SDK hooks](https://workflow-sdk.dev/docs/foundations/hooks), [NATS KV](https://docs.nats.io/learn/key-value), [JetStream consumption](https://docs.nats.io/learn/jetstream/pull-consumers), and [publishing guarantees](https://docs.nats.io/learn/jetstream/advanced-publishing). Repository foundations: ADR 0002 for Park and ADR 0003 for recoverable publication.
