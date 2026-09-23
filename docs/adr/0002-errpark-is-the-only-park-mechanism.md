# ErrPark is the only park mechanism

The engine originally had two ways to park a run: a `Step.Approval` predicate (the engine peeked at the *next* step's predicate after each step and parked before delivering it) and `ErrPark` (a step returns it mid-execution). One concept, two mechanisms — and the fraud-review step needed both at once: a gate to park before it, plus an `ErrPark` fallback for "resumed without a decision". `Step.Approval` and the `Always` helper were deleted; `ErrPark` is the single mechanism, and the Step interface shrank to `Name` + `Run`.

**The protocol that replaces the gate:** a parking step keys on a state field its Resume patch must set, and *consumes* (clears) that field when it proceeds. Consumption is what lets one generic field (e.g. `Decision`) serve several parks in the same workflow — without it, the answer to an earlier park would silently ack the next one. A Resume whose patch leaves the input missing simply parks the run again; that is correct behaviour, not an error.

**Trade-off accepted:** a gated step used to park *undelivered* — zero executions before approval. Under `ErrPark` the step runs at least once (and once more per Resume), so every parking step must be safe to enter without its input — which the protocol's "check input first, park if missing" shape gives for free. Park still acks the message, so a parked run has no message in flight and consumes no delivery budget while parked.

**Rejected alternative:** an engine-tracked `Resumed` flag on the Run, so steps could ask "was I resumed?" without a state field. Rejected because it hand-rolls durable engine state for something the state patch already expresses, and crash-redelivery of a resumed-but-reparked run makes the flag ambiguous.

At the same time the status spelling `waiting_approval` was renamed to `parked`: approval is one *reason* to park, not the operation, and the domain term (CONTEXT.md) was always Park.
