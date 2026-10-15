# Webhooks: single-event Hooks reachable by outside services

Services the application does not control call back over HTTP with their own body and their own signature. A Webhook is a Hook whose contract adds a verify function and an optional accept function, served by an engine-provided handler at an engine-generated address. Applications mount the handler and supply verification; they write no per-service route.

Webhooks keep the one-event model from ADR 0004. Callers such as job services send every state change, but a Step usually waits for one final state. The accept function declines the rest without storing them, and the handler answers them `202`, so the Hook keeps waiting. Hooks holding a queue of repeated events would need consumption cursors, new capacity and cleanup rules, and have no caller yet.

The handler answers `202` to anything that can no longer change the Run: a duplicate, a declined event, a used Hook or a cleaned-up one. Callers retry on other answers, and retrying those would never succeed. Unknown tokens and tokens of plain Hooks answer `404`, so plain Hooks stay unreachable from outside.

The request body is the Hook input and its SHA-256 the submission ID, because outside services send no idempotency ID the engine could use. Webhook contracts allow unknown object fields, since outside services add fields without notice; plain Hook contracts keep rejecting them.

Verification is a function the application supplies. Signature schemes differ per service and need the application's secrets, so the engine ships none. Engine-generated tokens are unguessable but are not a substitute for verification.
