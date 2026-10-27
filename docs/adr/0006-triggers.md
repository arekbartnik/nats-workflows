# Triggers: one address that starts a Run per delivery

Some services keep one callback URL for many events over months and replace it with each new request. A Webhook belongs to one wait of one Run and takes one event, so such a service would reach a used Hook and get `202` for every later call. A Trigger is a stable token, chosen by the application and bound to a workflow. Each verified call starts a new Run of that workflow with the body as its initial State.

Starting a Run per delivery keeps the one-event model of ADR 0004 and ADR 0005. Hooks that queue repeated events would need consumption cursors, a Step that handles an event without advancing, and new cleanup rules. A Run per delivery needs none of these, because the stored Run is the acceptance boundary and `StartWithID` already makes a repeated start a no-op.

The Run ID is a UUIDv5 of the token and a delivery key. The application supplies the key, because services often resend a delivery with a changed send time. The body's SHA-256 is the fallback. Delivery keys are external text, and raw IDs fail as KV keys or subjects for characters such as `.`, `:`, `*` and spaces. The UUID is always valid and scoped to its token.

The handler answers `202` whenever the Run exists, including when publishing its first Step failed, because reconciliation publishes it. It answers `503` only when no Run was stored, within five seconds, so callers may always retry. Bodies are capped below the NATS message size, since a Run that cannot be stored would otherwise answer `503` on every retry.

Trigger tokens share the token space of Hooks and are reserved when the Engine is created. Without the reservation, a Trigger would silently take the address of a parked Hook with the same token. `New` refuses a token held by a Hook or bound to another workflow. A removed Trigger keeps its reservation, and calls to it get `202`, like a cleaned-up Hook.

Verification stays with the application, as in ADR 0005. The Trigger's input comes from `WebhookType`, so a body the State type cannot hold is rejected with `400` before a Run exists.
