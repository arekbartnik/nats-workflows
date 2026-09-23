# Human approvals and external callbacks share durable delivery

Human approvals and external callbacks will use the same durable input-delivery mechanism, preserving the approval commands. This avoids separate acceptance and crash-recovery rules for two sources of input to a parked Run. Application-controlled handlers will validate incoming payloads; this release assumes trusted local/private access. Generated public callback URLs and public authentication integration are outside the initial scope.

Hooks accept one event each; later waits use separate hooks. This fits the current ordered-step model without adding durable event loops. Reusable hooks are outside the first release.

Resume will validate submissions against the current Hook's payload type instead of accepting arbitrary State patches. Existing approval commands remain, but unrelated State changes are rejected. This deliberately narrows the generic Resume API so human approvals and external callbacks obey the same input contract.

After durable registration, a Hook accepts input even before the Run parks. The first valid input wins; repeating its submission ID and payload returns the original receipt, while another submission or changed payload conflicts. Accepted input remains available when the receiving Step is retried. Hooks have no deadlines in the first release.

Support both engine-generated and caller-chosen Hook tokens. Tokens are unique across the engine and never reassigned, so late callbacks cannot reach later waits. This requires permanent token reservations even after Run cleanup. Registration uses a stable Hook name within the Run, so retrying registration recovers the same Hook and token.

Every submission carries an idempotency ID. Duplicate checks compare validated JSON values, ignoring whitespace and object field order. Keep payloads and receipts for the Run's lifetime and seven days after its terminal transition, preserving them if Retry wins before cleanup. After cleanup, return gone and keep the token reservation.

Allow one outstanding Hook per Run. Default the configurable JSON payload limit to 64 KiB and bound storage without evicting accepted input. Expose Hook progress through existing API and CLI run details, distinguishing acceptance from Step completion.

Use JetStream KV for token ownership, the accepted payload, and its receipt. A KV revision check selects the winning submission; persisted delivery intent and reconciliation publish work to the existing Step stream after interruptions. Single-use Hooks do not need a separate incoming-event stream. The accepted recovery protocol is documented in [the hooks proposal](../hooks-proposal.md).
