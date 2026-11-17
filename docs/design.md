# Design notes

The prototype separates task policy from persistence. Each state change checks a caller-provided approval function, acquires a host-local lock, appends a hash-chained JSON event, and atomically replaces a checkpoint. If an event exists without a matching checkpoint, reads stop for reconciliation.

## Current boundaries

- The approval callback is an integration point, not proof of an approved policy. A real deployment needs an independently anchored, verified authority.
- Locks protect cooperating processes on one host. Shared-folder synchronization does not provide distributed exclusion or protect against an offline former coordinator.
- A real controller must resolve uncertain side effects before retrying and independently verify results against exact artifact revisions.
- No worker invocation, service, watchdog, multi-host transfer, production approval, or cost enforcement is implemented here.
- All examples and tests use disposable synthetic data.