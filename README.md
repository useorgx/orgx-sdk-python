# OrgX Python client

Dependency-free client for OrgX REST API v1.

```python
from orgx_client import OrgXClient

orgx = OrgXClient(api_key="oxk_...")
created = orgx.create_work(
    "Review the launch plan",
    idempotency_key="launch-plan-review-001",
)
orgx.complete_work(
    created["taskId"],
    created["task"]["updated_at"],
    created["aggregateVersion"],
    evidence={"reviewed_sections": 12, "broken_links": 0},
    idempotency_key="launch-plan-review-complete-001",
)
```

API reference: https://docs.useorgx.com/docs/api/overview

## Context delivery

The OrgX 1.1 source supports prepared delivery and portable full/delta continuation.
Registry publication is tracked in [Release setup](RELEASING.md).

### Prepared context

```python
prepared = orgx.prepare_context(workspace_id, response_profile="prepared")
```

Prepared delivery requests a compact direct response. It cannot be combined with
delta mode. Inspect `context_delivery` for source consistency and completeness;
context delivery does not grant authority to act.

### Portable continuation

```python
first = orgx.sync_context(workspace_id)
next_context = orgx.sync_context(workspace_id, previous=first)
evidence = orgx.expand_context_evidence(artifact_id, expected_version=2)
```

Retain the returned continuation, including its exact serialized bytes, between
calls. Each sync authenticates and prepares current context using the full profile.
The server selects a delta only when it is smaller; otherwise it sends full state.
The client validates transfer hashes and repairs a missing or corrupted retained
base with one fresh read. The older `acknowledged_capsule_id` form requests a full rebootstrap.

Artifact expansion with an expected version returns a conflict if the current
revision differs. Include expanded evidence in the receiving model's input budget.

Transport savings do not establish model-token savings, task correctness, human
acceptance, or a performance SLA.
