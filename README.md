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

## Context continuation

`prepare_context`, `sync_context`, and `expand_context_evidence` use the existing context-pack and artifact APIs.
Sync requests a fresh full pack until the server supports verified coherent deltas.
Inspect `context_delivery`: current best-effort capsules do not establish action
authority, completeness, or measured model-token savings. Count expanded evidence
in the receiving model’s input budget. Requires the app context-delivery release.
