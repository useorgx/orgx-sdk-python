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
