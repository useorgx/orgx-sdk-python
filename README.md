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


## Portable context continuation

```python
first = client.sync_context(workspace_id)
next_context = client.sync_context(workspace_id, previous=first)
evidence = client.expand_context_evidence(artifact_id, expected_version=2)
```

Retain the returned continuation, including its exact serialized bytes, between
calls. Each sync authenticates and prepares current context. The client validates
full or delta transfer hashes and retries a missing or corrupted base once with
a fresh read. Acknowledgement is transport state, not permission to act. Artifact
expansion with an expected version returns an API conflict if the revision changed.

## Prepared context and portable continuation

Version 1.1 adds `response_profile: "prepared"` (Python: `response_profile="prepared"`) to context preparation. This requests the compact direct response from the deployed API. Use the full profile for acknowledged delta transfer; prepared delivery and delta mode cannot be combined. The existing continuation helpers validate exact source bytes and recover a lost base with a fresh authorized read. Artifact expansion can require an exact current version. These protocol checks do not establish a native client handoff, task correctness, human acceptance, or a performance SLA.
