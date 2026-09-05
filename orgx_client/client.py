"""Dependency-free transport for the OrgX v1 OpenAPI contract.

The client intentionally owns only HTTP concerns. Authorization, workspace
scope, idempotency semantics, and lifecycle transitions remain server-owned.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, MutableMapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class OrgXApiError(RuntimeError):
    """An HTTP error returned by the OrgX API."""

    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.details = details


class EventStreamSubscription:
    """Blocking, reconnectable iterator over the v1 SSE ledger transport."""

    def __init__(self, response: Any):
        self._response = response
        self._closed = False

    def __iter__(self) -> Iterator[tuple[Mapping[str, Any], str]]:
        event_name = "message"
        event_id = ""
        data_lines: list[str] = []
        for raw_line in self._response:
            if self._closed:
                break
            line = raw_line.decode("utf-8").rstrip("\r\n")
            if line == "":
                if data_lines:
                    data = "\n".join(data_lines)
                    if event_name == "ledger_event":
                        yield json.loads(data), event_id
                    elif event_name == "error":
                        payload = json.loads(data)
                        raise OrgXApiError(
                            503,
                            payload.get("code", "event_stream_unavailable"),
                            payload.get(
                                "message", "OrgX event stream unavailable"
                            ),
                        )
                event_name = "message"
                event_id = ""
                data_lines = []
                continue
            if line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if separator and value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_name = value
            elif field == "id":
                event_id = value
            elif field == "data":
                data_lines.append(value)

    def close(self) -> None:
        self._closed = True
        self._response.close()

    def __enter__(self) -> "EventStreamSubscription":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


@dataclass(frozen=True)
class OrgXClient:
    """Synchronous OrgX v1 client using Python's standard library only."""

    api_key: Optional[str] = None
    token: Optional[str] = None
    base_url: str = "https://useorgx.com/api/v1"
    timeout_seconds: float = 30.0

    def prepare_context(self, workspace_id: str, *, initiative_id: Optional[str] = None,
                        workstream_id: Optional[str] = None, task_id: Optional[str] = None,
                        acknowledged_capsule_id: Optional[str] = None) -> Mapping[str, Any]:
        """Prepare current context; this response does not grant action authority."""
        body = {"workspace_id": workspace_id}
        for key, value in (("initiative_id", initiative_id), ("workstream_id", workstream_id),
                           ("task_id", task_id), ("acknowledged_capsule_id", acknowledged_capsule_id)):
            if value is not None:
                body[key] = value
        return self._request("/context-pack", method="POST", body=body)["data"]

    def sync_context(self, workspace_id: str, acknowledged_capsule_id: str,
                     **scope: Any) -> Mapping[str, Any]:
        """Request full rebootstrap until coherent base verification is available."""
        return self.prepare_context(workspace_id, acknowledged_capsule_id=acknowledged_capsule_id, **scope)

    def expand_context_evidence(self, artifact_id: str) -> Mapping[str, Any]:
        """Read an artifact through the existing authorized API; account for its tokens."""
        return self._request(f"/artifacts/{quote(artifact_id, safe='')}")["data"]

    def start_discovery_run(
        self,
        workspace_id: str,
        *,
        idempotency_key: str,
        mode: str = "bounded_sync",
        query: Optional[str] = None,
        source_kinds: Optional[list[str]] = None,
    ) -> Mapping[str, Any]:
        return self._request(
            "/discovery-runs",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "mode": mode,
                "query": query,
                "source_kinds": source_kinds or [],
            },
        )["data"]

    def list_discovery_runs(self, workspace_id: str, *, limit: int = 20) -> list[Mapping[str, Any]]:
        payload = self._request(
            f"/discovery-runs?workspace_id={quote(workspace_id)}&limit={limit}"
        )
        return list(payload["data"])

    def get_discovery_run(self, workspace_id: str, run_id: str) -> Mapping[str, Any]:
        payload = self._request(
            f"/discovery-runs/{quote(run_id)}?workspace_id={quote(workspace_id)}"
        )
        return payload["data"]

    def propose_process_from_discovery(
        self,
        workspace_id: str,
        discovery_run_id: str,
        process_candidate_id: str,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/discovery-runs/{quote(discovery_run_id)}/propose",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "process_candidate_id": process_candidate_id,
            },
        )["data"]

    def list_operating_processes(self, workspace_id: str) -> list[Mapping[str, Any]]:
        payload = self._request(
            f"/operating-processes?workspace_id={quote(workspace_id)}"
        )
        return list(payload["data"])

    def list_episodes(self, workspace_id: str, *, limit: int = 50) -> list[Mapping[str, Any]]:
        payload = self._request(
            f"/episodes?workspace_id={quote(workspace_id)}&limit={limit}"
        )
        return list(payload["data"])

    def list_events(
        self,
        workspace_id: str,
        *,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        event_types: Optional[list[str]] = None,
        aggregate_type: Optional[str] = None,
    ) -> Mapping[str, Any]:
        params = [f"workspace_id={quote(workspace_id)}"]
        if cursor:
            params.append(f"cursor={quote(cursor)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        for event_type in event_types or []:
            params.append(f"event_type={quote(event_type)}")
        if aggregate_type:
            params.append(f"aggregate_type={quote(aggregate_type)}")
        return self._request(f"/events/stream?{'&'.join(params)}")

    def subscribe_events(
        self,
        workspace_id: str,
        *,
        after: Optional[str] = None,
        limit: Optional[int] = None,
        event_types: Optional[list[str]] = None,
        aggregate_type: Optional[str] = None,
        stream_ms: Optional[int] = None,
        poll_ms: Optional[int] = None,
    ) -> EventStreamSubscription:
        """Open a bounded SSE lease and iterate ``(event, opaque_cursor)`` pairs."""

        params = [
            f"workspace_id={quote(workspace_id)}",
            "transport=sse",
        ]
        if after:
            params.append(f"after={quote(after)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        if stream_ms is not None:
            params.append(f"stream_ms={quote(str(stream_ms))}")
        if poll_ms is not None:
            params.append(f"poll_ms={quote(str(poll_ms))}")
        if aggregate_type:
            params.append(f"aggregate_type={quote(aggregate_type)}")
        for event_type in event_types or []:
            params.append(f"event_type={quote(event_type)}")

        headers = {"Accept": "text/event-stream"}
        credential = self.api_key or self.token
        if credential:
            headers["Authorization"] = f"Bearer {credential}"
        request = Request(
            f"{self.base_url.rstrip('/')}/events/stream?{'&'.join(params)}",
            headers=headers,
            method="GET",
        )
        try:
            response = urlopen(request, timeout=self.timeout_seconds)
        except HTTPError as error:
            try:
                error_payload = json.loads(error.read().decode("utf-8"))
            except (OSError, ValueError):
                error_payload = {}
            detail = (
                error_payload.get("error", {})
                if isinstance(error_payload, dict)
                else {}
            )
            raise OrgXApiError(
                error.code,
                detail.get("code", "event_stream_failed"),
                detail.get("message", "OrgX event stream failed"),
                detail.get("details"),
            ) from error
        except URLError as error:
            raise OrgXApiError(0, "transport_failed", str(error.reason)) from error
        return EventStreamSubscription(response)

    def get_work_ledger(
        self,
        workspace_id: str,
        *,
        from_iso: Optional[str] = None,
        to_iso: Optional[str] = None,
        granularity: Optional[str] = None,
        timezone: Optional[str] = None,
        include_source_health: Optional[bool] = None,
    ) -> MutableMapping[str, Any]:
        params = [f"workspace_id={quote(workspace_id)}"]
        if from_iso:
            params.append(f"from={quote(from_iso)}")
        if to_iso:
            params.append(f"to={quote(to_iso)}")
        if granularity:
            params.append(f"granularity={quote(granularity)}")
        if timezone:
            params.append(f"timezone={quote(timezone)}")
        if include_source_health is False:
            params.append("include_source_health=false")
        return self._request(f"/projections/work-ledger?{'&'.join(params)}")

    def get_adoption_projection(
        self, workspace_id: str, process_id: str
    ) -> MutableMapping[str, Any]:
        """Read evidence-gated OperatingProcess adoption metrics.

        Behavioral and self-reported adoption are returned separately. The
        server returns explicit limitations when source metrics are absent;
        bound episode count is provenance context, not an adoption denominator.
        """
        params = [
            f"workspace_id={quote(workspace_id)}",
            f"process_id={quote(process_id)}",
        ]
        return self._request(f"/projections/adoption?{'&'.join(params)}")

    def get_value_case_projection(
        self, workspace_id: str, process_id: str
    ) -> MutableMapping[str, Any]:
        """Read an evidence-gated process ValueCase projection."""
        params = [
            f"workspace_id={quote(workspace_id)}",
            f"process_id={quote(process_id)}",
        ]
        return self._request(f"/projections/value-case?{'&'.join(params)}")

    def get_meter_usage_projection(
        self,
        workspace_id: str,
        *,
        from_iso: Optional[str] = None,
        to_iso: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Read immutable meter-event usage evidence.

        The response is explicitly non-billing-ready until verified provider
        ingress, rating, entitlement, invoice, and double-billing controls are
        proven in the deployment environment.
        """
        params = [f"workspace_id={quote(workspace_id)}"]
        if from_iso:
            params.append(f"from={quote(from_iso)}")
        if to_iso:
            params.append(f"to={quote(to_iso)}")
        return self._request(f"/projections/meter-usage?{'&'.join(params)}")

    def list_handoffs(self, workspace_id: str) -> list[Mapping[str, Any]]:
        payload = self._request(f"/handoffs?workspace_id={quote(workspace_id)}")
        return list(payload["data"])

    def create_work(
        self,
        title: str,
        *,
        idempotency_key: str,
        workspace_id: Optional[str] = None,
        command_id: Optional[str] = None,
        initiative_id: Optional[str] = None,
        workstream_id: Optional[str] = None,
        milestone_id: Optional[str] = None,
        description: Optional[str] = None,
        priority: str = "medium",
        due_date: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        estimated_cost_cents: int = 0,
        causation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {
            "title": title,
            "description": description,
            "priority": priority,
            "due_date": due_date,
            "metadata": dict(metadata or {}),
            "estimated_cost_cents": estimated_cost_cents,
            "causation_id": causation_id,
            "correlation_id": correlation_id,
        }
        if workspace_id:
            body["workspace_id"] = workspace_id
        if command_id:
            body["command_id"] = command_id
        hierarchy = (initiative_id, workstream_id, milestone_id)
        if any(hierarchy) and not all(hierarchy):
            raise ValueError(
                "initiative_id, workstream_id, and milestone_id must be supplied together"
            )
        if all(hierarchy):
            body.update(
                initiative_id=initiative_id,
                workstream_id=workstream_id,
                milestone_id=milestone_id,
            )
        return self._request(
            "/work",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )["data"]

    def complete_work(
        self,
        task_id: str,
        expected_updated_at: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
        summary: Optional[str] = None,
        evidence: Optional[Mapping[str, Any]] = None,
        cost_cents: int = 0,
        causation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/work/{quote(task_id)}/complete",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "expected_updated_at": expected_updated_at,
                "expected_aggregate_version": expected_aggregate_version,
                "summary": summary,
                "evidence": dict(evidence or {}),
                "cost_cents": cost_cents,
                "causation_id": causation_id,
                "correlation_id": correlation_id,
            },
        )["data"]

    def get_handoff(self, workspace_id: str, handoff_id: str) -> Mapping[str, Any]:
        payload = self._request(
            f"/handoffs/{quote(handoff_id)}?workspace_id={quote(workspace_id)}"
        )
        return payload["data"]

    def create_handoff(
        self,
        workspace_id: str,
        handoff_key: str,
        from_stage_key: str,
        to_stage_key: str,
        title: str,
        *,
        idempotency_key: str,
        handoff_id: Optional[str] = None,
        source_process_ref: Optional[str] = None,
        source_revision_ref: Optional[str] = None,
        summary: Optional[str] = None,
        priority: str = "normal",
        sla_minutes: Optional[int] = None,
        due_at: Optional[str] = None,
        proof_requirements: Optional[list[Mapping[str, Any]]] = None,
    ) -> Mapping[str, Any]:
        return self._request(
            "/handoffs",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "handoff_id": handoff_id,
                "handoff_key": handoff_key,
                "from_stage_key": from_stage_key,
                "to_stage_key": to_stage_key,
                "source_process_ref": source_process_ref,
                "source_revision_ref": source_revision_ref,
                "title": title,
                "summary": summary,
                "priority": priority,
                "sla_minutes": sla_minutes,
                "due_at": due_at,
                "proof_requirements": proof_requirements or [],
            },
        )["data"]

    def claim_handoff(
        self,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/handoffs/{quote(handoff_id)}/claim",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "expected_aggregate_version": expected_aggregate_version,
            },
        )["data"]

    def fulfill_handoff(
        self,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        result: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/handoffs/{quote(handoff_id)}/fulfill",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "expected_aggregate_version": expected_aggregate_version,
                "result": dict(result),
            },
        )["data"]

    def propose_operating_process(
        self,
        workspace_id: str,
        process: Mapping[str, Any],
        revision: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            "/operating-processes",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "process": dict(process),
                "revision": dict(revision),
            },
        )["data"]

    def confirm_operating_process(
        self,
        workspace_id: str,
        process_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._transition(
            "confirm", workspace_id, process_id, expected_aggregate_version, idempotency_key
        )

    def activate_operating_process(
        self,
        workspace_id: str,
        process_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._transition(
            "activate", workspace_id, process_id, expected_aggregate_version, idempotency_key
        )

    def get_operating_process(
        self, workspace_id: str, process_id: str
    ) -> Mapping[str, Any]:
        payload = self._request(
            f"/operating-processes/{quote(process_id)}?workspace_id={quote(workspace_id)}"
        )
        return payload["data"]

    def get_operating_map(
        self, workspace_id: str, *, limit: Optional[int] = None
    ) -> MutableMapping[str, Any]:
        params = [f"workspace_id={quote(workspace_id)}"]
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        return self._request(f"/operating-map?{'&'.join(params)}")

    def return_handoff(
        self,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._handoff_transition(
            "return", workspace_id, handoff_id, expected_aggregate_version, idempotency_key
        )

    def escalate_handoff(
        self,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._handoff_transition(
            "escalate", workspace_id, handoff_id, expected_aggregate_version, idempotency_key
        )

    def cancel_handoff(
        self,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        *,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._handoff_transition(
            "cancel", workspace_id, handoff_id, expected_aggregate_version, idempotency_key
        )

    def list_work(
        self,
        workspace_id: str,
        *,
        initiative_id: Optional[str] = None,
        status: Optional[str] = None,
        updated_since: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> MutableMapping[str, Any]:
        """List owned work items; pass ``meta["nextCursor"]`` back as ``cursor``."""
        params = [f"workspace_id={quote(workspace_id)}"]
        if initiative_id:
            params.append(f"initiative_id={quote(initiative_id)}")
        if status:
            params.append(f"status={quote(status)}")
        if updated_since:
            params.append(f"updated_since={quote(updated_since)}")
        if cursor:
            params.append(f"cursor={quote(cursor)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        return self._request(f"/work?{'&'.join(params)}")

    def get_work_task(self, workspace_id: str, task_id: str) -> Mapping[str, Any]:
        """Read one work item with the ``concurrency`` block completion requires.

        Echo ``concurrency.expected_updated_at`` back to ``complete_work``
        exactly as received; the server compares it for exact equality.
        """
        payload = self._request(
            f"/work/{quote(task_id)}?workspace_id={quote(workspace_id)}"
        )
        return payload["data"]

    def create_initiative(
        self,
        *,
        idempotency_key: str,
        workspace_id: Optional[str] = None,
        title: Optional[str] = None,
        summary: Optional[str] = None,
        plan: Optional[Mapping[str, Any]] = None,
        plan_digest: Optional[str] = None,
        proposal_id: Optional[str] = None,
        proposal_digest: Optional[str] = None,
        initiative_id: Optional[str] = None,
        overrides: Optional[Mapping[str, Any]] = None,
        expected_aggregate_version: Optional[int] = None,
    ) -> Mapping[str, Any]:
        """Create an initiative in one of the three contract forms.

        Supply ``proposal_id`` + ``proposal_digest`` to commit a reviewed
        proposal, ``plan`` + ``plan_digest`` to commit an inline plan, or
        ``title`` (with optional ``summary``) for the starter scaffold.
        """
        body: dict[str, Any] = {}
        if workspace_id:
            body["workspace_id"] = workspace_id
        if proposal_id or proposal_digest:
            if not (proposal_id and proposal_digest):
                raise ValueError(
                    "proposal_id and proposal_digest must be supplied together"
                )
            body["proposal_id"] = proposal_id
            body["proposal_digest"] = proposal_digest
        elif plan is not None or plan_digest:
            if plan is None or not plan_digest:
                raise ValueError("plan and plan_digest must be supplied together")
            body["plan"] = dict(plan)
            body["plan_digest"] = plan_digest
        elif title:
            body["title"] = title
            if summary is not None:
                body["summary"] = summary
        else:
            raise ValueError(
                "supply proposal_id+proposal_digest, plan+plan_digest, or title"
            )
        if "title" not in body:
            if initiative_id:
                body["initiative_id"] = initiative_id
            if overrides is not None:
                body["overrides"] = dict(overrides)
            if expected_aggregate_version is not None:
                body["expected_aggregate_version"] = expected_aggregate_version
        return self._request(
            "/initiatives",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )["data"]

    def get_initiative(
        self,
        initiative_id: str,
        *,
        workspace_id: Optional[str] = None,
        include: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Read an initiative; ``include`` is comma-separated (``tree,launches``)."""
        params = []
        if workspace_id:
            params.append(f"workspace_id={quote(workspace_id)}")
        if include:
            params.append(f"include={quote(include)}")
        suffix = f"?{'&'.join(params)}" if params else ""
        payload = self._request(f"/initiatives/{quote(initiative_id)}{suffix}")
        return payload["data"]

    def propose_initiative_scaffold(
        self,
        title: str,
        *,
        idempotency_key: str,
        workspace_id: Optional[str] = None,
        summary: Optional[str] = None,
        prompt: Optional[str] = None,
        goal_ids: Optional[list[str]] = None,
        context: Optional[Mapping[str, Any]] = None,
        depth: Optional[str] = None,
        workstreams: Optional[list[Mapping[str, Any]]] = None,
        agent_assignment: Optional[str] = None,
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {"title": title}
        if workspace_id:
            body["workspace_id"] = workspace_id
        if summary is not None:
            body["summary"] = summary
        if prompt is not None:
            body["prompt"] = prompt
        if goal_ids is not None:
            body["goal_ids"] = list(goal_ids)
        if context is not None:
            body["context"] = dict(context)
        if depth:
            body["depth"] = depth
        if workstreams is not None:
            body["workstreams"] = [dict(workstream) for workstream in workstreams]
        if agent_assignment:
            body["agent_assignment"] = agent_assignment
        return self._request(
            "/initiatives/proposals",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )["data"]

    def get_initiative_scaffold_proposal(
        self, proposal_id: str, *, workspace_id: Optional[str] = None
    ) -> Mapping[str, Any]:
        suffix = f"?workspace_id={quote(workspace_id)}" if workspace_id else ""
        payload = self._request(f"/initiatives/proposals/{quote(proposal_id)}{suffix}")
        return payload["data"]

    def create_decision(
        self,
        workspace_id: str,
        title: str,
        *,
        idempotency_key: Optional[str] = None,
        description: Optional[str] = None,
        shape: Optional[str] = None,
        shape_context: Optional[Mapping[str, Any]] = None,
        urgency: Optional[str] = None,
        blocks_task: Optional[bool] = None,
        task_id: Optional[str] = None,
        initiative_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Raise a decision for a human ruling; replay safety is caller-owned."""
        body: dict[str, Any] = {"workspace_id": workspace_id, "title": title}
        if description is not None:
            body["description"] = description
        if shape:
            body["shape"] = shape
        if shape_context is not None:
            body["shape_context"] = dict(shape_context)
        if urgency:
            body["urgency"] = urgency
        if blocks_task is not None:
            body["blocks_task"] = blocks_task
        if task_id:
            body["task_id"] = task_id
        if initiative_id:
            body["initiative_id"] = initiative_id
        return self._request(
            "/decisions",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )["decision"]

    def list_decisions(
        self,
        workspace_id: str,
        *,
        shape: Optional[str] = None,
        urgency: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[Mapping[str, Any]]:
        params = [f"workspace_id={quote(workspace_id)}"]
        if shape:
            params.append(f"shape={quote(shape)}")
        if urgency:
            params.append(f"urgency={quote(urgency)}")
        if status:
            params.append(f"status={quote(status)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        payload = self._request(f"/decisions?{'&'.join(params)}")
        return list(payload["decisions"])

    def list_artifact_types(self) -> list[Mapping[str, Any]]:
        """List the global artifact type vocabulary ``create_artifact`` accepts."""
        payload = self._request("/artifact-types")
        return list(payload["data"])

    def create_artifact(
        self,
        entity_type: str,
        entity_id: str,
        name: str,
        artifact_type: str,
        *,
        idempotency_key: Optional[str] = None,
        artifact_url: Optional[str] = None,
        external_url: Optional[str] = None,
        description: Optional[str] = None,
        preview_markdown: Optional[str] = None,
        initiative_id: Optional[str] = None,
        status: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        created_by_type: Optional[str] = None,
        created_by_id: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Register produced work; one of ``artifact_url`` or ``external_url`` is required."""
        body: dict[str, Any] = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "name": name,
            "artifact_type": artifact_type,
        }
        if artifact_url:
            body["artifact_url"] = artifact_url
        if external_url:
            body["external_url"] = external_url
        if description is not None:
            body["description"] = description
        if preview_markdown is not None:
            body["preview_markdown"] = preview_markdown
        if initiative_id:
            body["initiative_id"] = initiative_id
        if status:
            body["status"] = status
        if metadata is not None:
            body["metadata"] = dict(metadata)
        if created_by_type:
            body["created_by_type"] = created_by_type
        if created_by_id:
            body["created_by_id"] = created_by_id
        return self._request(
            "/artifacts",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )

    def list_artifacts(
        self,
        workspace_id: str,
        *,
        initiative_id: Optional[str] = None,
        task_id: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[Mapping[str, Any]]:
        params = [f"workspace_id={quote(workspace_id)}"]
        if initiative_id:
            params.append(f"initiative_id={quote(initiative_id)}")
        if task_id:
            params.append(f"task_id={quote(task_id)}")
        if status:
            params.append(f"status={quote(status)}")
        if since:
            params.append(f"since={quote(since)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        payload = self._request(f"/artifacts?{'&'.join(params)}")
        return list(payload["artifacts"])

    def list_artifacts_by_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        kind: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[Mapping[str, Any]]:
        params = [
            f"entity_type={quote(entity_type)}",
            f"entity_id={quote(entity_id)}",
        ]
        if kind:
            params.append(f"kind={quote(kind)}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        payload = self._request(f"/artifacts/by-entity?{'&'.join(params)}")
        return list(payload["artifacts"])

    def get_artifact(self, artifact_id: str) -> MutableMapping[str, Any]:
        """Read one artifact with its entity relationships."""
        return self._request(f"/artifacts/{quote(artifact_id)}")

    def control_run(
        self,
        run_id: str,
        action: str,
        *,
        idempotency_key: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Apply ``pause``, ``resume``, ``cancel``, or ``rollback`` to a run.

        ``checkpoint_id`` is required for ``rollback``.
        """
        body: dict[str, Any] = {}
        if checkpoint_id:
            body["checkpointId"] = checkpoint_id
        if reason is not None:
            body["reason"] = reason
        return self._request(
            f"/runs/{quote(run_id)}/actions/{quote(action)}",
            method="POST",
            idempotency_key=idempotency_key,
            body=body or None,
        )["data"]

    def apply_lifecycle_action(
        self,
        level: str,
        node_id: str,
        action: str,
        *,
        idempotency_key: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Pause, resume, retry, or cancel an initiative, workstream, milestone, task, or run."""
        return self._request(
            "/lifecycle",
            method="POST",
            idempotency_key=idempotency_key,
            body={"level": level, "id": node_id, "action": action},
        )

    def import_agent_work_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        idempotency_key: str,
        workspace_id: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Validate and store a portable Agent Work Receipt in the workspace."""
        body: dict[str, Any] = {"receipt": dict(receipt)}
        if workspace_id:
            body["workspace_id"] = workspace_id
        return self._request(
            "/agent-work-receipts",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )

    def get_agent_work_receipt_validator(self) -> MutableMapping[str, Any]:
        """Get the supported receipt schema, limits, and a runnable example."""
        return self._request("/agent-work-receipts/validate")

    def validate_agent_work_receipt(
        self, receipt: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        """Validate a portable Agent Work Receipt without storing it."""
        return self._request(
            "/agent-work-receipts/validate",
            method="POST",
            body=dict(receipt),
        )

    def get_workload_doctor_metadata(self) -> MutableMapping[str, Any]:
        """Get the workload diagnosis request schema and a runnable example."""
        return self._request("/doctor/workload")

    def diagnose_workload_boundaries(
        self,
        workload: Mapping[str, Any],
        *,
        schema_version: str = "workload-diagnosis/0.1",
    ) -> MutableMapping[str, Any]:
        """Score a workload across time, agents, systems, authority, and accountability."""
        return self._request(
            "/doctor/workload",
            method="POST",
            body={"schema_version": schema_version, "workload": dict(workload)},
        )

    def claim_dedup_fingerprint(
        self,
        source: str,
        event_key: str,
        *,
        idempotency_key: Optional[str] = None,
        initiative_id: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        active_run_id: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Claim a durable duplicate-trigger fingerprint; the first claimant wins."""
        body: dict[str, Any] = {"source": source, "event_key": event_key}
        if initiative_id:
            body["initiative_id"] = initiative_id
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        if active_run_id:
            body["active_run_id"] = active_run_id
        return self._request(
            "/live/dedup/claim",
            method="POST",
            idempotency_key=idempotency_key,
            body=body,
        )

    def create_estimate(
        self,
        prompt: str,
        content_types: list[str],
        *,
        variant_count: Optional[int] = None,
        brand_url: Optional[str] = None,
        brand_id: Optional[str] = None,
        platform: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Calculate the price and delivery range for a Content Studio request."""
        body: dict[str, Any] = {
            "prompt": prompt,
            "contentTypes": list(content_types),
        }
        if variant_count is not None:
            body["variantCount"] = variant_count
        if brand_url:
            body["brandUrl"] = brand_url
        if brand_id:
            body["brandId"] = brand_id
        if platform:
            body["platform"] = platform
        return self._request("/studio/estimate", method="POST", body=body)

    def get_showcase(
        self,
        *,
        query: Optional[str] = None,
        content_type: Optional[str] = None,
        industry: Optional[str] = None,
        style: Optional[str] = None,
        featured: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> MutableMapping[str, Any]:
        """Browse completed Content Studio examples."""
        params = []
        if query:
            params.append(f"query={quote(query)}")
        if content_type:
            params.append(f"contentType={quote(content_type)}")
        if industry:
            params.append(f"industry={quote(industry)}")
        if style:
            params.append(f"style={quote(style)}")
        if featured is not None:
            params.append(f"featured={'true' if featured else 'false'}")
        if limit is not None:
            params.append(f"limit={quote(str(limit))}")
        if offset is not None:
            params.append(f"offset={quote(str(offset))}")
        suffix = f"?{'&'.join(params)}" if params else ""
        return self._request(f"/studio/showcase{suffix}")

    def create_checkout(self, estimate_id: str) -> MutableMapping[str, Any]:
        """Create a checkout session for an accepted Content Studio estimate."""
        return self._request(
            "/studio/checkout",
            method="POST",
            body={"estimateId": estimate_id},
        )

    def _handoff_transition(
        self,
        action: str,
        workspace_id: str,
        handoff_id: str,
        expected_aggregate_version: int,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/handoffs/{quote(handoff_id)}/{action}",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "expected_aggregate_version": expected_aggregate_version,
            },
        )["data"]

    def _transition(
        self,
        action: str,
        workspace_id: str,
        process_id: str,
        expected_aggregate_version: int,
        idempotency_key: str,
    ) -> Mapping[str, Any]:
        return self._request(
            f"/operating-processes/{quote(process_id)}/{action}",
            method="POST",
            idempotency_key=idempotency_key,
            body={
                "workspace_id": workspace_id,
                "expected_aggregate_version": expected_aggregate_version,
            },
        )["data"]

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: Optional[Mapping[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        headers = {"Accept": "application/json"}
        credential = self.api_key or self.token
        if credential:
            headers["Authorization"] = f"Bearer {credential}"
        payload: Optional[bytes] = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            payload = json.dumps(body).encode("utf-8")
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        request = Request(
            f"{self.base_url.rstrip('/')}{path}",
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            try:
                error_payload = json.loads(error.read().decode("utf-8"))
            except (OSError, ValueError):
                error_payload = {}
            detail = error_payload.get("error", {}) if isinstance(error_payload, dict) else {}
            raise OrgXApiError(
                error.code,
                detail.get("code", "request_failed"),
                detail.get("message", f"OrgX API request failed ({error.code})"),
                detail.get("details"),
            ) from error
        except URLError as error:
            raise OrgXApiError(0, "transport_failed", str(error.reason)) from error
