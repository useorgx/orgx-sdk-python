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
from .continuation import ContextContinuation, apply_context_transfer


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
                        acknowledged_capsule_id: Optional[str] = None,
                        reader_tokenizer: Optional[str] = None, max_payload_tokens: Optional[int] = None,
                        delivery_mode: Optional[str] = None, acknowledged_context_version: Optional[str] = None) -> Mapping[str, Any]:
        """Prepare current context; this response does not grant action authority."""
        body = {"workspace_id": workspace_id}
        for key, value in (("initiative_id", initiative_id), ("workstream_id", workstream_id),
                           ("task_id", task_id), ("acknowledged_capsule_id", acknowledged_capsule_id),
                           ("reader_tokenizer", reader_tokenizer), ("max_payload_tokens", max_payload_tokens),
                           ("delivery_mode", delivery_mode), ("acknowledged_context_version", acknowledged_context_version)):
            if value is not None:
                body[key] = value
        return self._request("/context-pack", method="POST", body=body)["data"]

    def sync_context(self, workspace_id: str, acknowledged_capsule_id: Optional[str] = None,
                     *, previous: Optional[ContextContinuation] = None, **scope: Any) -> Any:
        """Resume from a portable base, or use the legacy capsule rebootstrap form."""
        if acknowledged_capsule_id is not None:
            if previous is not None:
                raise ValueError("Use one continuation acknowledgement format")
            return self.prepare_context(workspace_id, acknowledged_capsule_id=acknowledged_capsule_id, **scope)
        scope.pop("delivery_mode", None)
        scope.pop("acknowledged_context_version", None)
        response = self.prepare_context(workspace_id, delivery_mode="delta",
            acknowledged_context_version=previous.version if previous else None, **scope)
        try:
            return apply_context_transfer(response, previous)
        except ValueError:
            if previous is None:
                raise
            return apply_context_transfer(self.prepare_context(workspace_id, delivery_mode="delta", **scope))

    def expand_context_evidence(self, artifact_id: str, expected_version: Optional[int] = None) -> Mapping[str, Any]:
        """Read an artifact through the existing authorized API; account for its tokens."""
        if expected_version is not None and (type(expected_version) is not int or expected_version < 1):
            raise ValueError("Artifact version must be a positive integer")
        suffix = f"?expected_version={expected_version}" if expected_version is not None else ""
        return self._request(f"/artifacts/{quote(artifact_id, safe='')}{suffix}")["data"]

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
