"""Controller methods using the client's existing authenticated transport."""
from __future__ import annotations

from typing import Any, Literal, Mapping, MutableMapping, Optional
from urllib.parse import quote, urlencode

ControllerDomain = Literal['product', 'engineering', 'growth', 'sales', 'design', 'operations']


class ControllerOperations:
    def _request(
        self, path: str, *, method: str = 'GET',
        body: Optional[Mapping[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        raise NotImplementedError

    def get_controller_status(
        self, workspace_id: str, domain: ControllerDomain,
    ) -> MutableMapping[str, Any]:
        """Read status; a healthy historical run does not imply current enablement."""
        query = urlencode({'workspace_id': workspace_id, 'protocol_version': 'orgx.controller.v1'})
        return self._request(f'/controllers/{quote(domain, safe="")}?{query}')

    def reconcile_controller(
        self, workspace_id: str, domain: ControllerDomain, *, idempotency_key: str,
        spec_revision: Optional[str] = None, input_cursor: Optional[str] = None,
        max_input_age_seconds: Optional[int] = None,
    ) -> MutableMapping[str, Any]:
        """Request shadow reconciliation; proposals do not grant action authority."""
        body: dict[str, Any] = {
            'workspace_id': workspace_id, 'idempotency_key': idempotency_key,
            'protocol_version': 'orgx.controller.v1', 'mode': 'shadow',
        }
        for key, value in [('spec_revision', spec_revision), ('input_cursor', input_cursor),
                           ('max_input_age_seconds', max_input_age_seconds)]:
            if value is not None:
                body[key] = value
        return self._request(f'/controllers/{quote(domain, safe="")}/reconcile',
                             method='POST', body=body, idempotency_key=idempotency_key)
