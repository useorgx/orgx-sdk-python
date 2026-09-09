import io
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlparse, parse_qs
from orgx_client import OrgXClient, OrgXApiError


class ControllerTests(unittest.TestCase):
    def test_status_preserves_scope_and_protocol(self):
        payload = {'data': {'limitations': ['Current controller activation: policy_disabled.']}}
        with patch('orgx_client.client.urlopen', return_value=io.BytesIO(json.dumps(payload).encode())) as transport:
            self.assertEqual(OrgXClient(api_key='fixture').get_controller_status('workspace/a?b', 'growth'), payload)
            request = transport.call_args.args[0]
            self.assertEqual(urlparse(request.full_url).path, '/api/v1/controllers/growth')
            self.assertEqual(parse_qs(urlparse(request.full_url).query), {
                'workspace_id': ['workspace/a?b'], 'protocol_version': ['orgx.controller.v1']})
            self.assertEqual(request.get_header('Authorization'), 'Bearer fixture')
            self.assertEqual(transport.call_count, 1)

    def test_reconcile_preserves_idempotency_controls_and_receipt(self):
        payload = {'data': {'receipt_id': 'receipt-server', 'duplicate': True}}
        with patch('orgx_client.client.urlopen', return_value=io.BytesIO(json.dumps(payload).encode())) as transport:
            result = OrgXClient(token='fixture').reconcile_controller('w', 'growth', idempotency_key='retry-key', spec_revision='revision', input_cursor='', max_input_age_seconds=0)
            self.assertEqual(result, payload)
            request = transport.call_args.args[0]
            self.assertEqual(request.get_method(), 'POST')
            self.assertEqual(request.get_header('Idempotency-key'), 'retry-key')
            self.assertEqual(json.loads(request.data), {
                'workspace_id': 'w', 'idempotency_key': 'retry-key', 'mode': 'shadow',
                'protocol_version': 'orgx.controller.v1', 'spec_revision': 'revision',
                'input_cursor': '', 'max_input_age_seconds': 0})
            self.assertEqual(transport.call_count, 1)

    def test_disabled_controller_is_not_retried(self):
        error = HTTPError('https://fixture.invalid', 403, 'Forbidden', {}, io.BytesIO(json.dumps({'error': {'code': 'controller_disabled', 'message': 'Disabled'}}).encode()))
        with patch('orgx_client.client.urlopen', side_effect=error) as transport:
            with self.assertRaises(OrgXApiError) as caught:
                OrgXClient().reconcile_controller('w', 'growth', idempotency_key='key')
            self.assertEqual(caught.exception.status, 403)
            self.assertEqual(caught.exception.code, 'controller_disabled')
            self.assertEqual(transport.call_count, 1)
