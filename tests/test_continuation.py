import hashlib
import json
import unittest
from unittest.mock import patch
from orgx_client import OrgXClient, apply_context_transfer


def full(text):
    return {'context_transfer': {'schema_version': 'orgx.context-transfer/v1', 'mode': 'full', 'serialized': text, 'version': 'sha256:' + hashlib.sha256(text.encode()).hexdigest()}}


class ContinuationTests(unittest.TestCase):
    def test_exact_bytes_and_invalid_ranges(self):
        text = json.dumps({'context_delivery': {}, 'text': 'résumé\u2028中文', 'value': 1e-7}, ensure_ascii=False, indent=2)
        base = apply_context_transfer(full(text))
        delta = {'context_transfer': {'schema_version': 'orgx.context-transfer/v1', 'mode': 'delta', 'base_version': base.version, 'version': base.version, 'operations': [{'copy': [0, len(text.split('\n'))]}]}}
        self.assertEqual(apply_context_transfer(delta, base), base)
        delta['context_transfer']['operations'] = [{'copy': [True, 1]}]
        with self.assertRaisesRegex(ValueError, 'range'):
            apply_context_transfer(delta, base)

    def test_sync_rebootstraps_after_invalid_delta(self):
        text = '{"context_delivery": {}}'
        base = apply_context_transfer(full(text))
        with patch.object(OrgXClient, '_request', side_effect=[{'data': {'context_transfer': {}}}, {'data': full(text)}]) as request:
            result = OrgXClient().sync_context('w', previous=base)
            self.assertEqual(result.serialized, text)
            self.assertEqual(request.call_count, 2)
            self.assertEqual(request.call_args_list[0].kwargs['body']['acknowledged_context_version'], base.version)
            self.assertNotIn('acknowledged_context_version', request.call_args_list[1].kwargs['body'])
