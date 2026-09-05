import unittest
from unittest.mock import patch
from orgx_client.client import OrgXClient


class ContextTransportTests(unittest.TestCase):
    def test_sync_preserves_scope_and_acknowledged_base(self):
        with patch.object(OrgXClient, '_request', return_value={'data': {'context_delivery': {'base_verified': False}}}) as request:
            result = OrgXClient().sync_context('w', 'capsule_base', task_id='t')
            request.assert_called_once_with('/context-pack', method='POST', body={
                'workspace_id': 'w', 'task_id': 't', 'acknowledged_capsule_id': 'capsule_base'})
            self.assertFalse(result['context_delivery']['base_verified'])

    def test_expansion_encodes_artifact_id(self):
        with patch.object(OrgXClient, '_request', return_value={'data': {'artifact': {'id': 'a'}}}) as request:
            OrgXClient().expand_context_evidence('a/b')
            request.assert_called_once_with('/artifacts/a%2Fb')


if __name__ == '__main__':
    unittest.main()
