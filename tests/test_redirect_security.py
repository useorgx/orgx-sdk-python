import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from orgx_client.client import OrgXApiError, OrgXClient


class RedirectSecurityTests(unittest.TestCase):
    def setUp(self):
        self.received = []
        received = self.received

        class Target(BaseHTTPRequestHandler):
            def do_GET(handler):
                received.append(handler.headers.get('Authorization'))
                handler.send_response(200)
                handler.end_headers()
                handler.wfile.write(b'{"data": {"ok": true}}')

            def log_message(self, *_):
                pass

        self.target = ThreadingHTTPServer(('127.0.0.1', 0), Target)
        target_port = self.target.server_port

        class Source(Target):
            def do_GET(handler):
                if handler.path == '/safe':
                    return super().do_GET()
                handler.send_response(302)
                handler.send_header('Location', '/safe' if handler.path == '/same'
                                    else f'http://localhost:{target_port}/capture')
                handler.end_headers()

        self.source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
        self.threads = []
        for server in (self.source, self.target):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        self.client = OrgXClient(api_key='SYNTHETIC_REDIRECT_TEST',
                                 base_url=f'http://127.0.0.1:{self.source.server_port}')

    def tearDown(self):
        for server in (self.source, self.target):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join()

    def test_json_cross_origin_redirect_never_sends_credentials(self):
        with self.assertRaises(OrgXApiError):
            self.client._request('/cross')
        self.assertEqual(self.received, [])

    def test_sse_cross_origin_redirect_never_sends_credentials(self):
        with self.assertRaises(OrgXApiError):
            self.client.subscribe_events('synthetic-workspace')
        self.assertEqual(self.received, [])

    def test_same_origin_redirect_preserves_authenticated_request(self):
        self.assertTrue(self.client._request('/same')['data']['ok'])
        self.assertEqual(self.received, ['Bearer SYNTHETIC_REDIRECT_TEST'])


if __name__ == '__main__':
    unittest.main()
