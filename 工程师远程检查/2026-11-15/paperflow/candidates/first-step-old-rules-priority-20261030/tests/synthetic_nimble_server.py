"""A local fake used exclusively by the 4292/14204 synthetic integration profile."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import time

lock = threading.Lock()
active = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        global active
        if self.path != '/v1/systemone':
            self.send_error(404)
            return
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        choices = payload['questions']['decision']['criteria']
        outcome = 'unknown' if 'MOCK_UNKNOWN' in json.dumps(payload, ensure_ascii=False) else 'normal'
        with lock:
            active += 1
            print(json.dumps({'event': 'begin', 'active': active, 'kind': outcome, 'time': time.time()}), flush=True)
        try:
            time.sleep(1.5)
            if outcome == 'unknown':
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            selected = ('attachment' if 'attachment' in choices else
                        next((name for name in choices if name != 'none'), 'none'))
            response = json.dumps({'answers': {'decision': {'choice': selected, 'confidence': 1,
                'probabilities': {name: 1 if name == selected else 0 for name in choices}}}}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        finally:
            with lock:
                active -= 1
                print(json.dumps({'event': 'finish', 'active': active, 'time': time.time()}), flush=True)


if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1', 11436), Handler).serve_forever()
