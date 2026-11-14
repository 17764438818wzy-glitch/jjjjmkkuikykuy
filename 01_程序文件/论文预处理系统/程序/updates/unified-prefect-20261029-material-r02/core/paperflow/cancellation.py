"""One non-blocking cooperative SDK close, shared by pause and model timeout."""
import threading
import time


class Cancellation:
    def __init__(self, close):
        self.close = close
        self.lock = threading.Lock()
        self.requested = threading.Event()
        self.finished = threading.Event()
        self.requested_at = self.finished_at = None
        self.reason = self.error = None

    def request(self, reason='pause'):
        with self.lock:
            if self.requested.is_set():
                return False
            self.reason, self.requested_at = reason, time.monotonic()
            self.requested.set()
            threading.Thread(target=self._close, daemon=True, name='paperflow-sdk-close').start()
            return True

    def _close(self):
        try:
            self.close()
        except Exception as exc:
            self.error = type(exc).__name__
        finally:
            self.finished_at = time.monotonic()
            self.finished.set()
