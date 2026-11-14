"""urllib transport cancellation, without detached requests or paid retries."""
import functools
import errno
import http.client
import io
import select
import socket
import threading
import time
from urllib.request import HTTPHandler, HTTPSHandler, build_opener


class _Scope:
    def __init__(self, stop):
        self.stop = stop
        self.done = threading.Event()
        self.lock = threading.Lock()
        self.sockets = []
        self.watcher = threading.Thread(target=self._watch, name='model-stop', daemon=True)
        self.watcher.start()

    def check(self):
        if self.stop.is_set(): raise InterruptedError('已停止模型请求；已收到的输出保留，不自动重发')

    @staticmethod
    def _abort(sock):
        try: sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass

    def register(self, sock):
        with self.lock: self.sockets.append(sock)
        if self.stop.is_set():
            self._abort(sock)
            sock.close()
            self.check()
        return sock

    def _watch(self):
        while not self.done.wait(.05):
            if self.stop.is_set():
                with self.lock: sockets = list(self.sockets)
                for sock in sockets: self._abort(sock)
                return

    def finish(self):
        self.done.set()
        self.watcher.join(.2)


class _TLSContext:
    def __init__(self, context, scope):
        self.context, self.scope = context, scope

    def __getattr__(self, name): return getattr(self.context, name)

    def wrap_socket(self, *args, **kwargs):
        # Register the SSLSocket before its handshake can block.
        kwargs['do_handshake_on_connect'] = False
        sock = self.context.wrap_socket(*args, **kwargs)
        try:
            self.scope.register(sock)
            timeout = sock.gettimeout()
            deadline = time.monotonic() + timeout if timeout else None
            sock.settimeout(min(.1, timeout) if timeout else .1)
            while True:
                self.scope.check()
                try:
                    sock.do_handshake()
                    break
                except socket.timeout:
                    if deadline is not None and time.monotonic() >= deadline: raise
            sock.settimeout(timeout)
            return sock
        except BaseException:
            sock.close()
            raise


class _Connection:
    def __init__(self, *args, scope, **kwargs):
        super().__init__(*args, **kwargs)
        def connect(*args, **kwargs):
            return _connect(scope, *args, **kwargs)
        self._create_connection = connect
        self.response_class = functools.partial(_HTTPResponse, scope=scope)
        if hasattr(self, '_context'): self._context = _TLSContext(self._context, scope)


def _connect(scope, address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None, *, all_errors=False):
    """Preserve urllib address/proxy selection; poll only this TCP connection."""
    scope.check()
    host, port = address
    if timeout is socket._GLOBAL_DEFAULT_TIMEOUT: timeout = socket.getdefaulttimeout()
    errors = []
    pending = {errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, errno.EINTR, 10035, 10036, 10037}
    for family, kind, proto, _, target in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
        scope.check()
        sock = socket.socket(family, kind, proto)
        try:
            scope.register(sock)
            if source_address: sock.bind(source_address)
            sock.setblocking(False)
            code = sock.connect_ex(target)
            deadline = time.monotonic() + timeout if timeout is not None else None
            while code in pending:
                scope.check()
                remaining = deadline - time.monotonic() if deadline is not None else .1
                if remaining <= 0: raise socket.timeout('timed out')
                _, writable, failed = select.select([], [sock], [sock], min(.1, remaining))
                if writable or failed:
                    code = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            scope.check()
            if code: raise OSError(code, 'TCP connection failed')
            sock.settimeout(timeout)
            return sock
        except InterruptedError:
            sock.close()
            raise
        except OSError as exc:
            sock.close()
            errors.append(exc)
    scope.check()
    if not errors: raise OSError('getaddrinfo returns an empty list')
    if all_errors: raise ExceptionGroup('create_connection failed', errors)
    raise errors[-1]


class _SocketReader(io.RawIOBase):
    """Poll inside raw IO, so BufferedReader never sees a transient timeout."""
    def __init__(self, sock, scope):
        self.sock, self.scope = sock, scope
        self.timeout = sock.gettimeout()
        # Keep the socket's IO reference until the response closes it. urllib
        # closes the connection's own reference after returning HTTP headers.
        self.owner = sock.makefile('rb', buffering=0)
        sock.settimeout(min(.1, self.timeout) if self.timeout else .1)

    def readable(self): return True

    def readinto(self, buffer):
        deadline = time.monotonic() + self.timeout if self.timeout else None
        while True:
            self.scope.check()
            try:
                return self.sock.recv_into(buffer)
            except socket.timeout:
                if deadline is not None and time.monotonic() >= deadline: raise
            except OSError:
                self.scope.check()
                raise

    def close(self):
        try:
            if not self.closed:
                try: self.sock.settimeout(self.timeout)
                except OSError: pass
            self.owner.close()
        finally: super().close()


class _ReadSocket:
    def __init__(self, sock, scope): self.sock, self.scope = sock, scope
    def makefile(self, mode): return io.BufferedReader(_SocketReader(self.sock, self.scope))


class _HTTPResponse(http.client.HTTPResponse):
    def __init__(self, sock, *args, scope, **kwargs):
        super().__init__(_ReadSocket(sock, scope), *args, **kwargs)


class _HTTPConnection(_Connection, http.client.HTTPConnection): pass
class _HTTPSConnection(_Connection, http.client.HTTPSConnection): pass


class _HTTPHandler(HTTPHandler):
    def __init__(self, local):
        super().__init__(); self.local = local

    def http_open(self, req):
        return self.do_open(functools.partial(_HTTPConnection, scope=self.local.scope), req)


class _HTTPSHandler(HTTPSHandler):
    def __init__(self, local):
        super().__init__(); self.local = local

    def https_open(self, req):
        return self.do_open(functools.partial(_HTTPSConnection, scope=self.local.scope), req, context=self._context)


class _Response:
    def __init__(self, response, scope): self.response, self.scope = response, scope
    def __getattr__(self, name): return getattr(self.response, name)
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    def __iter__(self): return self
    def __next__(self):
        value = self.readline()
        if not value: raise StopIteration
        return value

    def _read(self, method, *args):
        self.scope.check()
        try:
            value = method(*args)
        except BaseException:
            self.scope.check()
            raise
        # shutdown may produce EOF instead of an exception.
        if not value: self.scope.check()
        return value

    def read(self, *args): return self._read(self.response.read, *args)
    def readline(self, *args): return self._read(self.response.readline, *args)
    def close(self):
        try: self.response.close()
        finally: self.scope.finish()


class _Opener:
    def __init__(self, stop, handlers):
        self.stop, self.local = stop, threading.local()
        self.opener = build_opener(*handlers, _HTTPHandler(self.local), _HTTPSHandler(self.local))

    def open(self, *args, **kwargs):
        scope = _Scope(self.stop)
        previous = getattr(self.local, 'scope', None)
        self.local.scope = scope
        try:
            scope.check()
            return _Response(self.opener.open(*args, **kwargs), scope)
        except BaseException:
            scope.finish()
            scope.check()
            raise
        finally:
            self.local.scope = previous


def cancellable_opener(stop, *handlers):
    return build_opener(*handlers) if stop is None else _Opener(stop, handlers)
