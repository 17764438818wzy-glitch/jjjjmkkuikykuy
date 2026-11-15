"""Loopback-only adapter for the WorkBuddy dashboard; no automatic model calls."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import socket
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


GATEWAY = "http://192.168.1.120:8789"
PREFECT_LIMIT = "http://192.168.1.120:4200/api/v2/concurrency_limits/paperflow-connected-total_llm"
ALLOWED_ORIGINS = {"http://127.0.0.1:8795", "http://localhost:8795"}
ACCOUNT_TEST_MODEL = "deepseek-v4.1-flash"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = build_opener(ProxyHandler({}), NoRedirect())


def request_json(path, *, token=None, data=None, headers=None, timeout=3):
    request_headers = {"Accept": "application/json", **(headers or {})}
    if token:
        request_headers["X-Panel-Token"] = token
    body = None
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = Request(GATEWAY + path, data=body, headers=request_headers,
                      method="POST" if body is not None else "GET")
    with OPENER.open(request, timeout=timeout) as response:
        return json.loads(response.read(4 * 1024 * 1024).decode("utf-8"))


def read_prefect_limit():
    with OPENER.open(Request(PREFECT_LIMIT, headers={"Accept": "application/json"}), timeout=3) as response:
        return json.loads(response.read(65536).decode("utf-8"))


def secret_path():
    return Path(os.environ["LOCALAPPDATA"]) / "Paperflow" / "workbuddy-monitor-8795.dpapi"


def save_password(password):
    import win32crypt
    target = secret_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    encrypted = win32crypt.CryptProtectData(password.encode("utf-8"), None, None, None, None, 1)
    target.write_bytes(encrypted)


def load_password():
    try:
        import win32crypt
        _, password = win32crypt.CryptUnprotectData(secret_path().read_bytes(), None, None, None, 1)
        return password.decode("utf-8")
    except Exception:
        return None


def count(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def amount(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0 else None


class Monitor:
    def __init__(self, transport=request_json, prefect_transport=read_prefect_limit, password_loader=load_password):
        self.transport = transport
        self.prefect_transport = prefect_transport
        self.password_loader = password_loader
        self._token = None
        self._target = None
        self._lock = threading.RLock()
        self._snapshot_lock = threading.Lock()
        self._test_lock = threading.Lock()
        self._cache = None
        self._cache_time = 0.0
        self._last_login_attempt = float("-inf")

    def login(self, password):
        if not isinstance(password, str) or not password or len(password) > 512:
            return False
        try:
            reply = self.transport("/panel/login", data={"password": password})
        except (HTTPError, URLError, OSError, ValueError, TimeoutError):
            return False
        token = reply.get("token") if isinstance(reply, dict) and reply.get("ok") is True else None
        if not isinstance(token, str) or not token:
            return False
        with self._lock:
            self._token, self._target, self._cache = token, None, None
        return True

    def summary(self):
        # One shared sample at a time even if several 8795 tabs are open.
        with self._snapshot_lock:
            with self._lock:
                if self._cache is not None and time.monotonic() - self._cache_time < 0.8:
                    return dict(self._cache)
                token = self._token
            state = {
                "connected": False, "authorized": bool(token), "usage_available": False, "realm": None,
                "domestic_only_test": True,
                "accounts_ready": None, "accounts_total": None,
                "input_tokens": None, "output_tokens": None,
                "successful_requests": None, "failed_requests": None,
                "recent_model": None, "recent_realm": None,
                "credit_balance": None, "credits_partial": False, "credits_scope": None,
                "in_flight": None, "prefect_active_slots": None, "concurrency_limit": None,
                "as_of": datetime.now().astimezone().isoformat(timespec="seconds"),
            }
            with ThreadPoolExecutor(max_workers=2) as pool:
                health_read = pool.submit(self.transport, "/health")
                limit_read = pool.submit(self.prefect_transport)
                try:
                    health = health_read.result()
                    if not isinstance(health, dict) or health.get("ok") is not True:
                        raise ValueError("invalid health response")
                    state["connected"] = True
                    state["realm"] = health.get("realm") if health.get("realm") in ("cn", "intl") else None
                    ready, total = count(health.get("accounts_ready")), count(health.get("accounts"))
                    if ready is not None and total is not None and ready <= total:
                        state["accounts_ready"], state["accounts_total"] = ready, total
                except (HTTPError, URLError, OSError, ValueError, TimeoutError):
                    pass

                try:
                    limit = limit_read.result()
                    if (isinstance(limit, dict) and limit.get("name") == "paperflow-connected-total_llm"
                            and limit.get("active") is True and limit.get("slot_decay_per_second") == 0):
                        state["prefect_active_slots"] = count(limit.get("active_slots"))
                        state["concurrency_limit"] = count(limit.get("limit"))
                except (HTTPError, URLError, OSError, ValueError, TimeoutError):
                    pass

            if not token and state["connected"]:
                with self._lock:
                    attempt = time.monotonic() - self._last_login_attempt >= 60
                    if attempt:
                        self._last_login_attempt = time.monotonic()
                if attempt:
                    password = self.password_loader()
                    if password:
                        self.login(password)
                    with self._lock:
                        token = self._token
                    state["authorized"] = bool(token)

            target = None
            if token and state["connected"]:
                paths = {
                    "usage": "/usage/analytics?realm=all&range=today",
                    "recent": "/usage/recent?realm=all&limit=1",
                    "balance": "/v1/balance-summary?realm=all",
                }
                def read(path):
                    try:
                        return self.transport(path, token=token)
                    except HTTPError as exc:
                        return "unauthorized" if exc.code == 401 else None
                    except (URLError, OSError, ValueError, TimeoutError):
                        return None
                with ThreadPoolExecutor(max_workers=3) as pool:
                    values = dict(zip(paths, pool.map(read, paths.values())))
                if values["usage"] == "unauthorized" or values["recent"] == "unauthorized":
                    with self._lock:
                        if self._token == token:
                            self._token = None
                    state["authorized"] = False
                else:
                    usage = values["usage"]
                    summary = usage.get("summary") if isinstance(usage, dict) else None
                    window = summary.get("window", {}) if isinstance(summary, dict) else {}
                    if isinstance(window, dict):
                        state["input_tokens"] = count(window.get("prompt_tokens"))
                        state["output_tokens"] = count(window.get("completion_tokens"))
                        state["successful_requests"] = count(window.get("requests"))
                        state["failed_requests"] = count(window.get("errors"))
                        state["usage_available"] = state["input_tokens"] is not None and state["output_tokens"] is not None
                    recent = values["recent"]
                    rows = recent.get("rows") if isinstance(recent, dict) else None
                    first = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
                    model, realm = first.get("model"), first.get("realm")
                    if isinstance(model, str) and model.strip() and len(model) <= 160:
                        state["recent_model"] = model
                        if realm in ("cn", "intl"):
                            state["recent_realm"] = realm
                            target = (model, realm)
                    balance = values["balance"]
                    if isinstance(balance, dict):
                        state["credit_balance"] = amount(balance.get("balance"))
                        enabled, known = count(balance.get("accounts_enabled")), count(balance.get("accounts_with_credit_data"))
                        state["credits_partial"] = enabled is None or known is None or known != enabled
                        state["credits_scope"] = balance.get("realm") if balance.get("realm") in ("all", "cn", "intl") else None
            with self._lock:
                if self._token == token:
                    self._target = target
                    self._cache, self._cache_time = dict(state), time.monotonic()
            return state

    def test_line(self, model, realm):
        with self._lock:
            token, target = self._token, self._target
        if not token or realm != "cn" or target is None or (model, realm) != target:
            return {"status": "unavailable"}
        started = time.monotonic()
        try:
            result = self.transport("/v1/chat/completions", token=token,
                headers={"X-Realm": realm}, timeout=45,
                data={"model": model, "messages": [{"role": "user", "content": "Reply OK"}],
                      "max_tokens": 16, "stream": False})
            choices = result.get("choices") if isinstance(result, dict) else None
            choice = choices[0] if isinstance(choices, list) and choices and isinstance(choices[0], dict) else {}
            message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
            content = message.get("content")
            success = choice.get("finish_reason") not in ("error", "content_filter") and isinstance(content, str) and bool(content.strip())
            status = "success" if success else "failed"
        except HTTPError:
            status = "failed"
        except (URLError, OSError, ValueError, TimeoutError, socket.timeout):
            # The gateway may still be processing a request after the client times out.
            status = "unknown"
        with self._lock:
            self._cache = None
        return {"status": status, "elapsed_ms": round((time.monotonic() - started) * 1000)}

    def list_accounts(self):
        with self._lock:
            token = self._token
        if not token:
            return {"status": "unavailable", "accounts": [], "error": "网关未连接"}
        try:
            # A gateway restart invalidates panel sessions. Retry this read only;
            # account probes, settings writes and model requests are never replayed.
            for attempt in range(2):
                try:
                    reply = self.transport("/accounts?realm=all", token=token, timeout=8)
                    break
                except HTTPError as exc:
                    if exc.code != 401:
                        raise
                    with self._lock:
                        if self._token == token:
                            self._token = self._target = self._cache = None
                        token = self._token
                    if attempt == 1:
                        raise
                    if not token:
                        if not self.login(self.password_loader()):
                            raise
                        with self._lock:
                            token = self._token
            rows = reply.get("accounts") if isinstance(reply, dict) else None
            if not isinstance(rows, list):
                raise ValueError("invalid accounts")
            accounts, seen = [], set()
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError("invalid account")
                uid, realm, enabled = row.get("uid"), row.get("realm"), row.get("enabled")
                if (not isinstance(uid, str) or not uid or len(uid) > 200 or uid in seen
                        or realm not in ("cn", "intl") or not isinstance(enabled, bool)):
                    raise ValueError("invalid account identity or state")
                seen.add(uid)
                name = row.get("nickname")
                accounts.append({"uid": uid, "nickname": name[:160] if isinstance(name, str) and name else uid[:8],
                                 "realm": realm, "enabled": enabled,
                                 "tasksEnabled": row.get("tasksEnabled") if type(row.get("tasksEnabled")) is bool else None,
                                 "status": "untested" if enabled else "disabled"})
            return {"status": "available", "accounts": accounts}
        except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
            return {"status": "unavailable", "accounts": [], "error": "无法读取账号状态"}

    def test_domestic_accounts(self, model, *, before_batch=None):
        """Probe enabled domestic accounts only when the user starts a task."""
        if not isinstance(model, str) or not model.strip():
            return {"status": "unavailable", "complete": False, "accounts": [], "usable_uids": []}
        if not self._test_lock.acquire(blocking=False):
            return {"status": "busy", "complete": False, "accounts": [], "usable_uids": []}
        try:
            protocol = "account-slot-v1"
            try:
                health = self.transport("/health", timeout=8)
            except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
                health = None
            if not isinstance(health, dict) or health.get("capacity_probe_protocol") != protocol:
                return {"status": "incomplete", "complete": False, "accounts": [], "usable_uids": []}
            with self._lock:
                token = self._token
            if not token:
                password = self.password_loader()
                if password:
                    self.login(password)
            listing = self.list_accounts()
            if listing["status"] != "available":
                return {**listing, "complete": False, "usable_uids": []}
            accounts = [account for account in listing["accounts"] if account["realm"] == "cn"]
            enabled = [account for account in accounts if account["enabled"]]
            with self._lock:
                token = self._token

            def probe(account):
                try:
                    result = self.transport(
                        "/accounts/test", token=token, timeout=45,
                        data={"uid": account["uid"], "realm": "cn", "model": model,
                              "capacity_probe": protocol})
                    if (not isinstance(result, dict) or result.get("capacity_probe_ack") != protocol
                            or result.get("uid") != account["uid"] or result.get("model") != model
                            or result.get("realm") != "cn"):
                        return "unknown"
                    state = result.get("admission_state")
                    if state == "local_busy":
                        return "busy"
                    if result.get("network") is True:
                        return "unknown"
                    status = result.get("status")
                    if status is not None and (type(status) is not int or status in (408, 429) or status >= 500):
                        return "unknown"
                    if state == "confirmed_unavailable":
                        return "failed" if result.get("ok") is False else "unknown"
                    if state is not None:
                        return "unknown"  # Includes readiness_unknown and future states.
                    if result.get("ok") is True and (status is None or 200 <= status < 300):
                        return "success"
                    return "unknown"
                except HTTPError:
                    return "unknown"
                except (URLError, OSError, ValueError, TimeoutError, socket.timeout):
                    return "unknown"

            if enabled:
                with ThreadPoolExecutor(max_workers=min(4, len(enabled))) as pool:
                    for index in range(0, len(enabled), 4):
                        if before_batch:
                            before_batch()
                        batch = enabled[index:index + 4]
                        for account, status in zip(batch, pool.map(probe, batch)):
                            account["status"] = status
            with self._lock:
                self._cache = None
            complete = all(account["status"] in ("success", "failed") for account in enabled)
            return {"status": "available" if complete else "incomplete", "complete": complete, "accounts": accounts,
                    "usable_uids": [account["uid"] for account in enabled if account["status"] == "success"]}
        finally:
            self._test_lock.release()

    def allow_domestic_accounts(self, key_id, uids):
        # An empty accounts list removes the gateway restriction; never write it.
        if not isinstance(key_id, str) or not key_id or not uids:
            return {"status": "unavailable"}
        return self._apply_domestic_scope(key_id, uids)

    def allow_domestic_pool(self, key_id, limit):
        # Explicit opt-in: unrestricted UIDs within a bound CN realm, never all realms.
        if not isinstance(key_id, str) or not key_id or type(limit) is not int or limit != 50:
            return {"status": "unavailable"}
        return self._apply_domestic_scope(key_id, [], limit=limit)

    def _apply_domestic_scope(self, key_id, uids, *, limit=None):
        with self._lock:
            token = self._token
        if not token:
            return {"status": "unavailable"}
        try:
            update = {"id": key_id, "realm": "cn", "accounts": uids}
            if limit is not None:
                update['concurrency_limit'] = limit
            self.transport("/settings/save", token=token, timeout=8,
                data={"api_keys": [update],
                      "deleted_api_key_ids": []})
            settings = self.transport("/settings", token=token, timeout=8)
        except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
            return {"status": "unknown"}
        keys = settings.get("api_keys") if isinstance(settings, dict) else None
        key = next((item for item in keys if isinstance(item, dict) and item.get("id") == key_id), None) if isinstance(keys, list) else None
        if not key or key.get("enabled") is not True or key.get("realm") != "cn" or set(key.get("accounts") or []) != set(uids):
            return {"status": "unknown"}
        if limit is not None and key.get('concurrency_limit') != limit:
            return {"status": "unknown"}
        return {"status": "confirmed"}

    def test_accounts(self):
        if not self._test_lock.acquire(blocking=False):
            return {"status": "busy", "accounts": [], "error": "账号测试正在进行"}
        try:
            listing = self.list_accounts()
            if listing["status"] != "available":
                return listing
            accounts = listing["accounts"]
            with self._lock:
                token = self._token
            realms = [realm for realm in ("cn",)
                      if any(row["enabled"] and row["realm"] == realm for row in accounts)]

            def probe(realm):
                try:
                    reply = self.transport("/accounts/test", token=token, data={"realm": realm}, timeout=90)
                    results = reply.get("results") if isinstance(reply, dict) else None
                    if not isinstance(results, list):
                        raise ValueError("invalid test response")
                    return realm, results
                except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
                    return realm, None

            with ThreadPoolExecutor(max_workers=2) as pool:
                probes = dict(pool.map(probe, realms))

            results = {}
            for realm, rows in probes.items():
                if rows is None:
                    continue
                for row in rows:
                    if isinstance(row, dict) and isinstance(row.get("uid"), str):
                        results[(realm, row["uid"])] = row
            for account in accounts:
                if not account["enabled"] or account["realm"] != "cn":
                    continue
                row = results.get((account["realm"], account["uid"]))
                if not isinstance(row, dict) or not isinstance(row.get("ok"), bool):
                    account.update(status="unknown", error="未收到该账号的测试结果")
                    continue
                error = row.get("error")
                if isinstance(error, str):
                    error = re.sub(r"(?i)(authorization|password|api[_-]?key|token)\s*[:=]\s*\S+", r"\1=[已隐藏]", error)[:240]
                else:
                    error = ""
                if row["ok"]:
                    status = "success"
                elif row.get("network") is True:
                    status = "unknown"
                    error = error or "本机网络不可用"
                else:
                    status = "failed"
                    error = error or "未返回失败原因"
                model = row.get("model")
                account.update(status=status, error=error,
                               model=model[:160] if isinstance(model, str) else None,
                               elapsed_ms=count(row.get("elapsed_ms")))
            statuses = {row["status"] for row in accounts if row["enabled"] and row["realm"] == "cn"}
            overall = ("failed" if "failed" in statuses else "unknown" if "unknown" in statuses
                       else "success" if statuses == {"success"} else "unavailable")
            with self._lock:
                self._cache = None
            return {"status": overall, "accounts": accounts}
        finally:
            self._test_lock.release()

    def disable_account(self, uid):
        if not self._test_lock.acquire(blocking=False):
            return {"status": "busy", "error": "账号测试正在进行，请稍后禁用"}
        try:
            return self._disable_account(uid)
        finally:
            self._test_lock.release()

    def _disable_account(self, uid):
        if not isinstance(uid, str) or not uid or len(uid) > 200:
            return {"status": "unavailable", "error": "账号无效"}
        listing = self.list_accounts()
        if listing["status"] != "available":
            return {"status": "unavailable", "error": listing["error"]}
        account = next((row for row in listing["accounts"] if row["uid"] == uid), None)
        if account is None:
            return {"status": "unavailable", "error": "未找到账号"}
        if account["enabled"] is False:
            return {"status": "disabled"}
        with self._lock:
            token = self._token
        try:
            self.transport("/accounts/set", token=token, data={"uid": uid, "enabled": False}, timeout=8)
        except HTTPError:
            return {"status": "failed", "error": "网关拒绝停用"}
        except (URLError, OSError, ValueError, TimeoutError, socket.timeout):
            return {"status": "unknown", "error": "未收到停用结果，请核对账号状态"}
        listing = self.list_accounts()
        if listing["status"] != "available":
            return {"status": "unknown", "error": "停用后暂时无法核对账号状态"}
        account = next((row for row in listing["accounts"] if row["uid"] == uid), None)
        if account is None or account["enabled"]:
            return {"status": "unknown", "error": "暂未确认停用，请核对账号状态"}
        with self._lock:
            self._cache = None
        return {"status": "disabled"}

    def enable_account(self, uid):
        if not self._test_lock.acquire(blocking=False):
            return {"status": "busy", "error": "账号检测正在进行，请稍后操作"}
        try:
            return self._enable_account(uid)
        finally:
            self._test_lock.release()

    def _enable_account(self, uid, expected_realm=None):
        if not isinstance(uid, str) or not uid or len(uid) > 200:
            return {"status": "unavailable", "error": "账号无效"}
        listing = self.list_accounts()
        account = next((row for row in listing["accounts"] if row["uid"] == uid), None)
        if listing["status"] != "available" or account is None or (expected_realm and account["realm"] != expected_realm):
            return {"status": "unavailable", "error": "账号状态或身份未确认，请刷新列表"}
        if account["enabled"]:
            return {"status": "enabled", "account": account, "newly_enabled": False}
        tasks = account.get("tasksEnabled")
        if type(tasks) is not bool:
            return {"status": "unknown", "error": "账号后台任务开关未确认，尚未启用"}
        with self._lock:
            token = self._token
        try:
            self.transport("/accounts/set", token=token,
                           data={"uid": uid, "enabled": True, "tasksEnabled": tasks}, timeout=8)
        except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
            pass  # A missing write response is resolved by readback, never a replay.
        observed = self.list_accounts()
        after = next((row for row in observed["accounts"] if row["uid"] == uid), None)
        with self._lock:
            self._cache = None
        if (observed["status"] == "available" and after and after["realm"] == account["realm"]
                and after["enabled"] is True and after.get("tasksEnabled") is tasks):
            return {"status": "enabled", "account": after, "newly_enabled": True}
        return {"status": "unknown", "error": "启用结果待核对，请刷新账号状态后确认"}

    def refresh_accounts(self, uids):
        """Explicit, small batches: native basic test first, then enable successes."""
        if (not isinstance(uids, list) or not 1 <= len(uids) <= 4
                or any(not isinstance(uid, str) or not uid or len(uid) > 200 for uid in uids)
                or len(set(uids)) != len(uids)):
            return {"status": "unavailable", "accounts": [], "error": "每批需提供 1 至 4 个不同的账号"}
        if not self._test_lock.acquire(blocking=False):
            return {"status": "busy", "accounts": [], "error": "账号检测正在进行，请稍后操作"}
        try:
            listing = self.list_accounts()
            by_uid = {row["uid"]: row for row in listing["accounts"]}
            if listing["status"] != "available" or any(uid not in by_uid for uid in uids):
                return {"status": "unavailable", "accounts": [], "error": "账号列表已变化，请重新刷新"}
            with self._lock:
                token = self._token

            def probe(uid):
                account = dict(by_uid[uid], status="unknown", error="未收到有效检测结果")
                try:
                    row = self.transport("/accounts/test", token=token, timeout=45,
                                         data={"uid": uid, "model": ACCOUNT_TEST_MODEL})
                except (HTTPError, URLError, OSError, ValueError, TimeoutError, socket.timeout):
                    account["error"] = "检测结果未知，请稍后核对"
                    return account, 0
                if (not isinstance(row, dict) or row.get("uid") != uid or row.get("model") != ACCOUNT_TEST_MODEL
                        or type(row.get("ok")) is not bool or row.get("realm", account["realm"]) != account["realm"]):
                    return account, 0
                account.update(model=ACCOUNT_TEST_MODEL, elapsed_ms=count(row.get("elapsed_ms")))
                status = row.get("status")
                if (row.get("network") is True or status is not None and
                        (type(status) is not int or status == 408 or status >= 500)):
                    account["error"] = "检测结果未知，网络或服务暂不可用"
                    return account, 0
                if not row["ok"]:
                    detail = row.get("error")
                    detail = re.sub(r"(?i)(authorization|password|api[_-]?key|token)\s*[:=]\s*(?:Bearer\s+)?\S+",
                                    r"\1=[已隐藏]", detail)[:240] if isinstance(detail, str) else "未提供失败原因"
                    account.update(status="failed", error=detail)
                    return account, 0
                if status is not None and not 200 <= status < 300:
                    return account, 0
                if account["enabled"]:
                    account.update(status="success", error="")
                    return account, 0
                enabled = self._enable_account(uid, expected_realm=account["realm"])
                if enabled["status"] != "enabled":
                    account["error"] = enabled.get("error", "启用结果待核对")
                    return account, 0
                account.update(enabled["account"], status="success", error="")
                return account, int(enabled["newly_enabled"])

            with ThreadPoolExecutor(max_workers=len(uids)) as pool:
                results = list(pool.map(probe, uids))
            accounts = [account for account, _ in results]
            statuses = {account["status"] for account in accounts}
            overall = "failed" if "failed" in statuses else "unknown" if "unknown" in statuses else "success"
            with self._lock:
                self._cache = None
            return {"status": overall, "accounts": accounts, "enabled_count": sum(n for _, n in results)}
        finally:
            self._test_lock.release()


MONITOR = Monitor()


class Handler(BaseHTTPRequestHandler):
    def _reply(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        origin = self.headers.get("Origin")
        if origin in ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        if self.headers.get("Origin") not in ALLOWED_ORIGINS:
            return self._reply(403, {"error": "forbidden"})
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Vary", "Origin")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.headers.get("Origin") not in ALLOWED_ORIGINS:
            return self._reply(403, {"error": "forbidden"})
        path = urlsplit(self.path).path
        if path == "/api/workbuddy/summary":
            return self._reply(200, MONITOR.summary())
        if path == "/api/workbuddy/accounts":
            return self._reply(200, MONITOR.list_accounts())
        return self._reply(404, {"error": "not found"})

    def do_POST(self):
        if self.headers.get("Origin") not in ALLOWED_ORIGINS:
            return self._reply(403, {"error": "forbidden"})
        if not self.headers.get("Content-Type", "").lower().startswith("application/json"):
            return self._reply(415, {"error": "json required"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 4096:
                raise ValueError("invalid length")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("invalid payload")
        except (ValueError, UnicodeDecodeError):
            return self._reply(400, {"error": "invalid request"})
        path = urlsplit(self.path).path
        if path == "/api/workbuddy/test":
            return self._reply(200, MONITOR.test_line(payload.get("model"), payload.get("realm")))
        if path == "/api/workbuddy/accounts/test":
            return self._reply(200, MONITOR.test_accounts())
        if path == "/api/workbuddy/accounts/disable":
            return self._reply(200, MONITOR.disable_account(payload.get("uid")))
        if path == "/api/workbuddy/accounts/enable":
            return self._reply(200, MONITOR.enable_account(payload.get("uid")))
        if path == "/api/workbuddy/accounts/refresh":
            return self._reply(200, MONITOR.refresh_accounts(payload.get("uids")))
        return self._reply(404, {"error": "not found"})

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    if sys.argv[1:] == ["--set-password"]:
        secret = sys.stdin.readline().rstrip("\r\n")
        if not secret:
            raise SystemExit("No password supplied")
        save_password(secret)
    else:
        ThreadingHTTPServer(("127.0.0.1", 8797), Handler).serve_forever()
