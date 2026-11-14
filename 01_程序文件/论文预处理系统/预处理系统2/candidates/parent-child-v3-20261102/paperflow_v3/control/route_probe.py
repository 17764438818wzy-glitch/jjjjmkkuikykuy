"""Identify the existing project route without exposing or changing credentials.

This deployment-specific candidate reuses the existing SSH monitor and the
gateway's read-only settings endpoint. A verified route identifies configuration;
it does not prove that a real request or any account test will succeed.
"""

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
from urllib.parse import urlsplit


BRIDGE = Path(__file__).resolve().parents[2] / "runtime" / "panel-monitor" / "panel_service.py"

# The only information emitted from the server is route metadata and key IDs.
# Both comparisons of credentials take place in server-side process memory.
REMOTE = r'''import hashlib, json, subprocess, sys, urllib.parse
p = json.loads(sys.argv[1])
digest, alias = p['digest'], p['alias']
def sql(query):
    return json.loads(subprocess.check_output(['docker', 'exec', '1Panel-postgresql-Bx3z', 'psql', '-U', 'user_QnrEMz', '-d', 'sub2api_nzmcfc', '-qAt', '-c', query]))
q = "SELECT COALESCE(json_agg(json_build_object('key_id',k.id,'key_status',k.status,'key_unexpired',k.expires_at IS NULL OR k.expires_at > now(),'user_status',u.status,'user_live',u.deleted_at IS NULL,'user_concurrency',u.concurrency,'group_id',g.id,'group_status',g.status,'group_live',g.deleted_at IS NULL,'fallback_group',g.fallback_group_id,'fallback_invalid',g.fallback_group_id_on_invalid_request,'model_routing_enabled',g.model_routing_enabled,'default_mapped_model',g.default_mapped_model,'allow_messages_dispatch',g.allow_messages_dispatch,'model_allowlist',g.model_allowlist)),'[]'::json) FROM api_keys k JOIN users u ON u.id=k.user_id JOIN groups g ON g.id=k.group_id WHERE k.deleted_at IS NULL AND encode(sha256(convert_to(k.key,'UTF8')),'hex') = '" + digest + "'"
incoming = sql(q)
if len(incoming) != 1:
    print(json.dumps({'incoming': incoming, 'routes': [], 'runtime_read_safe': False})); sys.exit()
group = int(incoming[0]['group_id'])
q = "SELECT COALESCE(json_agg(json_build_object('upstream_id',a.id,'type',a.type,'platform',a.platform,'upstream_concurrency',a.concurrency,'base_url',a.credentials->>'base_url','model_map',a.credentials->'model_mapping','proxy_id',a.proxy_id,'proxy_fallback',a.proxy_fallback_origin_id,'parent_account',a.parent_account_id,'secret_sha',CASE WHEN COALESCE(a.credentials->>'api_key','') <> '' THEN encode(sha256(convert_to(btrim(a.credentials->>'api_key'),'UTF8')),'hex') ELSE NULL END)),'[]'::json) FROM accounts a JOIN account_groups ag ON ag.account_id=a.id WHERE ag.group_id=" + str(group) + " AND a.deleted_at IS NULL AND a.status='active' AND a.schedulable IS TRUE"
rows = sql(q)
models = list({(row.get('model_map') or {}).get(alias) for row in rows} - {None})
script = """import sys,json,hashlib;sys.path.insert(0,'/opt/wb-node-hub/app');import wb_settings;d=wb_settings.load('/data/accounts');rows=wb_settings.api_keys('/data/accounts');safe=(isinstance(d.get(wb_settings.LIMITS_KEY),dict) or not any(k in d for k in wb_settings.LIMIT_KEYS)) and not (wb_settings.PRICING_REFRESH_MINUTES_KEY not in d and wb_settings.LEGACY_PRICING_REFRESH_HOURS_KEY in d);models=json.loads(sys.argv[1]);print(json.dumps({'keys':[{'id':v['id'],'enabled':v['enabled'],'realm':v['realm'],'concurrency':v.get('concurrency_limit'),'sha':hashlib.sha256(v['key'].encode('utf8')).hexdigest(),'model_allowed':{m:wb_settings.key_allows_model(v,m) for m in models}} for v in rows if v.get('key')], 'runtime_read_safe':safe}))"""
gateway = json.loads(subprocess.check_output(['docker', 'exec', 'workbuddy-node-hub-20261022', 'python3', '-c', script, json.dumps(models)]))
result = []
for a in rows:
    url = urllib.parse.urlsplit(a.pop('base_url') or '')
    model = (a.pop('model_map') or {}).get(alias)
    secret = a.pop('secret_sha')
    a['expected_gateway'] = url.scheme == p['gateway_scheme'] and url.hostname == p['gateway_host'] and url.port == p['gateway_port'] and url.path.rstrip('/') in ('', '/v1')
    a['model'] = model
    a['gateway_keys'] = [{**{k:v for k,v in key.items() if k not in ('sha', 'model_allowed')}, 'model_allowed': key['model_allowed'].get(model, False)} for key in gateway['keys'] if secret and key['sha'] == secret]
    result.append(a)
print(json.dumps({'incoming': incoming, 'routes': result, 'runtime_read_safe': gateway['runtime_read_safe']}))
'''


def _bridge():
    spec = importlib.util.spec_from_file_location("paperflow_monitor_bridge", BRIDGE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_route(digest, alias):
    bridge = _bridge()
    gateway = urlsplit(bridge.workbuddy.GATEWAY)
    params = json.dumps({"digest": digest, "alias": alias, "gateway_scheme": gateway.scheme,
                         "gateway_host": gateway.hostname, "gateway_port": gateway.port})
    command = "sudo -S -p '' -- python3 -c " + shlex.quote(REMOTE) + " " + shlex.quote(params)
    password = bridge.load_server_password()
    client = bridge.observer.connect(bridge.server.HOST, bridge.server.USERNAME, password=password)
    try:
        stdin, stdout, stderr = client.exec_command(command, timeout=25)
        stdin.write(password + "\n")
        stdin.flush()
        result = stdout.read()
        stderr.read()
        if stdout.channel.recv_exit_status() != 0:
            raise OSError("route lookup failed")
        return json.loads(result)
    finally:
        client.close()


def _read_runtime():
    bridge = _bridge()
    monitor = bridge.workbuddy.Monitor()
    if not monitor.login(bridge.workbuddy.load_password()):
        raise OSError("gateway session unavailable")
    return bridge.workbuddy.request_json("/settings", token=monitor._token)


def probe_route(inbound_key_sha256, model_alias, *, read_route=None, read_runtime=None):
    """Return a sanitized route or an unknown result. Never send model requests."""
    if not isinstance(inbound_key_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", inbound_key_sha256) or not isinstance(model_alias, str) or not model_alias:
        return {"status": "unknown", "reason": "项目凭据指纹或模型别名无效"}
    try:
        data = (read_route or _read_route)(inbound_key_sha256, model_alias)
        incoming, routes = data["incoming"], data["routes"]
        if len(incoming) != 1 or len(routes) != 1:
            return {"status": "unknown", "reason": "项目密钥或可调度上游无法唯一确定"}
        source, route = incoming[0], routes[0]
        if (source["key_status"] != "active" or not source["key_unexpired"]
                or source["user_status"] != "active" or not source["user_live"]
                or source["group_status"] != "active" or not source["group_live"]
                or source["fallback_group"] is not None or source["fallback_invalid"] is not None
                or source["model_routing_enabled"] is not False or source["default_mapped_model"]
                or source["allow_messages_dispatch"]):
            return {"status": "unknown", "reason": "项目组状态或分流配置无法确认"}
        allowed = source["model_allowlist"] or {}
        if allowed.get("enabled") is True and model_alias not in allowed.get("models", []):
            return {"status": "unknown", "reason": "项目模型不在组的白名单中"}
        keys = route["gateway_keys"]
        if (route["type"] != "apikey" or route["platform"] != "openai" or not route["expected_gateway"]
                or route["proxy_id"] is not None or route["proxy_fallback"] is not None
                or route["parent_account"] is not None or not route["model"] or len(keys) != 1
                or keys[0]["enabled"] is not True or keys[0]["model_allowed"] is not True
                or not data["runtime_read_safe"]):
            return {"status": "unknown", "reason": "上游、模型映射或网关密钥无法唯一确认"}
        settings = (read_runtime or _read_runtime)()
        matching = [key for key in settings["api_keys"] if key["id"] == keys[0]["id"]]
        cap = settings["account_model_limits"][route["model"].lower()]["cn"]
        if (len(matching) != 1 or matching[0]["enabled"] is not True
                or matching[0]["realm"] != keys[0]["realm"] or type(cap) is not int or cap != 2):
            return {"status": "unknown", "reason": "运行中网关密钥或国内账号并发无法确认"}
        caps = {"user": source["user_concurrency"], "upstream": route["upstream_concurrency"],
                "gateway_key": keys[0]["concurrency"]}
        if any(type(value) is not int or value < 0 for value in caps.values()):
            return {"status": "unknown", "reason": "线路并发上限不可读取"}
        result = {"status": "verified", "group_id": source["group_id"],
                  "upstream_id": route["upstream_id"], "gateway_key_id": keys[0]["id"],
                  "gateway_key_realm": keys[0]["realm"], "model": route["model"],
                  "concurrency_caps": caps, "per_account_limit": cap}
        identity = {"input_key_id": source["key_id"], "alias": model_alias, **result}
        result["route_fingerprint"] = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        seats = settings.get('seat_egress')
        if isinstance(seats, dict):
            # Keep credentials and changing in-flight counts out of the route.
            result['seat_egress'] = {key: seats.get(key) for key in
                ('protocol', 'configured_capacity', 'enabled_capacity', 'per_domestic_uid_limit')}
            rows = seats.get('groups')
            result['seat_egress']['groups'] = ([{key: row.get(key) for key in
                ('id', 'first', 'last', 'capacity', 'enabled', 'verified')} if isinstance(row, dict) else None
                for row in rows] if isinstance(rows, list) else None)
        return result
    except Exception:
        return {"status": "unknown", "reason": "线路配置或运行状态读取失败"}
