"""Connect the existing gateway and Prefect limits to an explicit start."""
import asyncio
import hashlib
import json

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate


class CapacityError(RuntimeError):
    """A safe, user-readable reason why capacity was not confirmed."""


class SeatCapacityError(CapacityError):
    """The saved fixed-seat configuration could not be confirmed."""


def checked_seats(route):
    value = route.get('seat_egress')
    if (not isinstance(value, dict) or value.get('protocol') != 'fixed-seat-egress-v1'
            or type(value.get('configured_capacity')) is not int or value['configured_capacity'] != 50
            or type(value.get('per_domestic_uid_limit')) is not int or value['per_domestic_uid_limit'] != 2
            or type(value.get('enabled_capacity')) is not int):
        raise SeatCapacityError('出口席位协议、总配置50或国内单号2上限未确认')
    groups = value.get('groups')
    if (not isinstance(groups, list) or not 1 <= len(groups) <= 3
            or any(not isinstance(row, dict) or not isinstance(row.get('id'), str) for row in groups)):
        raise SeatCapacityError('出口席位布局未确认')
    by_id = {row['id']: row for row in groups}
    if len(by_id) != len(groups) or not set(by_id) <= {'local', 'bwg', 'aliyun'}:
        raise SeatCapacityError('出口席位分组身份未确认')
    clean = []
    seats = []
    for row in groups:
        if (any(type(row.get(k)) is not int for k in ('first', 'last', 'capacity'))
                or not 1 <= row['first'] <= row['last'] <= 50
                or row['capacity'] != row['last'] - row['first'] + 1
                or any(type(row.get(k)) is not bool for k in ('enabled', 'verified'))
                or row['enabled'] and not row['verified']):
            raise SeatCapacityError('出口席位区间、数量或启用核验状态未确认')
        clean.append({k: row[k] for k in ('id', 'first', 'last', 'capacity', 'enabled', 'verified')})
        seats.extend(range(row['first'], row['last'] + 1))
    if sorted(seats) != list(range(1, 51)):
        raise SeatCapacityError('出口席位必须无重叠且完整覆盖1至50')
    clean.sort(key=lambda row: row['first'])
    enabled = sum(row['capacity'] for row in clean if row['enabled'] and row['verified'])
    if value['enabled_capacity'] != enabled:
        raise SeatCapacityError('出口启用席位合计与分组配置不一致')
    if enabled == 0:
        raise SeatCapacityError('当前没有已启用且已核验的出口席位，本次未就绪')
    summary = {'configured_capacity': 50, 'enabled_capacity': enabled, 'per_domestic_uid_limit': 2,
               'protocol': 'fixed-seat-egress-v1', 'groups': clean}
    return {**summary, 'fingerprint': hashlib.sha256(json.dumps(summary, sort_keys=True).encode()).hexdigest()}


def capacity_fields(result):
    if result.get('capacity_policy') != 'domestic-credit-pool-v1':
        return {}
    return {key: result[key] for key in ('capacity_policy', 'account_capacity', 'seat_capacity', 'confirmed_capacity')}


def capacity_description(result):
    return (f"总上限 {result['llm_limit']}，本次计算容量上限 {result['confirmed_capacity']}"
            f"（账号探测通过 {result['successful_count']} 个，启用席位 {result['seat_capacity']}；"
            '按本次账号探测和席位配置计算）')


def tested_llm_limit(result):
    count = result.get('successful_count')
    limit = result.get('llm_limit', count * 2 if type(count) is int else None)
    if result.get('capacity_policy') == 'domestic-credit-pool-v1':
        if type(count) is not int or count < 0 or type(limit) is not int or limit != 50:
            raise CapacityError('国内额度池总上限必须为50，账号确认数量必须有效')
        seats = checked_seats(result.get('route') or {})
        expected = min(50, count * 2, seats['enabled_capacity'])
        if (result.get('seat_fingerprint') != seats['fingerprint']
                or any(type(result.get(key)) is not int for key in ('account_capacity', 'seat_capacity', 'confirmed_capacity', 'upstream_capacity'))
                or result['seat_capacity'] != seats['enabled_capacity'] or result['account_capacity'] != count * 2
                or result['confirmed_capacity'] != expected or result['upstream_capacity'] != expected):
            raise CapacityError('国内额度池账号与出口席位计算容量未确认')
        return limit
    if (type(count) is not int or count < 0 or type(limit) is not int
            or not (limit == 0 if count == 0 else 0 < limit <= count * 2)):
        raise CapacityError('已确认账号数量与 LLM 总并发不一致，本次未启动')
    return limit


def _confirmed_uids(result, field, *, credit_pool=False):
    accounts, uids = result.get("accounts"), result.get(field)
    allowed_states = ("success", "failed", "busy", "unknown") if credit_pool else ("success", "failed")
    complete = (result.get('status') in ('available','incomplete') and type(result.get('complete')) is bool
                if credit_pool else result.get('status') == 'available' and result.get('complete') is True)
    if (not complete
            or not isinstance(accounts, list) or not isinstance(uids, list)
            or any(not isinstance(uid, str) or not uid for uid in uids)):
        raise CapacityError("账号探测尚未全部确认，保留现有账号范围和并发，本次未启动")
    if any(not isinstance(row, dict) or row.get("realm") != "cn" or type(row.get("enabled")) is not bool
           or row["enabled"] and row.get("status") not in allowed_states for row in accounts):
        raise CapacityError("账号探测含忙或未知结果，保留现有账号范围和并发，本次未启动")
    expected = [row.get("uid") for row in accounts if row["enabled"] and row.get("status") == "success"]
    if len(uids) != len(set(uids)) or len(uids) != len(expected) or set(uids) != set(expected):
        raise CapacityError("成功账号身份未确认，保留现有账号范围和并发，本次未启动")
    return uids


class AutoCapacity:
    def __init__(self, gateway, read_route, client_factory=get_client, read_limit=None, *, credit_pool=False):
        self.gateway = gateway
        self.read_route = read_route
        self.client_factory = client_factory
        self.read_limit = read_limit
        self.credit_pool = credit_pool is True
        self.apply_lock = asyncio.Lock()

    def _target_limit(self, count):
        configured = self.read_limit() if self.read_limit is not None else None
        if configured is not None and (type(configured) is not int or configured <= 0):
            raise CapacityError('项目 LLM 总并发配置无效，本次未启动')
        if self.credit_pool:
            if configured != 50:
                raise CapacityError('国内额度池的项目总上限必须为50，本次未启动')
            return 50
        return min(configured, count * 2) if configured is not None else count * 2

    async def test_start_accounts(self, *, ensure_current):
        route = await asyncio.to_thread(self.read_route)
        if route["per_account_limit"] != 2:
            raise CapacityError("国内账号每号 2 并发尚未确认，本次未启动")
        seats = checked_seats(route) if self.credit_pool else None
        loop = asyncio.get_running_loop()

        def before_batch():
            asyncio.run_coroutine_threadsafe(ensure_current(), loop).result()

        result = await asyncio.to_thread(self.gateway.test_domestic_accounts, route["model"], before_batch=before_batch)
        uids = _confirmed_uids(result, "usable_uids", credit_pool=self.credit_pool)
        observed = {"status": result['status'], "complete": result['complete'], "accounts": result["accounts"],
                "successful_count": len(uids), "account_uids": uids, "route": route,
                "llm_limit": self._target_limit(len(uids))}
        if self.credit_pool:
            await ensure_current()
            current = await asyncio.to_thread(self.read_route)
            current_seats = checked_seats(current)
            if current['route_fingerprint'] != route['route_fingerprint'] or current_seats['fingerprint'] != seats['fingerprint']:
                raise SeatCapacityError('自检期间线路或出口席位发生变化，请重新核对')
            confirmed = min(50, len(uids) * 2, seats['enabled_capacity'])
            observed.update(capacity_policy='domestic-credit-pool-v1', upstream_capacity=confirmed,
                            account_capacity=len(uids)*2, seat_capacity=seats['enabled_capacity'],
                            confirmed_capacity=confirmed, seat_fingerprint=seats['fingerprint'])
        return observed

    async def apply_start_capacity(self, result, limit, *, ensure_current):
        # Serialize this shared configuration, while Pause remains independent.
        async with self.apply_lock:
            uids = _confirmed_uids(result, "account_uids", credit_pool=self.credit_pool)
            if (type(result.get("successful_count")) is not int or result["successful_count"] != len(uids)
                    or not uids or type(limit) is not int or limit != tested_llm_limit(result)
                    or limit != self._target_limit(len(uids))):
                raise CapacityError("账号数量与并发不一致，保留现有账号范围和并发，本次未启动")
            route = await asyncio.to_thread(self.read_route)
            previous = result["route"]
            if self.credit_pool and checked_seats(route)['fingerprint'] != result['seat_fingerprint']:
                raise SeatCapacityError('账号测试后出口席位配置已变化，本次未写入或启动')
            if (route["route_fingerprint"] != previous["route_fingerprint"]
                    or route["model"] != previous["model"] or route["gateway_key_id"] != previous["gateway_key_id"]
                    or route["per_account_limit"] != 2):
                raise CapacityError("账号测试期间论文线路发生变化，请重新核对")
            for name, cap in route["concurrency_caps"].items():
                if name == "gateway_key" and type(cap) is int and cap == 0:
                    continue  # The existing gateway uses zero for no key-level limit.
                if not isinstance(cap, int) or isinstance(cap, bool) or cap <= 0:
                    raise CapacityError("路由并发上限尚未确认，本次未启动")
                if cap < limit:
                    label = {"user": "项目账号", "upstream": "路由线路", "gateway_key": "网关线路"}.get(name, "路由")
                    raise CapacityError(f"{label}上限为 {cap}，低于所需并发 {limit}，本次未启动")
            await ensure_current()
            if self.credit_pool:
                if route.get('gateway_key_realm') != 'cn':
                    raise CapacityError('国内额度池必须绑定国内线路，本次未启动')
                applied = await asyncio.to_thread(self.gateway.allow_domestic_pool, route['gateway_key_id'], limit)
            else:
                applied = await asyncio.to_thread(self.gateway.allow_domestic_accounts,
                                                  route["gateway_key_id"], result["account_uids"])
            if applied["status"] != "confirmed":
                raise CapacityError("线路账号更新结果未确认，本次未启动")
            async with self.client_factory() as client:
                for kind in ("total_llm", "body_llm", "attachment_llm"):
                    name = "paperflow-connected-" + kind
                    await ensure_current()
                    await client.update_global_concurrency_limit(name, GlobalConcurrencyLimitUpdate(limit=limit))
                    observed = await client.read_global_concurrency_limit_by_name(name)
                    if not observed.active or observed.limit != limit or observed.slot_decay_per_second:
                        raise CapacityError("Prefect 并发上限更新结果未确认，本次未启动")
