from paperflow_v3.control import route_probe


def observed(group_id=37):
    return {
        "incoming": [{"key_id": 24, "key_status": "active", "key_unexpired": True,
                      "user_status": "active", "user_live": True, "user_concurrency": 140,
                      "group_id": group_id, "group_status": "active", "group_live": True,
                      "fallback_group": None, "fallback_invalid": None,
                      "model_routing_enabled": False, "default_mapped_model": "",
                      "allow_messages_dispatch": False,
                      "model_allowlist": {"enabled": True, "models": ["paper-alias"]}}],
        "routes": [{"upstream_id": 91, "type": "apikey", "platform": "openai",
                    "upstream_concurrency": 140, "expected_gateway": True,
                    "proxy_id": None, "proxy_fallback": None, "parent_account": None,
                    "model": "cn-model", "gateway_keys": [{"id": "panel-id", "enabled": True,
                                                               "realm": "", "concurrency": 140,
                                                               "model_allowed": True}]}],
        "runtime_read_safe": True,
    }


def runtime():
    return {"api_keys": [{"id": "panel-id", "enabled": True, "realm": ""}],
            "account_model_limits": {"cn-model": {"cn": 2}}}


def probe(data, settings=None):
    return route_probe.probe_route("a" * 64, "paper-alias", read_route=lambda *_: data,
                                   read_runtime=lambda: runtime() if settings is None else settings)


def test_resolves_changed_group_and_never_returns_key_hash():
    assert callable(getattr(route_probe, "probe_route", None))
    result = probe(observed(73))
    assert result["status"] == "verified"
    assert result["group_id"] == 73
    assert result["gateway_key_id"] == "panel-id"
    assert result["model"] == "cn-model"
    assert result["concurrency_caps"] == {"user": 140, "upstream": 140, "gateway_key": 140}
    assert result["per_account_limit"] == 2
    assert "a" * 64 not in str(result)


def test_multiple_upstreams_are_unknown():
    data = observed()
    data["routes"].append(dict(data["routes"][0], upstream_id=92))
    assert probe(data)["status"] == "unknown"


def test_missing_model_or_multiple_gateway_keys_are_unknown():
    data = observed()
    data["routes"][0]["model"] = None
    assert probe(data)["status"] == "unknown"
    data = observed()
    data["routes"][0]["gateway_keys"].append(dict(data["routes"][0]["gateway_keys"][0], id="other"))
    assert probe(data)["status"] == "unknown"


def test_gateway_key_must_allow_the_project_model():
    data = observed()
    data["routes"][0]["gateway_keys"][0]["model_allowed"] = False
    assert probe(data)["status"] == "unknown"


def test_group_fallback_or_routing_is_unknown():
    data = observed()
    data["incoming"][0]["fallback_group"] = 40
    assert probe(data)["status"] == "unknown"
    data = observed()
    data["incoming"][0]["model_routing_enabled"] = True
    assert probe(data)["status"] == "unknown"


def test_runtime_must_show_two_slots_and_same_enabled_key():
    settings = runtime()
    settings["account_model_limits"]["cn-model"]["cn"] = 1
    assert probe(observed(), settings)["status"] == "unknown"
    settings = runtime()
    settings["api_keys"][0]["enabled"] = False
    assert probe(observed(), settings)["status"] == "unknown"


def test_connection_errors_are_unknown_and_do_not_disclose_error():
    def unavailable(*_):
        raise RuntimeError("secret-like upstream error")
    result = route_probe.probe_route("a" * 64, "paper-alias", read_route=unavailable)
    assert result["status"] == "unknown"
    assert "secret-like" not in str(result)
