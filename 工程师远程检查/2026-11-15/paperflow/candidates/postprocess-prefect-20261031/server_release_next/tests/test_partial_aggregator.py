"""Read-only, automatically discovered folder observations using synthetic fixtures."""
import importlib.util
from pathlib import Path
import threading
import time
import sqlite3

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    assert path.is_file(), 'the partial aggregation candidate is missing'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_aggregator_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_server_projection.py')


def test_one_slow_folder_does_not_hide_another_verified_folder(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    first, second = fixtures.add_complete_pass(db, 1), fixtures.add_complete_pass(db, 2)
    inspector, calls = fixtures.make_inspector(db)
    remote = inspector._inspect_batch
    entered, release = threading.Event(), threading.Event()
    def inspect(requests):
        if requests[0]['job']['identity']['group_id'] == second['group_id']:
            entered.set(); release.wait(timeout=3)
            raise ValueError('synthetic historical version unavailable')
        return remote(requests)
    inspector._inspect_batch = inspect
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_aggregator_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    try:
        observer.snapshot()
        assert entered.wait(timeout=2)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            value = observer.snapshot(result_filter='passed')
            if value['results']['counts']['passed'] == 1: break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
        assert value['results']['partial'] is True
        assert value['stage']['counts']['waiting_rag_upload'] is None
        assert value['stage']['counts']['waiting_postprocessing'] is None
        assert value['server_observation']['scope'] == 'verified_registered_subset'
        rows = {row['group_id']: row for row in value['results']['list']['items']}
        assert set(rows) == {first['group_id'], second['group_id']}
        assert rows[first['group_id']].get('verification_pending') is not True
        assert rows[second['group_id']]['verification_pending'] is True
        assert value['results']['list']['total'] is None
        api = load('synthetic_aggregator_waiting_api', HERE / 'waiting_api.py')
        assert api._valid_registered(value, 'passed', 1, 20, None, allow_partial_listing=True) is True
        assert api._valid_registered(value, 'passed', 1, 20, None) is False
        assert api._normalize_server_observation(value)['results']['counts']['passed'] == 1
        assert len(calls) == 1
    finally:
        release.set()
        observer.close()


def test_pending_only_results_can_paginate_with_the_client_visible_revision(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    fixtures.add_complete_pass(db, 2)
    inspector, _ = fixtures.make_inspector(db)
    release = threading.Event()
    inspector._inspect_batch = lambda _: (release.wait(timeout=3), (_ for _ in ()).throw(ValueError()))[1]
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_pending_pagination_server', HERE / 'server_projection.py')
    partial = load('synthetic_pending_pagination_partial', HERE / 'partial_projection.py')
    module = load('synthetic_pending_pagination', HERE / 'partial_aggregator.py')
    api = load('synthetic_pending_pagination_api', HERE / 'waiting_api.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    try:
        first = api._normalize_server_observation(observer.snapshot(result_filter='passed', page=1, page_size=1))
        assert first['results']['list']['has_next'] is True
        assert first['results']['list']['items'][0]['verification_pending'] is True
        second = observer.snapshot(result_filter='passed', page=2, page_size=1, expected_revision=first['revision'])
        assert second['available'] is True
        assert second['results']['list']['items'][0]['verification_pending'] is True
        assert second['results']['list']['items'][0]['group_id'] != first['results']['list']['items'][0]['group_id']
    finally:
        release.set()
        observer.close()


def test_unfinished_or_unknown_folder_never_causes_remote_inspection(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.helpers.add_folder(db, 1, job='execution_unknown', claim='unknown',
                                outcome=('unresolved', 'outcome_unknown'), receipt=False)
    inspector, calls = fixtures.make_inspector(db)
    server = load('synthetic_aggregator_unknown_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_unknown_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator_unknown', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=object(), server_helpers=server, partial=partial)
    try:
        value = observer.snapshot()
        assert value['results']['counts']['passed'] is None
        assert value['stage']['counts']['waiting_rag_upload'] is None
        assert calls == []
    finally:
        observer.close()


def test_verified_folder_refreshes_in_background_before_its_observation_expires(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    original = load('synthetic_refresh_partial', HERE / 'partial_projection.py')
    server = load('synthetic_refresh_server', HERE / 'server_projection.py')
    module = load('synthetic_refresh_aggregator', HERE / 'partial_aggregator.py')
    calls, entered, release = [], threading.Event(), threading.Event()
    class Partial:
        registered_candidates = staticmethod(original.registered_candidates)
        @staticmethod
        def observe_one(database, key, **_kwargs):
            calls.append(key)
            if len(calls) > 1:
                entered.set(); release.wait(timeout=2)
            base, items = original.registered_candidates(database, ledger_snapshot=fixtures.helpers.projection.snapshot,
                component_id_fn=fixtures.helpers.upstream.component_id, server_helpers=server)
            now = time.monotonic()
            return dict(available=True, scope='verified_registered_subset', item=items[0],
                        ledger_revision=base['revision'], as_of='2026-11-01T00:00:00Z',
                        observed_monotonic=now, valid_until_monotonic=now + .6)
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=object(), server_helpers=server, partial=Partial(),
        refresh_before_seconds=.3)
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value['results']['counts']['passed'] == 1: break
            time.sleep(.01)
        assert value['results']['counts']['passed'] == 1
        time.sleep(.35)
        still_valid = observer.snapshot()
        assert still_valid['results']['counts']['passed'] == 1
        assert entered.wait(timeout=1) and len(calls) == 2
        assert calls[1] == (identity['task_id'], identity['group_id'])
    finally:
        release.set()
        observer.close()


def test_many_waiting_folders_are_observed_before_early_folders_refresh(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identities = [fixtures.add_complete_pass(db, number) for number in range(1, 41)]
    inspector, _ = fixtures.make_inspector(db)
    server = load('synthetic_fairness_server', HERE / 'server_projection.py')
    original = load('synthetic_fairness_partial', HERE / 'partial_projection.py')
    module = load('synthetic_fairness_aggregator', HERE / 'partial_aggregator.py')
    calls, active, max_active, protect = [], [0], [0], threading.Lock()
    class Partial:
        registered_candidates = staticmethod(original.registered_candidates)
        @staticmethod
        def observe_one(database, key, **_kwargs):
            with protect:
                calls.append(key)
                active[0] += 1
                max_active[0] = max(max_active[0], active[0])
            try:
                time.sleep(.01)
                base, items = original.registered_candidates(database,
                    ledger_snapshot=fixtures.helpers.projection.snapshot,
                    component_id_fn=fixtures.helpers.upstream.component_id, server_helpers=server)
                item = next(row for row in items if (row['task_id'], row['group_id']) == key)
                now = time.monotonic()
                return dict(available=True, scope='verified_registered_subset', item=item,
                            ledger_revision=base['revision'], as_of='2026-11-01T00:00:00Z',
                            observed_monotonic=now, valid_until_monotonic=now + .08)
            finally:
                with protect:
                    active[0] -= 1
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=object(), server_helpers=server, partial=Partial(),
        refresh_before_seconds=.07)
    expected = {(identity['task_id'], identity['group_id']) for identity in identities}
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            observer.snapshot()
            with protect:
                if expected.issubset(set(calls)):
                    break
            time.sleep(.025)
        with protect:
            first = list(calls[:40])
        assert set(first) == expected
        assert len(first) == 40
        assert max_active[0] <= 2
    finally:
        observer.close()


def test_llm_retry_change_invalidates_cached_observation_and_rechecks_after_recovery(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    with sqlite3.connect(db.path) as conn:
        conn.execute('CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT, retry_key TEXT)')
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, _request, _keys):
            with sqlite3.connect(db.path) as conn:
                return conn.execute('SELECT COUNT(*) FROM paperflow_llm_stage_jobs').fetchone()[0] == 0
    server = load('synthetic_retry_changed_server', HERE / 'server_projection.py')
    partial = load('synthetic_retry_changed_partial', HERE / 'partial_projection.py')
    module = load('synthetic_retry_changed_aggregator', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            before = observer.snapshot()
            if before['results']['counts']['passed'] == 1:
                break
            time.sleep(.02)
        assert before['results']['counts']['passed'] == 1
        old_revision = fixtures.helpers.projection.snapshot(db.path,
            component_id_fn=fixtures.helpers.upstream.component_id)['revision']
        with sqlite3.connect(db.path) as conn:
            conn.execute('INSERT INTO paperflow_llm_stage_jobs VALUES (?,?)', ('retry-job', 'retry-key'))
        assert fixtures.helpers.projection.snapshot(db.path,
            component_id_fn=fixtures.helpers.upstream.component_id)['revision'] == old_revision
        changed = observer.snapshot()
        assert changed.get('results', {}).get('counts', {}).get('passed') is None
        with sqlite3.connect(db.path) as conn:
            conn.execute('DELETE FROM paperflow_llm_stage_jobs')
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            recovered = observer.snapshot()
            if recovered.get('results', {}).get('counts', {}).get('passed') == 1:
                break
            time.sleep(.02)
        assert recovered['results']['counts']['passed'] == 1
    finally:
        observer.close()


def test_control_change_during_observation_discards_old_result_and_reobserves(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    with sqlite3.connect(db.path) as conn:
        conn.execute('CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT, retry_key TEXT)')
    inspector, _ = fixtures.make_inspector(db)
    original_remote = inspector._inspect_batch
    entered_first, release_first = threading.Event(), threading.Event()
    finished_first, entered_second, release_second = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def inspect(requests):
        calls.append(True)
        if len(calls) == 1:
            entered_first.set()
            assert release_first.wait(timeout=3)
        else:
            entered_second.set()
            assert release_second.wait(timeout=3)
        return original_remote(requests)
    inspector._inspect_batch = inspect
    class Scope:
        def verify_selected(self, _request, _keys): return True
    server = load('synthetic_inflight_revision_server', HERE / 'server_projection.py')
    original = load('synthetic_inflight_revision_partial', HERE / 'partial_projection.py')
    module = load('synthetic_inflight_revision_aggregator', HERE / 'partial_aggregator.py')
    class Partial:
        registered_candidates = staticmethod(original.registered_candidates)
        @staticmethod
        def observe_one(*args, **kwargs):
            result = original.observe_one(*args, **kwargs)
            if len(calls) == 1:
                finished_first.set()
            return result
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=Partial())
    try:
        observer.snapshot()
        assert entered_first.wait(timeout=3)
        with sqlite3.connect(db.path) as conn:
            conn.execute('INSERT INTO paperflow_llm_stage_jobs VALUES (?,?)', ('retry-job', 'retry-key'))
        release_first.set()
        assert finished_first.wait(timeout=3)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not entered_second.is_set():
            value = observer.snapshot()
            assert value.get('results', {}).get('counts', {}).get('passed') is None
            time.sleep(.02)
        assert entered_second.is_set(), 'the changed control state must trigger a new observation'
        release_second.set()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value.get('results', {}).get('counts', {}).get('passed') == 1:
                break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
    finally:
        release_first.set()
        release_second.set()
        observer.close()


def test_control_change_during_final_check_cannot_publish_cached_observation(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    with sqlite3.connect(db.path) as conn:
        conn.execute('CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT, retry_key TEXT)')
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, _request, _keys): return True
    phase = {'armed': False, 'checks': 0}
    def check_inputs():
        if phase['armed']:
            phase['checks'] += 1
            if phase['checks'] == 2:
                with sqlite3.connect(db.path) as conn:
                    conn.execute('INSERT INTO paperflow_llm_stage_jobs VALUES (?,?)', ('retry-job', 'retry-key'))
    server = load('synthetic_final_revision_server', HERE / 'server_projection.py')
    partial = load('synthetic_final_revision_partial', HERE / 'partial_projection.py')
    module = load('synthetic_final_revision_aggregator', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial,
        check_inputs=check_inputs)
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value['results']['counts']['passed'] == 1:
                break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
        phase['armed'] = True
        result = observer.snapshot()
        assert phase['checks'] == 2
        assert result.get('results', {}).get('counts', {}).get('passed') is None
    finally:
        observer.close()


def test_changed_registration_never_reuses_an_old_verified_receipt(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_aggregator_changed_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_changed_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator_changed', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value['results']['counts']['passed'] == 1: break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
        with sqlite3.connect(db.path) as conn:
            conn.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
        value = observer.snapshot()
        assert value['results']['counts']['passed'] is None
        assert value['server_observation']['available'] is False
    finally:
        observer.close()


def test_changed_release_inputs_close_the_partial_projection_immediately(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    allowed = [True]
    def check_inputs():
        if not allowed[0]: raise ValueError('synthetic approved source changed')
    server = load('synthetic_aggregator_release_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_release_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator_release', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial,
        check_inputs=check_inputs)
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value['results']['counts']['passed'] == 1: break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
        allowed[0] = False
        assert observer.snapshot()['available'] is False
    finally:
        observer.close()


def test_deletion_during_final_input_check_is_not_published(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    phase = {'delete': False, 'checks': 0}
    def check_inputs():
        phase['checks'] += 1
        if phase['delete'] and phase['checks'] % 2 == 0:
            with sqlite3.connect(db.path) as conn:
                conn.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
    server = load('synthetic_aggregator_during_delete_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_during_delete_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator_during_delete', HERE / 'partial_aggregator.py')
    observer = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial,
        check_inputs=check_inputs)
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            value = observer.snapshot()
            if value['results']['counts']['passed'] == 1: break
            time.sleep(.02)
        assert value['results']['counts']['passed'] == 1
        phase.update(delete=True, checks=0)
        assert observer.snapshot()['available'] is False
    finally:
        observer.close()


def test_verified_subset_reaches_the_actual_candidate_http_projection(tmp_path):
    from fastapi.testclient import TestClient
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_aggregator_http_server', HERE / 'server_projection.py')
    partial = load('synthetic_aggregator_http_partial', HERE / 'partial_projection.py')
    module = load('synthetic_aggregator_http', HERE / 'partial_aggregator.py')
    api = load('synthetic_aggregator_http_api', HERE / 'waiting_api.py')
    bounded = load('synthetic_aggregator_http_bounded', HERE / 'bounded_summary.py')
    api.PROJECTION_REFRESH_SECONDS = .01
    api.PROJECTION_MAX_AGE_SECONDS = .3
    aggregate = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='synthetic', scan_source_instance='synthetic',
                        waiting_available=True, upstream_scan_verified=True,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z', waiting_llm_total=0)
    class Observer:
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
    try:
        app = api.create_app(Queue(), ticket_observer=Observer(), registered_projection=aggregate.snapshot,
                             postprocess_only=True, partial_observation=True)
        with TestClient(bounded.BoundedSummary(app, max_age_seconds=.3)) as client:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                response = client.get('/api/v1/local-intake/preprocessing/summary',
                                      params={'postprocessing_result_filter': 'passed'})
                if response.status_code == 200:
                    ledger = response.json()['postprocessing_registered_ledger']
                    if ledger.get('results', {}).get('counts', {}).get('passed') == 1: break
                time.sleep(.03)
            else:
                raise AssertionError('the verified subset did not reach the candidate HTTP API')
            assert ledger['results']['partial'] is True
            assert ledger['stage']['counts']['waiting_rag_upload'] is None
            assert [item['group_id'] for item in ledger['results']['list']['items']] == [identity['group_id']]
            assert 0 < response.json()['postprocessing_observation_remaining_ms'] <= 30000
    finally:
        aggregate.close()


def test_pending_polls_do_not_hide_a_slow_verified_folder_from_http(tmp_path):
    from fastapi.testclient import TestClient
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    remote = inspector._inspect_batch
    entered, release = threading.Event(), threading.Event()
    def inspect(requests):
        entered.set()
        assert release.wait(timeout=4)
        return remote(requests)
    inspector._inspect_batch = inspect
    class Scope:
        def verify_selected(self, _request, _keys): return True
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='synthetic', scan_source_instance='synthetic',
                        waiting_available=True, upstream_scan_verified=True,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z', waiting_llm_total=0)
    class Observer:
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
    server = load('synthetic_slow_http_server', HERE / 'server_projection.py')
    partial = load('synthetic_slow_http_partial', HERE / 'partial_projection.py')
    module = load('synthetic_slow_http_aggregator', HERE / 'partial_aggregator.py')
    api = load('synthetic_slow_http_api', HERE / 'waiting_api.py')
    bounded = load('synthetic_slow_http_bounded', HERE / 'bounded_summary.py')
    revision = load('synthetic_slow_http_revision', HERE / 'revision_witness.py').ReadOnlyRevision(db.path)
    api.PROJECTION_REFRESH_SECONDS = .05
    api.PROJECTION_MAX_AGE_SECONDS = 2
    aggregate = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial,
        revision_witness=revision.current)
    app = api.create_app(Queue(), ticket_observer=Observer(), registered_projection=aggregate.snapshot,
                         postprocess_only=True, partial_observation=True, registered_revision=revision.current)
    try:
        witness = lambda: (revision.current(), app.state.postprocessing_cache_revision())
        with TestClient(bounded.BoundedSummary(app, max_age_seconds=2, revision_witness=witness)) as client:
            client.get('/api/v1/local-intake/preprocessing/summary')
            assert entered.wait(timeout=2)
            # Force several later pending snapshots while this observation is
            # in progress. They are not failed server observations.
            for _ in range(4):
                client.get('/api/v1/local-intake/preprocessing/summary')
                time.sleep(.08)
            release.set()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                response = client.get('/api/v1/local-intake/preprocessing/summary')
                if response.status_code == 200:
                    ledger = response.json()['postprocessing_registered_ledger']
                    if ledger.get('results', {}).get('counts', {}).get('passed') == 1:
                        break
                time.sleep(.03)
            else:
                raise AssertionError('a completed verified folder was hidden by later pending polls')
    finally:
        release.set()
        aggregate.close()
        revision.close()


def test_late_real_aggregator_failure_invalidates_another_filter_cached_pass(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, _request, _keys): return True
    entered, release = threading.Event(), threading.Event()
    state = {'armed': False, 'taken': False}
    def check_inputs():
        if state['armed'] and not state['taken']:
            state['taken'] = True
            entered.set()
            assert release.wait(timeout=3)
            raise ValueError('synthetic release input became unavailable')
    server = load('synthetic_late_failure_server', HERE / 'server_projection.py')
    partial = load('synthetic_late_failure_partial', HERE / 'partial_projection.py')
    module = load('synthetic_late_failure_aggregator', HERE / 'partial_aggregator.py')
    api = load('synthetic_late_failure_api', HERE / 'waiting_api.py')
    aggregate = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial,
        check_inputs=check_inputs)
    cache = api._ProjectionCache(invalidate_server_failures=True)
    epoch = cache.bind_source('synthetic-source', 1)
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            result = aggregate.snapshot()
            if result.get('results', {}).get('counts', {}).get('passed') == 1:
                break
            time.sleep(.02)
        assert result['results']['counts']['passed'] == 1
        state['armed'] = True
        cache.get('older-failing-filter', aggregate.snapshot, expected_epoch=epoch)
        assert entered.wait(timeout=2)
        cache.get('newer-passing-filter', aggregate.snapshot, expected_epoch=epoch)
        deadline = time.monotonic() + 2
        while 'newer-passing-filter' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert cache.entries['newer-passing-filter']['value']['results']['counts']['passed'] == 1
        release.set()
        deadline = time.monotonic() + 2
        while 'older-failing-filter' in cache.inflight and time.monotonic() < deadline:
            time.sleep(.01)
        assert 'newer-passing-filter' not in cache.entries
        failure = cache.entries['older-failing-filter']['value']
        assert failure['available'] is False
    finally:
        release.set()
        aggregate.close()


def test_partial_http_can_display_one_verified_folder_when_global_scan_is_incomplete(tmp_path):
    from fastapi.testclient import TestClient
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_incomplete_http_server', HERE / 'server_projection.py')
    partial = load('synthetic_incomplete_http_partial', HERE / 'partial_projection.py')
    module = load('synthetic_incomplete_http_aggregator', HERE / 'partial_aggregator.py')
    api = load('synthetic_incomplete_http_api', HERE / 'waiting_api.py')
    bounded = load('synthetic_incomplete_http_bounded', HERE / 'bounded_summary.py')
    api.PROJECTION_REFRESH_SECONDS, api.PROJECTION_MAX_AGE_SECONDS = .01, .3
    aggregate = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='synthetic', scan_source_instance='synthetic',
                        waiting_available=False, upstream_scan_verified=False,
                        upstream_scan_completed_at=None, waiting_llm_total=None)
    class Observer:
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=False, counts={})
    try:
        app = api.create_app(Queue(), ticket_observer=Observer(), registered_projection=aggregate.snapshot,
                             postprocess_only=True, partial_observation=True)
        with TestClient(bounded.BoundedSummary(app, max_age_seconds=.3)) as client:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                response = client.get('/api/v1/local-intake/preprocessing/summary')
                if response.status_code == 200:
                    ledger = response.json()['postprocessing_registered_ledger']
                    if ledger.get('results', {}).get('counts', {}).get('passed') == 1: break
                time.sleep(.03)
            else:
                raise AssertionError('a separately verified folder was hidden by unrelated global incompleteness')
            assert ledger['results']['partial'] is True
            assert ledger['stage']['counts']['waiting_postprocessing'] is None
            assert ledger['stage']['counts']['waiting_rag_upload'] is None
    finally:
        aggregate.close()


def test_both_http_caches_stop_serving_a_deleted_registered_folder(tmp_path):
    from fastapi.testclient import TestClient
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(db, 1)
    inspector, _ = fixtures.make_inspector(db)
    class Scope:
        def verify_selected(self, request, keys): return True
    server = load('synthetic_delete_http_server', HERE / 'server_projection.py')
    partial = load('synthetic_delete_http_partial', HERE / 'partial_projection.py')
    module = load('synthetic_delete_http_aggregator', HERE / 'partial_aggregator.py')
    api = load('synthetic_delete_http_api', HERE / 'waiting_api.py')
    bounded = load('synthetic_delete_http_bounded', HERE / 'bounded_summary.py')
    witness_module = load('synthetic_delete_http_witness', HERE / 'revision_witness.py')
    api.PROJECTION_REFRESH_SECONDS, api.PROJECTION_MAX_AGE_SECONDS = .01, 8
    witness = witness_module.ReadOnlyRevision(db.path)
    aggregate = module.PartialAggregator(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server, partial=partial)
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='synthetic', scan_source_instance='synthetic',
                        waiting_available=True, upstream_scan_verified=True,
                        upstream_scan_completed_at='2026-11-01T00:00:00Z', waiting_llm_total=0)
    class Observer:
        def summary(self, _): return dict(source='prefect', scope='all_tasks', available=True, counts={})
    try:
        app = api.create_app(Queue(), ticket_observer=Observer(), registered_projection=aggregate.snapshot,
            postprocess_only=True, partial_observation=True, registered_revision=witness.current)
        with TestClient(bounded.BoundedSummary(app, max_age_seconds=.3, revision_witness=witness.current)) as client:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                response = client.get('/api/v1/local-intake/preprocessing/summary')
                if response.status_code == 200 and response.json()['postprocessing_registered_ledger'].get(
                        'results', {}).get('counts', {}).get('passed') == 1: break
                time.sleep(.03)
            else:
                raise AssertionError('synthetic receipt did not reach the API')
            with sqlite3.connect(db.path) as conn:
                conn.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
            result = client.get('/api/v1/local-intake/preprocessing/summary')
            assert result.status_code == 503 or result.json()['postprocessing_registered_ledger'].get(
                'results', {}).get('counts', {}).get('passed') != 1
    finally:
        aggregate.close()
        witness.close()
