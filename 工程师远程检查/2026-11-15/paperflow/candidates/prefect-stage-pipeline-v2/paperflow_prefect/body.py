"""Body-only native preparation, MD extraction, and immutable publication."""
from pathlib import Path
from .records import read_json, write_json, sha_file, identity


def prepare_body(source, work, catalog_path, *, max_pages=160):
    from preextract_material import prepare_material
    return prepare_material(source, work, catalog_path, render_missing=True,
                            max_pages=max_pages, associated_records=None)


def extract_profile(work, template_path, catalog, connection, invoke):
    from .records import Records
    return execute_profile_steps(work, lambda: profile_steps(work, template_path, catalog, connection,
        cached_result=Records(work).cached('profile')), invoke)


def execute_profile_steps(work, factory, invoke):
    """Keep the existing synchronous lock/error contract around the shared steps."""
    from filelock import Timeout
    from .profile_recovery import ProfileRecoveryError, recovery_lock, inspect_request
    try:
        with recovery_lock(work):
            return consume_profile_steps(factory(), invoke)
    except ProfileRecoveryError:
        raise
    except Timeout as exc:
        raise ProfileRecoveryError('busy') from exc
    except Exception as exc:
        try:
            observed = inspect_request(work)
            code = observed['code'] if observed['outcome'] not in {'complete', 'not_started'} else 'profile_error'
        except ProfileRecoveryError:
            code = 'evidence_changed'
        raise ProfileRecoveryError(code) from exc


def consume_profile_steps(steps, invoke):
    """Synchronous consumer of the same build/send/offline suspension contract."""
    response = None
    try:
        while True:
            try: request, connection, directory = steps.send(response)
            except StopIteration as finished: return finished.value
            response = invoke(request, connection, directory)
    finally:
        steps.close()


def _extract_profile(work, template_path, catalog, connection, invoke):
    from .records import Records
    return consume_profile_steps(profile_steps(work, template_path, catalog, connection,
        cached_result=Records(work).cached('profile')), invoke)


def _verify_reply_journal(model):
    """Require the historical byte bindings, not a hash of newly observed bytes."""
    from .profile_recovery import _checked_request, ProfileRecoveryError
    from preextract_request_control import verified_saved_state
    from preextract_scheduler_adapter import _event_response
    _checked_request(model, require_saved_hash=True)
    state = verified_saved_state(model)
    events = model / 'response-events.jsonl'
    response = model / 'response.txt'
    if (state.get('status') != 'response_complete' or not response.is_file()
            or sha_file(response) != state.get('response_sha256') or not events.is_file()
            or (state.get('events_sha256') and sha_file(events) != state['events_sha256'])
            or _event_response(model) != response.read_text(encoding='utf8')):
        raise ProfileRecoveryError('evidence_changed')


def _verify_cached_inputs(work, original):
    from .profile_recovery import selected_model, ProfileRecoveryError
    source = original['source']
    for path_key, hash_key in (('file','source_sha256'), ('text_file','text_sha256'), ('index_file','index_sha256')):
        if not source.get(path_key) or sha_file(source[path_key]) != source.get(hash_key):
            raise ProfileRecoveryError('evidence_changed')
    partition = work / 'model/partition-state.json'
    if partition.exists():
        from .pipeline_partition import _directory
        state = read_json(partition)
        if state.get('status') != 'assembled' or not state.get('requests'):
            raise ProfileRecoveryError('evidence_changed')
        for row in state['requests']:
            _verify_reply_journal(_directory(work/'model', state, row['id']))
    else:
        _verify_reply_journal(selected_model(work))


def profile_steps(work, template_path, catalog, connection, invoke=None, *, cached_result=None):
    from preextract_contract import load_template, validate_candidate
    from preextract_images import prepare_request_images, restore_request_images
    from preextract_request import plan_requests
    from .pipeline_partition import create_plan, run_partition_steps
    from preextract_image_alias import AliasContext
    from preextract_pipeline import validate_saved_request
    from .profile_recovery import selected_model, _read_complete, parse_response, inspect_request, ProfileRecoveryError, INTENT
    work = Path(work)
    original = read_json(work / 'material.json')
    aliases = AliasContext()
    model = selected_model(work)
    template, _ = load_template(template_path)
    if cached_result is not None:
        _verify_cached_inputs(work, original)
    if (work / 'model/partition-state.json').is_file():
        # Preserve the original guard against switching an unresolved root
        # request into the partition path.
        if (work / 'model/request-state.json').exists():
            observed = inspect_request(work)
            if observed['outcome'] != 'complete':
                raise ProfileRecoveryError(observed['code'], outcome=observed['outcome'])
        response, _, bundle = yield from run_partition_steps(original, work, template, catalog, connection, invoke, alias_context=aliases)
    elif (work / INTENT).exists() or any((model / name).exists() for name in
            ('request-state.json', 'request.json', 'response.txt', 'response-events.jsonl', 'http-error.txt')):
        model, response = _read_complete(work)
        actual = read_json(model / 'request.json')
        validate_saved_request(actual, original, template)
        bundle = restore_request_images(original, work, actual, alias_context=aliases, original_bundle=original)
    else:
        if cached_result is not None: raise ProfileRecoveryError('evidence_changed')
        adapted, _ = prepare_request_images(original, work, connection)
        plan = plan_requests(adapted, template, catalog, connection)
        if plan['mode'] == 'single':
            response = yield (plan['parts'][0]['body'], connection, work / 'model')
            actual = read_json(work / 'model/request.json')
            if identity(actual) != identity(plan['parts'][0]['body']):
                raise ValueError('实际请求与本次计划身份不一致')
            validate_saved_request(actual, original, template)
            bundle = restore_request_images(original, work, actual, alias_context=aliases, original_bundle=original)
        else:
            create_plan(plan, original, adapted, work, template, connection)
            response, _, bundle = yield from run_partition_steps(original, work, template, catalog, connection, invoke, alias_context=aliases)
    if cached_result is not None:
        from preextract_contract import parse_saved_candidate
        candidate = parse_saved_candidate(response)
        repairs, repair_record = [], None
    else:
        candidate, repairs, repair_record = parse_response(work, response, model=model)
    if candidate is None:
        raise ProfileRecoveryError('profile_format_error', diagnostic=repair_record['receipt'],
            **{k: repair_record['error'][k] for k in ('line', 'column', 'offset') if k in repair_record['error']})
    validation = validate_candidate(candidate, bundle, catalog, redaction_policy='best_effort',
                                    admission_policy='learning', alias_context=aliases)
    if cached_result is None: write_json(work / 'private/validation.json', validation)
    if validation.get('valid') is not True:
        raise ProfileRecoveryError('profile_validation_failed', diagnostic=str(work / 'private/validation.json'))
    if cached_result is not None:
        saved_candidate = read_json(cached_result['candidate'])
        saved_validation = read_json(cached_result['validation'])
        if (identity(bundle) != identity(read_json(cached_result['bundle']))
                or identity(validation['candidate']) != identity(saved_candidate)
                or saved_validation.get('valid') is not True
                or identity(saved_validation.get('candidate')) != identity(saved_candidate)):
            raise ProfileRecoveryError('evidence_changed')
        return cached_result
    write_json(work / 'private/candidate.json', validation['candidate'])
    write_json(work / 'private/validated-bundle.json', bundle)
    result = {'candidate': str(work / 'private/candidate.json'),
              'validation': str(work / 'private/validation.json'),
              'bundle': str(work / 'private/validated-bundle.json'), 'syntax_repairs': repairs}
    write_json(work / 'profile-result.json', result)
    return result


def package_body(work, runtime, catalog, paperflow_root):
    work = Path(work)
    # This stage has no connection or invoke argument and cannot fill missing model work.
    candidate = read_json(work / 'private/candidate.json')
    validation = read_json(work / 'private/validation.json')
    bundle = read_json(work / 'private/validated-bundle.json')
    from preextract_publish import prepare_publication
    from paperflow.package import validate_package
    cached = work / 'prepared-publication.json'
    if cached.is_file():
        prepared = read_json(cached)
        if prepared['candidate_sha256'] != sha_file(work / 'private/candidate.json'):
            raise ValueError('画像与原封包身份不符')
        packet = validate_package(prepared['package_path'], prepared['task'], catalog, strict_seal=True)
        if packet['manifest_sha256'] != prepared['validation']['manifest_sha256']:
            raise ValueError('已保存封包发生变化')
        return prepared
    # A crash during packaging cannot silently create a new publication identity.
    target = work / 'publication'
    if target.exists(): raise RuntimeError('已有未登记封包，请先核对；不会生成第二个交付身份')
    from preextract_pipeline import offline_counter
    counter = offline_counter(runtime, paperflow_root)
    try:
        prepared = prepare_publication(candidate, bundle, target, paperflow_root=paperflow_root,
            catalog=catalog, counter=counter, redaction_policy='best_effort',
            candidate_validation=validation, admission_policy='learning')
    finally: counter.close()
    prepared = {key: value for key, value in prepared.items() if key != 'packet'}
    prepared['candidate_sha256'] = sha_file(work / 'private/candidate.json')
    write_json(cached, prepared)
    return prepared
