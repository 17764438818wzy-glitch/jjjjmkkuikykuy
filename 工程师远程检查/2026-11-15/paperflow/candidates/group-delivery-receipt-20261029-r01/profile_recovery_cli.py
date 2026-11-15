"""Explicit one-workspace engineering recovery. No batch, RAG or NAS entry."""
import argparse
import json
from pathlib import Path
import uuid

from filelock import FileLock, Timeout
from paperflow_prefect import legacy
from paperflow_prefect.settings import Settings
from paperflow_prefect.records import Records, identity, read_json, sha_file, write_json
from paperflow_prefect.profile_recovery import (ProfileRecoveryError, inspect_request,
    repair_saved_profile, retry_saved_request, recovery_lock, safe_error)


def bound_work(settings, work, receipt_path=None):
    work = Path(work).resolve()
    try:
        parts = work.relative_to(settings.path('data_root')).parts
        if len(parts) != 4 or parts[0] != 'papers' or parts[2] != 'body':
            raise ProfileRecoveryError('unsupported_recovery')
        material = read_json(work / 'material.json')
        if receipt_path is not None:
            from paperflow_prefect.stage3_handoff import read_receipt
            from paperflow_prefect.request_admission import _binding_path, _work_identity
            from paperflow_prefect.profile_recovery import require_delivery_sources
            receipt, paper = read_receipt(receipt_path)
            if not receipt.get('delivery_ref') or not paper or not paper.get('body'):
                raise ProfileRecoveryError('evidence_changed')
            body = paper['body']
            binding = read_json(_binding_path(settings.path('data_root'), paper, body, 'body'))
            if (paper['paper_id'] != parts[1] or binding.get('schema') != 'source-work-binding/1'
                    or binding.get('identity') != _work_identity(paper, body, 'body')
                    or Path(binding['work_dir']).resolve() != work
                    or material['source']['source_sha256'] != body['sha256']):
                raise ProfileRecoveryError('evidence_changed')
            require_delivery_sources(work, body)
            key = identity(str(Path(paper['person_dir']).absolute()).casefold())
            return work, paper, settings.path('data_root') / 'locks' / (key + '.lock')
        matches = []
        for path in (work.parent.parent / 'revisions').glob('*/*/inventory.json'):
            paper = read_json(path)
            body = paper['body']
            key = identity([body['sha256'], body['relative'], path.parent.name])[:24]
            if (paper['paper_id'] == parts[1] and key == work.name
                    and body['sha256'] == material['source']['source_sha256']
                    and Path(body['path']).resolve() == Path(material['source']['file']).resolve()):
                matches.append(paper)
        if not matches or len({str(Path(p['person_dir']).resolve()) for p in matches}) != 1:
            raise ProfileRecoveryError('evidence_changed')
        paper = matches[0]
        if sha_file(paper['body']['path']) != paper['body']['sha256']:
            raise ProfileRecoveryError('evidence_changed')
        key = identity(str(Path(paper['person_dir']).resolve()).casefold())
        return work, paper, settings.path('data_root') / 'locks' / (key + '.lock')
    except ProfileRecoveryError: raise
    except (OSError, ValueError, KeyError, TypeError):
        raise ProfileRecoveryError('evidence_changed') from None


def run_action(settings, work, action, receipt_path=None):
    work, paper, lock = bound_work(settings, work, receipt_path)
    legacy.bind(settings)
    lock.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(lock, timeout=0):
        if action == 'inspect': return inspect_request(work)
        if action == 'diagnose':
            with recovery_lock(work):
                observed = inspect_request(work)
                path = work / 'private/profile-recovery' / ('observation-' + uuid.uuid4().hex + '.json')
                write_json(path, observed)
                if Records(work).status('profile')['state'] == 'failed' and observed['outcome'] != 'complete':
                    Records(work).failed('profile', ProfileRecoveryError(observed['code'],
                        outcome=observed['outcome'], diagnostic=str(path)))
                return dict(observed, diagnostic=str(path))
        if action == 'repair':
            report = repair_saved_profile(work)
            if report['status'] == 'format_error' and Records(work).status('profile')['state'] == 'failed':
                details = {k: report['error'][k] for k in ('line', 'column', 'offset') if k in report['error']}
                Records(work).failed('profile', ProfileRecoveryError('profile_format_error',
                    diagnostic=report['receipt'], **details))
            return report
        if action != 'retry': raise ValueError('Unknown recovery action')
        settings.require_execution()
        observed = inspect_request(work)
        if observed['outcome'] == 'complete': return observed
        if not observed['retry_allowed']:
            raise ProfileRecoveryError(observed['code'], outcome=observed['outcome'])
        # Only this explicit eligible retry branch can load a credential or dispatch.
        connection = legacy.connection(settings)
        from paperflow_prefect.flows import invoke_model
        source = paper['body']['path']
        if paper['body'].get('delivery_ref') is not None:
            from paperflow_prefect.profile_recovery import require_delivery_sources
            require_delivery_sources(work, paper['body'])
            source = read_json(work / 'material.json')['source']['file']
        result = retry_saved_request(work, source, settings.path('template_path'),
            connection, lambda body, cfg, directory: invoke_model('body', body, cfg, directory))
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'diagnose', 'repair', 'retry'))
    parser.add_argument('--work', required=True, help='Existing exact data/papers/PF_*/body/* directory')
    parser.add_argument('--config', type=Path)
    parser.add_argument('--receipt', type=Path, help='交付目录接收回执；用于绑定并重验副本')
    args = parser.parse_args(argv)
    try:
        result = run_action(Settings.load(args.config) if args.config else Settings.load(), args.work, args.action, args.receipt)
        # Diagnostics on disk may contain original values. Console output is allowlisted.
        summary = {key: result[key] for key in ('status', 'outcome', 'code', 'message', 'retry_allowed',
                   'error', 'receipt', 'diagnostic', 'model_requests_added') if key in result}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if result.get('outcome') == 'complete' or result.get('status') == 'parsed' else 2
    except (ProfileRecoveryError, Timeout) as exc:
        details = safe_error(ProfileRecoveryError('busy') if isinstance(exc, Timeout) else exc)
        print(json.dumps({'status': 'blocked', 'error': details}, ensure_ascii=False, indent=2))
        return 2


if __name__ == '__main__': raise SystemExit(main())
