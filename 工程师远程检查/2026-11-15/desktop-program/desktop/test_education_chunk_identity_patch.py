"""Exercise the deployed chunk-processing method and its installer offline."""
import ast
import copy
from datetime import datetime
import hashlib
import importlib
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pytest


EDUCATION = 'ecf88dc4b82b11f1a8846d29005ec004'
LOCAL_EDUCATION = '3e8ef88cbc4e11f1a8e165c0e34ea629'
# Exact deployed method, including keyword conversion after ID assignment.
DEPLOYED = '''import copy
class DataflowService:
    def _process_chunks(self, chunks: List[Dict]) -> Dict:
        """Process chunks for metadata and indexing."""
        ctx = self._task_context
        metadata = {}
        for ck in chunks:
            ck["doc_id"] = ctx.doc_id
            ck["kb_id"] = [str(ctx.kb_id)]
            ck["docnm_kwd"] = ctx.name
            ck["create_time"] = str(datetime.now()).replace("T", " ")[:19]
            ck["create_timestamp_flt"] = datetime.now().timestamp()
            text = ck.get("text") or ck.get("content_with_weight") or ck.get("summary") or ck.get("questions") or ""
            if not isinstance(text, str):
                text = str(text)

            if not ck.get("id"):
                ck["id"] = xxhash.xxh64((text + str(ck["doc_id"])).encode("utf-8")).hexdigest()

            if "questions" in ck:
                if "question_tks" not in ck:
                    ck["question_kwd"] = ck["questions"].split("\\n")
                    ck["question_tks"] = rag_tokenizer.tokenize(str(ck["questions"]))
                del ck["questions"]

            if "keywords" in ck:
                if "important_tks" not in ck:
                    ck["important_kwd"] = [k for k in re.split(r"[,，;；、\\r\\n]+", ck["keywords"]) if k.strip()]
                    ck["important_tks"] = rag_tokenizer.tokenize(str(ck["keywords"]))
                del ck["keywords"]

            if "summary" in ck:
                if "content_ltks" not in ck:
                    ck["content_ltks"] = rag_tokenizer.tokenize(str(ck["summary"]))
                    ck["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(ck["content_ltks"])
                del ck["summary"]

            if "metadata" in ck:
                metadata = update_metadata_to(metadata, ck["metadata"])
                del ck["metadata"]

            if "content_with_weight" not in ck:
                ck["content_with_weight"] = text
            ck.pop("text", None)

            if "positions" in ck:
                add_positions(ck, ck["positions"])
                del ck["positions"]

        return metadata
'''


def installer():
    return importlib.import_module('server_patches.education_chunk_identity.install')


def process(rows, dataset=EDUCATION, document='document-1', *, patched=True):
    source = installer().transform(DEPLOYED) if patched else DEPLOYED
    module = ast.parse(source)
    cls = next(n for n in module.body if isinstance(n, ast.ClassDef))
    functions = [n for n in module.body if isinstance(n, ast.FunctionDef)] + cls.body
    # The hash boundary is deterministic; server replay separately uses real xxhash.
    namespace = {'List': list, 'Dict': dict, 'datetime': datetime, 're': re, 'json': json,
                 'xxhash': SimpleNamespace(xxh64=hashlib.sha256),
                 'rag_tokenizer': SimpleNamespace(tokenize=lambda s: s, fine_grained_tokenize=lambda s: s),
                 'update_metadata_to': lambda old, new: {**old, **new},
                 'add_positions': lambda row, positions: row.update(position_int=positions)}
    exec(compile(ast.Module(body=functions, type_ignores=[]), '<deployed-process-chunks>', 'exec'), namespace)
    result = copy.deepcopy(rows)
    service = SimpleNamespace(_task_context=SimpleNamespace(kb_id=dataset, doc_id=document, name='synthetic.md'))
    metadata = namespace['_process_chunks'](service, result)
    return result, metadata


def stored(rows):
    """An ID-addressed store replaces earlier rows with the same ID."""
    return list({row['id']: row for row in rows}.values())


@pytest.mark.parametrize('dataset', [EDUCATION, LOCAL_EDUCATION])
@pytest.mark.parametrize('role', ['body', 'index'])
def test_equal_text_keeps_sections_parts_and_repeated_occurrences(role, dataset):
    rows = [
        {'text': 'same text', 'important_kwd': [f'role={role}', 'section_id=a', 'part=1/2']},
        {'text': 'same text', 'important_kwd': [f'role={role}', 'section_id=b', 'part=1/2']},
        {'text': 'same text', 'important_kwd': [f'role={role}', 'section_id=a', 'part=2/2']},
        {'text': 'same text', 'important_kwd': [f'role={role}', 'section_id=a', 'part=1/2']},
    ]
    processed, _ = process(rows, dataset)
    assert len(stored(processed)) == len(rows), 'The ID store lost a source occurrence'
    assert [r['content_with_weight'] for r in processed] == [r['text'] for r in rows]
    assert [r['important_kwd'] for r in processed] == [r['important_kwd'] for r in rows]
    again, _ = process(rows, dataset)
    assert [r['id'] for r in again] == [r['id'] for r in processed]


def test_repeated_body_survives_storage_and_strict_source_proof(tmp_path):
    from ingest_execution_diagnostics import execution_source_proof, _output
    from test_ingest_artifact_provenance import admitted_source
    task, remote, log, _chunks, _client = admitted_source(tmp_path)
    tokens = _output(log['dsl'], 'Tokenizer:EducationIndex', 'chunks')
    tokens[0]['important_kwd'] += ['section_id=a']
    tokens[0]['keywords'] = ','.join(tokens[0]['important_kwd'])
    repeated = copy.deepcopy(tokens[0])
    repeated['important_kwd'] = ['role=body', 'section_id=b']
    repeated['keywords'] = ','.join(repeated['important_kwd'])
    tokens.append(repeated)

    def fetched(patched):
        rows, _ = process(tokens, document=task['remote_id'], patched=patched)
        return [{'id': r['id'], 'document_id': r['doc_id'], 'content': r['content_with_weight'],
                 'important_keywords': r['important_kwd'], 'available': bool(r['available_int'])}
                for r in stored(rows)]

    assert execution_source_proof(task, remote, log, fetched(False)) is None
    assert execution_source_proof(task, remote, log, fetched(True)) is not None


@pytest.mark.parametrize('separator', [',', '，', ';', '；', '、', '\r', '\n'])
def test_keyword_string_matches_the_deployed_conversion(separator):
    rows = [{'text': 'same', 'keywords': separator.join(['role=body', f'section_id={section}'])}
            for section in ('a', 'b')]
    processed, _ = process(rows)
    assert len(stored(processed)) == 2
    assert [r['important_kwd'] for r in processed] == [
        ['role=body', 'section_id=a'], ['role=body', 'section_id=b']]


@pytest.mark.parametrize('has_important_tks', [False, True])
def test_keyword_precedence_matches_the_deployed_important_tks_guard(has_important_tks):
    rows = [{'text': 'same', 'keywords': 'role=assets',
             'important_kwd': ['role=body', f'section_id={section}']}
            for section in ('a', 'b')]
    if has_important_tks:
        for row in rows:
            row['important_tks'] = 'already-tokenized'
    processed, _ = process(rows)
    assert len(stored(processed)) == (2 if has_important_tks else 1)
    assert all(('role=body' if has_important_tks else 'role=assets') in r['important_kwd']
               for r in processed)


def without_clock(rows):
    return [{k: v for k, v in row.items() if k not in ('create_time', 'create_timestamp_flt')}
            for row in rows]


@pytest.mark.parametrize('dataset,words', [
    ('finance', ['role=body', 'section_id=a']),
    ('finance', ['role=index']),
    (EDUCATION, ['role=assets', 'part=1/2']),
    (EDUCATION, ['role=unknown']),
    (EDUCATION, []),
    (EDUCATION, ['role=body', 'role=assets']),
])
def test_other_datasets_and_roles_keep_every_original_field(dataset, words):
    row = {'text': 'same', 'important_kwd': words, 'metadata': {'x': 'y'}, 'available_int': 0,
           'questions': 'question one\nquestion two', 'positions': [1, 2]}
    actual, metadata = process([row, row], dataset)
    original, old_metadata = process([row, row], dataset, patched=False)
    assert without_clock(actual) == without_clock(original)
    assert metadata == old_metadata == {'x': 'y'}


@pytest.mark.parametrize('role', ['body', 'index'])
def test_native_id_is_preserved(role):
    row = {'id': 'native-source-id', 'text': 'same', 'important_kwd': [f'role={role}']}
    result, _ = process([row, row])
    original, _ = process([row, row], patched=False)
    assert without_clock(result) == without_clock(original)


def test_identity_is_stable_across_unrelated_rows_and_keyword_order():
    body = {'text': 'same', 'important_kwd': ['role=body', 'section_id=a', 'part=1/2']}
    index = {'text': 'same', 'important_kwd': ['role=index']}
    baseline, _ = process([body, index])
    reordered = copy.deepcopy(body)
    reordered['important_kwd'].reverse()
    after, _ = process([{'text': 'other', 'important_kwd': ['role=assets']}, index, reordered])
    assert [r['id'] for r in baseline] == [after[2]['id'], after[1]['id']]
    assert baseline[0]['id'] != baseline[1]['id']
    other_doc, _ = process([body], document='different-document')
    assert other_doc[0]['id'] != baseline[0]['id']


@pytest.fixture
def gated_fixture(monkeypatch):
    candidate = installer()
    before = DEPLOYED.encode('utf-8')
    after = candidate.transform(DEPLOYED).encode('utf-8')
    # Exercise installer I/O against a public synthetic source, without shipping
    # the full private deployment snapshot. Actual release hashes are replayed separately.
    monkeypatch.setattr(candidate, 'ORIGINAL', hashlib.sha256(before).hexdigest())
    monkeypatch.setattr(candidate, 'PATCHED', hashlib.sha256(after).hexdigest())
    return candidate, before, after


def test_install_verifies_source_keeps_backup_and_is_idempotent(tmp_path, gated_fixture):
    candidate, before, after = gated_fixture
    target = tmp_path / 'dataflow_service.py'
    target.write_bytes(before)
    assert candidate.install(target) == 'installed'
    assert target.read_bytes() == after
    backup = target.with_name(target.name + '.education-identity-original')
    assert backup.read_bytes() == before
    assert candidate.install(target) == 'already_installed'
    assert backup.read_bytes() == before
    assert len(list(tmp_path.iterdir())) == 2


@pytest.mark.parametrize('problem', ['changed_source', 'changed_backup', 'generated_hash'])
def test_failed_gate_leaves_target_and_backup_untouched(tmp_path, gated_fixture, monkeypatch, problem):
    candidate, before, _after = gated_fixture
    target = tmp_path / 'dataflow_service.py'
    backup = target.with_name(target.name + '.education-identity-original')
    if problem == 'changed_source':
        before += b'\n# unrelated runtime modification\n'
    if problem == 'changed_backup':
        backup.write_bytes(b'different backup')
    if problem == 'generated_hash':
        monkeypatch.setattr(candidate, 'PATCHED', '0' * 64)
    target.write_bytes(before)
    snapshot = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with pytest.raises(ValueError):
        candidate.install(target)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == snapshot
