# Education chunk identity repair

This patch targets the verified deployed file:

`/ragflow/rag/svr/task_executor_refactor/dataflow_service.py`

Only default IDs in the original education dataset
`ecf88dc4b82b11f1a8846d29005ec004` or local BGE-M3 dataset
`3e8ef88cbc4e11f1a8e165c0e34ea629`, with exactly one
`role=body` or `role=index` keyword, change. The ID input includes the document,
role, section markers, part markers, unchanged text, and an occurrence number
within that same source identity. The occurrence counter is local to one complete
`_process_chunks` call, so retries of the same ordered output produce the same IDs.

Existing nonempty IDs are preserved. Assets, other roles, and other datasets keep
the original `xxh64(text + doc_id)` behavior. Keywords use the deployed precedence:
the `important_kwd` list, unless `keywords` is present without `important_tks`, in
which case the existing delimiter conversion supplies the list. No content,
metadata, availability, embedding, or source-proof rules change.

## Hash gate and installation

- Original SHA-256: `ddc5870fb9723a9199f06b4650a887074e4b4615edb87d7f2b50e94b07fa14d3`
- Current patched SHA-256: `309ea86ae4a2f7932f3f8ebd34f70fa0bd7c08b17b7e3e7613da4b3b9e7e74c7`
- Previous patched SHA-256: `72eaac36c04be76cff18515fd8c3cc243db11d07ebb71097a1c636534e9e3dc5`

After other runtime installers, the deployment operator can run:

```sh
python install.py --target /ragflow/rag/svr/task_executor_refactor/dataflow_service.py
```

The installer rejects unknown source bytes or conflicting backups, compiles the
candidate, preserves a `.education-identity-original` backup, and replaces the
file atomically. A second invocation returns `already_installed`. It performs no
remote requests, process changes, or data migration.

All default body/index IDs in the target dataset use the new identity scheme on
their next execution, including unique text. Applying the source patch alone
does not repair previously stored chunks. Any subsequent document rebuild must
use the normal cleanup path so old IDs do not remain alongside the new ones;
strict source proof continues to reject missing or extra rows.

## Verification

`test_education_chunk_identity_patch.py` executes the deployed processing method
with an ID-addressed store boundary. It covers repeated content across sections,
parts and identical source occurrences, unchanged native IDs/assets/finance,
keyword formats and precedence, deterministic IDs, strict source proof, and
installer rejection/idempotency. The test hash boundary uses standard-library
SHA-256; the separate local deployment replay used real `xxhash`.

Read-only task 892 replay with the verified full deployed file and real xxhash:
135 Tokenizer rows became 134 stored rows before the patch and 135 after it.
Strict proof changed from rejection to success. All 49 asset IDs/texts were
unchanged; the two affected sections received distinct body IDs. Only a temporary
source copy was installed during this replay. No online deployment was performed.
