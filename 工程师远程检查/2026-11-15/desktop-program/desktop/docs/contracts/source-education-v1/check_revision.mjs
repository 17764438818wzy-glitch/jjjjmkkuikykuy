// Offline Python/JavaScript digest agreement for the same five contract fixtures.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import assert from 'node:assert/strict';

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  }
  if (typeof value === 'number') assert(Number.isSafeInteger(value), 'unsafe JSON number');
  return value;
}

const cases = JSON.parse(readFileSync(new URL('./examples.json', import.meta.url), 'utf8'));
for (const test of cases) {
  const bytes = JSON.stringify(canonical(test.metadata.source_education));
  const digest = createHash('sha256').update(bytes, 'utf8').digest('hex');
  assert.equal(digest, test.metadata.source_education_revision, test.case);
}
assert.throws(() => canonical({ page: 1.5 }));
assert.throws(() => canonical({ page: 9007199254740992 }));
process.stdout.write(JSON.stringify({ cross_language_revisions_matched: cases.length, RAG_calls: 0 }) + '\n');
