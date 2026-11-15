import { createRequire } from 'node:module';
import { readFile } from 'node:fs/promises';
const require = createRequire(import.meta.url);
const core = require('../../packages/api/dist/exemplars.cjs');
const { exemplarDimensions, exemplarActions, exemplarAspects, exemplarAspectCriteria } = require('librechat-data-provider');
/** Stable sentence/short-paragraph IDs refer to author text, not generated annotations. */
export function evidenceCatalog(original) {
  const author = core.exemplarAuthorText(original), prefix = core.stableHash(original).slice(0, 12), spans = [];
  const pattern = /[^\r\n。！？.!?]+[。！？.!?]?|[。！？.!?]/gu;
  for (const match of author.matchAll(pattern)) {
    const raw = match[0], leading = raw.length - raw.trimStart().length;
    let start = match.index + leading, end = match.index + raw.trimEnd().length;
    if (start >= end) continue;
    // Bound table rows and punctuation-free paragraphs without changing any source character.
    while (start < end) {
      let stop = Math.min(end, start + 600);
      if (stop < end && /[\uD800-\uDBFF]/u.test(original[stop - 1])) stop--;
      spans.push({ id: `${prefix}:${start}`, start, end: stop, text: original.slice(start, stop) });
      start = stop;
    }
  }
  return spans;
}
export function locateQuote(original, quote) {
  const body = core.exemplarAuthorText(original);
  if (body.includes(quote)) return { quote, start: body.indexOf(quote), alignment: 'exact' };
  // PDF wrapping/zero-width separators only. Never repair punctuation, figures or words.
  const chars = [], positions = [];
  for (let i = 0; i < body.length; i++) if (!/[\s\u200b\ufeff]/u.test(body[i])) { chars.push(body[i]); positions.push(i); }
  const normalized = chars.join(''), wanted = quote.replace(/[\s\u200b\ufeff]/gu, '');
  const index = normalized.indexOf(wanted);
  if (wanted.length < 8 || index < 0 || normalized.indexOf(wanted, index + 1) >= 0) return null;
  const start = positions[index], end = positions[index + wanted.length - 1] + 1;
  return { quote: body.slice(start, end), start, alignment: 'layout_only' };
}
export async function evaluationInput(doc, original, pack) {
  return {
    instructions: await readFile(new URL('./evaluation-prompt.md', import.meta.url), 'utf8'),
    payload: { document: { id: doc.id, title: doc.name }, actions: exemplarActions, aspects: exemplarAspects, aspect_criteria: exemplarAspectCriteria,
      discipline: pack, needs: core.compileNeeds({ topic: doc.name, level: '本科' }, pack),
      source_version: core.stableHash(original), author_spans: evidenceCatalog(original).map(({ id, text }) => ({ id, text })) },
  };
}
/** A small model-neutral exchange format is expanded to the shared, validated contract. */
export function importEvaluation(report, doc, original, chunks, pack, evaluator) {
  const p = core.importExemplarCandidate(doc, pack, original);
  const review = core.exemplarReviewText(original), evidence = new Map();
  const catalog = evidenceCatalog(original);
  for (const e of report.evidence ?? []) {
    if (!e.key || evidence.has(e.key)) throw Error(`invalid_evidence:${e.key}`);
    let ranges;
    if (e.span_ids) {
      if (!Array.isArray(e.span_ids) || !e.span_ids.length || e.span_ids.length > 512 || new Set(e.span_ids).size !== e.span_ids.length) throw Error(`invalid_evidence_spans:${e.key}`);
      const selected = e.span_ids.map(id => catalog.find(s => s.id === id));
      if (selected.some(s => !s)) throw Error(`evidence_span_missing:${e.key}`);
      ranges = [];
      for (const span of selected.sort((a,b) => a.start - b.start)) {
        const prior = ranges.at(-1);
        if (prior && !original.slice(prior.end, span.start).trim() && span.end - prior.start <= 1800) prior.end = span.end;
        else ranges.push({ start: span.start, end: span.end });
      }
    } else {
      if (typeof e.quote !== 'string' || e.quote.length < 2 || e.quote.length > 1800) throw Error(`invalid_evidence:${e.key}`);
      const found = locateQuote(original, e.quote);
      if (!found) throw Error(`evidence_not_in_author_text:${e.key}`);
      ranges = [{ start: found.start, end: found.start + found.quote.length }];
    }
    const entries = ranges.map(({start,end}) => {
      const quote = original.slice(start,end);
      if (quote.length < 2 || quote.length > 1800 || !review.includes(quote)) throw Error(`evidence_not_in_author_text:${e.key}`);
      return { id: `e:${core.stableHash([p.source_version, start, quote]).slice(0, 20)}`, quote, start, end, section: e.section, chunk_ids: core.evidenceChunkIds(quote, chunks) };
    });
    evidence.set(e.key, entries);
  }
  const list = (keys, name) => { if (keys == null) return []; if (!Array.isArray(keys)) throw Error(`expected_array:${name}`); return keys; };
  const eids = keys => [...new Set(list(keys, 'evidence_keys').flatMap(k => { if (!evidence.has(k)) throw Error(`evidence_key_missing:${k}`); return evidence.get(k).map(e => e.id); }))];
  const unitIds = new Map((report.units ?? []).map(u => [u.key, `${doc.id}:${core.stableHash([p.source_version, u.key, u.action, u.evidence_keys]).slice(0, 20)}`]));
  if (unitIds.size !== report.units?.length) throw Error('duplicate_unit_key');
  const uids = keys => list(keys, 'unit_keys').map(k => { if (!unitIds.has(k)) throw Error(`unit_key_missing:${k}`); return unitIds.get(k); });
  // Identify the repairable field without inventing a target or dropping a claim.
  for (const [index, relation] of (report.relations ?? []).entries()) {
    for (const side of ['from', 'to']) {
      const keys = relation[side], location = `relations[${index}].${side}`;
      if (!Array.isArray(keys) || !keys.length) throw Error(`relation_scope_invalid:${location}:nonempty_existing_unit_keys_required`);
      for (const key of keys) if (!unitIds.has(key)) throw Error(`relation_scope_invalid:${location}:unknown_unit_key:${key}`);
    }
  }
  const assessment = a => {
    const dimensions = exemplarDimensions.map(d => {
      const v = a.grades?.[d];
      return { dimension: d, content: v?.grade ?? null, verification: 'pending', processing: 'succeeded', evidence_ids: eids(v?.evidence_keys), reason: v?.reason ?? '未评价' };
    });
    const absent = a.finding === 'absent_in_scope';
    return { aspect: a.aspect, finding: a.finding, content: absent ? null : dimensions.some(d => d.content === 'invalid') ? 'invalid' : dimensions.some(d => d.content === 'defective') ? 'defective' : dimensions.every(d => d.content === 'sufficient') ? 'sufficient' : null,
      verification: 'pending', processing: 'succeeded', decision: 'pending',
      evidence_ids: eids(a.evidence_keys), dimensions, allowed_scope: a.allowed_scope ?? '', limitations: a.limitations ?? [], reason: a.reason };
  };
  p.units = report.units.map(u => ({ id: unitIds.get(u.key), action: u.action, title: u.title, problem: u.problem,
    inputs: u.inputs, steps: u.steps, output: u.output, evidence_ids: eids(u.evidence_keys),
    core: { value: u.core, verification: 'pending', reason: u.core_reason },
    features: (u.features ?? []).map(f => ({ name: f.name, value: f.value, role: f.role, origin: f.origin ?? 'ai_inference', verification: 'pending', evidence_ids: eids(f.evidence_keys), method_basis_ids: [] })),
    assessments: u.assessments.map(assessment) }));
  p.evidence.push(...new Map([...evidence.values()].flat().map(e => [e.id, e])).values());
  p.applicable_levels = (report.applicable_levels ?? []).map(l => ({ value: l.value, origin: 'ai_assessment', verification: 'pending', scope: uids(l.unit_keys), evidence_ids: eids(l.evidence_keys), reason: l.reason }));
  p.relations = (report.relations ?? []).map((r, i) => ({ id: `${doc.id}:r${i}`, from: uids(r.from), to: uids(r.to), claim: r.claim, assessment: assessment(r.assessment) }));
  p.evaluation = { id: `eval:${core.stableHash([p.source_version, p.rubric_version, report, evaluator]).slice(0, 24)}`, evaluator, created_at: new Date().toISOString(), coverage: 'full_text', chars_read: review.length, chars_total: review.length };
  return core.validateProfile(p, original);
}
