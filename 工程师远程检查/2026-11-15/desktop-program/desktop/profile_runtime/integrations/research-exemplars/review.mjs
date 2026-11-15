import fs from 'node:fs/promises';
import { createRequire } from 'node:module';
import { evidenceCatalog } from './evaluation.mjs';
const require = createRequire(import.meta.url), core = require('../../packages/api/dist/exemplars.cjs');
const { exemplarReviewResultSchema, exemplarAspectCriteria } = require('librechat-data-provider');

/** invoke must create an independent call; no generation messages or reasoning are supplied. */
export async function reviewProfile({ profile, original, invoke, reviewer, refresh = false, record = async () => {} }) {
  const p = core.validateProfile(profile, original), input = core.profileReviewInput(p), budget = core.profileReviewBudget;
  if (!refresh && p.semantic_review?.input_hash === input.input_hash && p.semantic_review.version === core.profileReviewVersion) {
    core.validateProfileReadScopes(p, original, p.semantic_review.read_scopes);
    return p;
  }
  const prepared = await prepareProfileReview(p, original);
  const { instructions, payload } = prepared, scopes = [payload.read_scope];
  const catalog = evidenceCatalog(original), author = core.exemplarAuthorText(original);
  const sourceRanges = ranges => ranges.map(r => ({ ...r, text: author.slice(r.start, r.end) }));
  const initial = await invoke({ stage: 'review', instructions, payload });
  const result = exemplarReviewResultSchema.parse(initial);
  if (result.checks.length !== input.targets.length || new Set(result.checks.map(c => c.target)).size !== result.checks.length || result.checks.some(c => !input.targets.some(t => t.id === c.target))) throw Error('review_scope_mismatch');
  await record({ stage: 'review', input_hash: input.input_hash, response: initial, read_scope: scopes[0] });
  if (!Array.isArray(initial.read_requests ?? [])) throw Error('review_read_requests_invalid');
  const granted = [], requested = new Set(); let used = 0;
  for (const request of initial.read_requests ?? []) {
    if (requested.has(request.target)) continue;
    requested.add(request.target);
    const check = result.checks.find(c => c.target === request.target);
    if (!check || check.status !== 'insufficient' || !Number.isInteger(request.start) || !Number.isInteger(request.end) || request.start < 0 || request.end > author.length || request.start >= request.end) continue;
    const chosen = catalog.filter(s => s.start >= request.start && s.end <= request.end && s.end - request.start <= budget.supplement_chars_per_claim);
    if (!chosen.length) continue;
    const range = { start: chosen[0].start, end: chosen.at(-1).end }, size = range.end - range.start;
    if (used + size > budget.supplement_chars_total) continue;
    const scope = { targets: [request.target], supplemental: true, ranges: [range] };
    granted.push(scope); used += size;
  }
  if (granted.length) {
    const ids = granted.flatMap(s => s.targets);
    const combined = { targets: ids, supplemental: true, ranges: granted.flatMap(s => s.ranges) };
    scopes.push(combined);
    // A fresh second call carries actual previous source context, not the first review's opinion.
    const supplement = await invoke({ stage: 'review_supplement', instructions,
      payload: { ...payload, targets: input.targets.filter(t => ids.includes(t.id)), read_scope: scopes,
        source_ranges: sourceRanges(scopes.flatMap(s => s.ranges)), budget: { ...budget, supplement_reads_per_claim: 0 } } });
    const extra = exemplarReviewResultSchema.parse(supplement);
    if (extra.checks.length !== ids.length || new Set(extra.checks.map(c => c.target)).size !== ids.length || extra.checks.some(c => !ids.includes(c.target))) throw Error('review_scope_mismatch');
    for (const check of extra.checks) result.checks[result.checks.findIndex(c => c.target === check.target)] = check;
    await record({ stage: 'review_supplement', input_hash: input.input_hash, response: supplement, read_scopes: [combined] });
    // Further requests are explicitly exhausted; no unbounded read loop.
  }
  return core.applyProfileReview(p, original, input.input_hash, result, reviewer, scopes);
}

/** Model-neutral fresh-context input for offline reviewers, with host-derived read scope. */
export async function prepareProfileReview(profile, original) {
  const input = core.profileReviewInput(profile), budget = core.profileReviewBudget;
  const catalog = evidenceCatalog(original), author = core.exemplarAuthorText(original);
  const end = author.length <= budget.initial_chars ? author.length : (catalog.filter(s => s.end <= budget.initial_chars).at(-1)?.end ?? 0);
  if (!end || !input.targets.length) throw Error('review_source_or_targets_empty');
  const scopes = [{ targets: input.targets.map(t => t.id), supplemental: false, ranges: [{ start: 0, end }] }];
  const instructions = await fs.readFile(new URL('./review-prompt.md', import.meta.url), 'utf8');
  const index = catalog.map(s => ({ id: s.id, start: s.start, end: s.end }));
  const sourceRanges = ranges => ranges.map(r => ({ ...r, text: author.slice(r.start, r.end) }));
  const { targets, evidence, ...identity } = input;
  const payload = { ...identity, budget, source_index: index, full_author_length: author.length, read_scope: scopes[0],
    source_ranges: sourceRanges(scopes[0].ranges), evidence: evidence.map(({ quote, ...location }) => location),
    targets: targets.map(t => ({ ...t, criterion: t.claim?.assessment?.aspect ? exemplarAspectCriteria[t.claim.assessment.aspect] : t.kind === 'core' ? '仅判断core.value所宣称的核心/非核心身份：是否承载主要问题或结论，去除后是否失去必要支撑。步骤正误由对应用途另核，不得把非核心false读成声称核心true。' : undefined })) };
  return { instructions, payload };
}
