Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
let librechat_data_provider = require("librechat-data-provider");
let node_crypto = require("node:crypto");
//#region src/research/exemplars/profileReview.ts
const profileReviewVersion = "1.0.1";
const profileReviewBudget = Object.freeze({
	initial_chars: 64e3,
	supplement_chars_per_claim: 6e3,
	supplement_chars_total: 6e3,
	supplement_reads_per_claim: 1
});
const claim = (a) => ({
	aspect: a.aspect,
	finding: a.finding,
	content: a.content,
	dimensions: a.dimensions.map((d) => ({
		dimension: d.dimension,
		content: d.content,
		evidence_ids: d.evidence_ids
	})),
	allowed_scope: a.allowed_scope,
	limitations: a.limitations,
	evidence_ids: a.evidence_ids
});
/** No generator rationale or chain of thought is sent to the reviewer. */
function profileReviewInput(p) {
	const targets = [];
	for (const u of p.units) {
		targets.push({
			id: `${u.id}/core`,
			kind: "core",
			claim: {
				unit_id: u.id,
				value: u.core.value
			}
		});
		u.assessments.forEach((a) => targets.push({
			id: `${u.id}/${a.aspect}`,
			kind: "use",
			claim: {
				unit_id: u.id,
				assessment: claim(a)
			}
		}));
		u.features.forEach((f, i) => targets.push({
			id: `${u.id}/feature/${i}`,
			kind: "feature",
			claim: {
				unit_id: u.id,
				name: f.name,
				value: f.value,
				role: f.role,
				origin: f.origin,
				evidence_ids: f.evidence_ids,
				method_basis_ids: f.method_basis_ids
			}
		}));
	}
	p.applicable_levels.forEach((l, i) => targets.push({
		id: `level/${i}`,
		kind: "level",
		claim: {
			value: l.value,
			scope: l.scope,
			evidence_ids: l.evidence_ids
		}
	}));
	p.relations.forEach((r) => targets.push({
		id: r.id,
		kind: "relation",
		claim: {
			from: r.from,
			to: r.to,
			claim: r.claim,
			assessment: claim(r.assessment)
		}
	}));
	const context = {
		version: profileReviewVersion,
		source_version: p.source_version,
		rubric_version: p.rubric_version,
		taxonomy_version: p.taxonomy_version,
		pack_id: p.pack_id,
		pack_version: p.pack_version,
		document_id: p.document_id,
		dataset_id: p.dataset_id,
		evidence: p.evidence,
		method_bases: p.method_bases,
		units: p.units.map((u) => ({
			id: u.id,
			action: u.action,
			title: u.title,
			problem: u.problem,
			inputs: u.inputs,
			steps: u.steps,
			output: u.output,
			evidence_ids: u.evidence_ids
		})),
		targets
	};
	return {
		...context,
		input_hash: stableHash(context)
	};
}
function union(ranges) {
	const out = [];
	for (const r of [...ranges].sort((a, b) => a.start - b.start)) if (out.length && r.start <= out[out.length - 1].end) out[out.length - 1].end = Math.max(out[out.length - 1].end, r.end);
	else out.push({ ...r });
	return out;
}
const length = (ranges) => union(ranges).reduce((sum, r) => sum + r.end - r.start, 0);
function validateProfileReadScopes(p, original, input) {
	if (!Array.isArray(input) || !input.length || input.length > 241) throw Error("review_read_scope_required");
	const scopes = input.map((s) => librechat_data_provider.exemplarReadScopeSchema.parse(s));
	const ids = profileReviewInput(p).targets.map((t) => t.id), author = exemplarAuthorText(original);
	for (const s of scopes) if (!s.targets.length || new Set(s.targets).size !== s.targets.length || s.targets.some((id) => !ids.includes(id)) || !s.ranges.length || s.ranges.some((r) => r.start >= r.end || r.end > author.length || !author.slice(r.start, r.end).trim())) throw Error("review_read_scope_invalid");
	if (scopes.filter((s) => !s.supplemental).length !== 1 || length(scopes.filter((s) => !s.supplemental).flatMap((s) => s.ranges)) > profileReviewBudget.initial_chars || length(scopes.filter((s) => s.supplemental).flatMap((s) => s.ranges)) > profileReviewBudget.supplement_chars_total) throw Error("review_read_budget_exceeded");
	for (const id of ids) {
		const reads = scopes.filter((s) => s.targets.includes(id));
		if (!reads.some((s) => !s.supplemental)) throw Error("review_read_scope_missing_target");
		const extra = reads.filter((s) => s.supplemental);
		if (extra.length > profileReviewBudget.supplement_reads_per_claim || length(extra.flatMap((s) => s.ranges)) > profileReviewBudget.supplement_chars_per_claim) throw Error("review_read_budget_exceeded");
	}
	return scopes;
}
function fullRead(original, scopes, id) {
	const author = exemplarAuthorText(original), ranges = union(scopes.filter((s) => s.targets.includes(id)).flatMap((s) => s.ranges));
	let cursor = 0;
	for (const r of ranges) {
		if (author.slice(cursor, r.start).trim()) return false;
		cursor = r.end;
	}
	return !author.slice(cursor).trim();
}
/** Only host-produced receipts matching the immutable claim/source versions can confirm grades. */
function enforceProfileReview(p, original) {
	const input = profileReviewInput(p), r = p.semantic_review;
	let valid = r?.version === "1.0.1" && r.input_hash === input.input_hash && !!r.reviewer;
	let scopes = [];
	if (valid) try {
		scopes = validateProfileReadScopes(p, original, r.read_scopes);
	} catch {
		valid = false;
	}
	if (r) r.coverage = valid && input.targets.every((t) => fullRead(original, scopes, t.id)) ? "full_text" : "partial";
	const supported = (target) => valid && r.checks.filter((c) => c.target === target).length === 1 && r.checks.some((c) => c.target === target && c.status === "supported" && c.problem === "none");
	const assess = (a, key) => {
		const ok = supported(key) && (a.finding !== "absent_in_scope" || fullRead(original, scopes, key));
		a.verification = ok ? "verified" : "pending";
		a.dimensions.forEach((d) => {
			d.verification = ok && d.content !== null ? "verified" : "pending";
		});
		if (!ok) a.decision = "pending";
		else if (a.finding === "absent_in_scope") a.decision = null;
	};
	p.units.forEach((u) => {
		u.core.verification = supported(`${u.id}/core`) && u.core.value !== null ? "verified" : "pending";
		u.assessments.forEach((a) => assess(a, `${u.id}/${a.aspect}`));
		u.features.forEach((f, i) => {
			f.verification = supported(`${u.id}/feature/${i}`) ? "verified" : "pending";
		});
	});
	p.applicable_levels.forEach((l, i) => {
		l.verification = supported(`level/${i}`) ? "verified" : "pending";
	});
	p.relations.forEach((r) => assess(r.assessment, r.id));
}
/** Never accepts verification flags or receipts from the generator's report. */
function applyProfileReview(p, original, inputHash, result, reviewer, readScopes) {
	if (p.rubric_version !== "1.1.1") throw Error("review_rubric_mismatch");
	const input = profileReviewInput(p), parsed = librechat_data_provider.exemplarReviewResultSchema.parse(result);
	if (inputHash !== input.input_hash) throw Error("review_input_changed");
	if (!reviewer.trim() || parsed.checks.length !== input.targets.length || new Set(parsed.checks.map((c) => c.target)).size !== parsed.checks.length || parsed.checks.some((c) => !input.targets.some((t) => t.id === c.target) || c.status === "supported" !== (c.problem === "none"))) throw Error("review_scope_mismatch");
	const read_scopes = validateProfileReadScopes(p, original, readScopes);
	const next = structuredClone(p);
	next.semantic_review = {
		...parsed,
		input_hash: inputHash,
		reviewer,
		version: profileReviewVersion,
		created_at: (/* @__PURE__ */ new Date()).toISOString(),
		read_scopes
	};
	return validateProfile(next, original);
}
//#endregion
//#region src/research/exemplars/sourceEducation.ts
const sourceEducationKeys = [
	"source_education",
	"source_education_revision",
	"source_education_verification",
	"source_education_verified_revision",
	"source_education_verification_reason"
];
const labels = {
	education_level: {
		associate: ["专科", "大专"],
		bachelor: ["本科", "大学本科"],
		master: ["硕士", "硕士研究生"],
		doctor: ["博士", "博士研究生"]
	},
	study_mode: {
		correspondence: ["函授"],
		adult_education: [
			"成人",
			"成人教育",
			"成人高等教育"
		],
		full_time: ["全日制"],
		part_time: ["非全日制"],
		amateur: ["业余"],
		off_job: ["脱产"],
		self_study: ["自考", "自学考试"],
		open_education: ["开放教育"],
		online_education: ["网络教育", "远程教育"]
	}
};
function canonical$1(v) {
	if (typeof v === "number" && !Number.isSafeInteger(v)) throw Error("source_education_unsafe_number");
	if (typeof v === "string" && /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(v)) throw Error("source_education_invalid_unicode");
	if (Array.isArray(v)) return v.map(canonical$1);
	if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([k, x]) => [canonical$1(k), canonical$1(x)]));
	if (v === void 0 || typeof v === "function" || typeof v === "bigint") throw Error("source_education_invalid_json");
	return v;
}
function sourceEducationRevision(value) {
	return (0, node_crypto.createHash)("sha256").update(JSON.stringify(canonical$1(value)), "utf8").digest("hex");
}
const sameSet = (a, b) => new Set(a).size === new Set(b).size && a.every((x) => b.includes(x));
/** Structural and semantic contract checks. These do not establish document identity or author role. */
function validateSourceEducation(input, revision) {
	if (sourceEducationRevision(input) !== revision) throw Error("source_education_revision_mismatch");
	const p = librechat_data_provider.sourceEducationSchema.parse(input);
	if (p.source_relation === "original" ? p.original_source_sha256 !== p.evidence_source_sha256 || p.derivation !== null : !p.derivation || p.derivation.input_sha256 !== p.original_source_sha256 || p.derivation.output_sha256 !== p.evidence_source_sha256) throw Error("source_education_derivation_mismatch");
	const ev = new Map(p.evidence.map((e) => [e.id, e]));
	if (ev.size !== p.evidence.length) throw Error("source_education_duplicate_evidence");
	for (const e of ev.values()) {
		const kind = {
			pdf: "pdf_page",
			docx: "docx_paragraph",
			txt: "text_lines",
			md: "text_lines"
		}[p.evidence_source_format];
		if (e.locator.kind !== kind || e.locator.kind === "text_lines" && e.locator.end_line < e.locator.start_line) throw Error("source_education_locator_mismatch");
	}
	const used = [];
	for (const name of ["education_level", "study_mode"]) {
		const f = p[name];
		const references = (c) => {
			if (c.value === "unknown" || !c.raw_labels.length || !c.evidence_ids.length) throw Error("source_education_evidence_required");
			const quotes = c.evidence_ids.map((id) => {
				const e = ev.get(id);
				if (!e || !e.supports.includes(name)) throw Error("source_education_invalid_reference");
				return e.quote;
			});
			const normalized = c.raw_labels.map((l) => l.normalize("NFKC").replace(/\s+/gu, ""));
			if (c.raw_labels.some((l) => !quotes.some((q) => q.includes(l)))) throw Error("source_education_label_not_quoted");
			if (c.value === "other" && name === "study_mode" ? normalized.some((l) => Object.values(labels[name]).flat().includes(l)) : normalized.some((l) => !labels[name][c.value]?.includes(l))) throw Error("source_education_label_mismatch");
		};
		if (f.status === "source_explicit") {
			references(f);
			if (f.candidates.length) throw Error("source_education_explicit_candidates");
		} else if (f.status === "unknown") {
			if (f.value !== "unknown" || f.raw_labels.length || f.evidence_ids.length || f.candidates.length) throw Error("source_education_unknown_has_value");
		} else {
			if (f.value !== "unknown" || f.candidates.length < 2 || new Set(f.candidates.map((c) => c.value)).size !== f.candidates.length) throw Error("source_education_invalid_conflict");
			f.candidates.forEach(references);
			if (!sameSet(f.evidence_ids, f.candidates.flatMap((c) => c.evidence_ids)) || !sameSet(f.raw_labels, f.candidates.flatMap((c) => c.raw_labels))) throw Error("source_education_conflict_lost_evidence");
		}
		used.push(...f.evidence_ids);
	}
	if (!sameSet(used, [...ev.keys()])) throw Error("source_education_unreferenced_evidence");
	const m = p.study_mode;
	const expected = {
		value: "unknown",
		status: m.status === "conflict" ? "conflict" : "unknown",
		basis: "none",
		rule_id: null,
		rule_version: "adult-to-correspondence/1.0",
		evidence_ids: m.status === "conflict" ? m.evidence_ids : []
	};
	if (m.status === "source_explicit" && ["adult_education", "correspondence"].includes(m.value)) Object.assign(expected, {
		value: "correspondence",
		status: "resolved",
		basis: m.value === "correspondence" ? "source_correspondence" : "business_rule",
		rule_id: m.value === "correspondence" ? "explicit_correspondence" : "adult_education_to_correspondence",
		evidence_ids: m.evidence_ids
	});
	if (sourceEducationRevision(expected) !== sourceEducationRevision(p.business_classification)) throw Error("source_education_business_rule_mismatch");
	return p;
}
const decode = (v) => {
	if (typeof v !== "string") return v;
	try {
		return JSON.parse(v);
	} catch {
		return v;
	}
};
/** Only receiver-owned metadata can carry a verified state. Mismatched revisions fail closed. */
function readSourceEducation(m) {
	if (m.source_education === void 0) return {};
	const source = decode(m.source_education), revision = typeof m.source_education_revision === "string" ? m.source_education_revision : "";
	const out = {
		source_education: source,
		source_education_revision: revision,
		source_education_verification: "pending",
		source_education_verified_revision: "",
		source_education_verification_reason: "等待接收方核对原文件、论文身份和原句位置"
	};
	try {
		const p = validateSourceEducation(source, revision);
		if (m.source_education_verification === "conflict" || p.education_level.status === "conflict" || p.study_mode.status === "conflict") out.source_education_verification = "conflict";
		else if (m.source_education_verification === "verified" && m.source_education_verified_revision === revision) {
			out.source_education_verification = "verified";
			out.source_education_verified_revision = revision;
		}
		if (typeof m.source_education_verification_reason === "string") out.source_education_verification_reason = m.source_education_verification_reason;
	} catch (e) {
		out.source_education_verification = "unsupported";
		out.source_education_verification_reason = `来源载荷未通过当前契约：${e instanceof Error ? e.message.slice(0, 250) : "invalid"}`;
	}
	return out;
}
function sourceEducationProjection(p) {
	const envelope = readSourceEducation(p);
	if (envelope.source_education === void 0) return {
		education_level: p.education.value,
		education_basis: p.evidence.filter((e) => p.education.evidence_ids.includes(e.id))
	};
	const source = envelope.source_education;
	const known = envelope.source_education_verification === "verified" && source.education_level.status === "source_explicit";
	return {
		...envelope,
		education_level: known ? source.education_level.value : "unknown",
		education_basis: known ? source.evidence.filter((e) => source.education_level.evidence_ids.includes(e.id)) : []
	};
}
/** Quality claims/evidence/receipt remain byte-for-byte unchanged; only source identity is projected. */
function withSourceEducation(profile, metadata) {
	const next = structuredClone(profile);
	for (const key of sourceEducationKeys) delete next[key];
	Object.assign(next, readSourceEducation(metadata));
	if (next.source_education !== void 0) {
		const projection = sourceEducationProjection(next), known = projection.education_level !== "unknown";
		const p = next.source_education;
		next.education = {
			value: projection.education_level,
			origin: known ? "source_explicit" : "unknown",
			verification: next.source_education_verification === "verified" ? "verified" : "pending",
			evidence_ids: known ? p.education_level.evidence_ids : [],
			reason: next.source_education_verification_reason ?? "原文件来源核验"
		};
	}
	return next;
}
/** Host keeps this complete record in its CAS ledger, including ignored/conflicting incoming values. */
function sourceEducationUpdate(before, incoming) {
	const old = readSourceEducation(before), next = readSourceEducation(incoming);
	const oldSource = old.source_education, newSource = next.source_education;
	if (old.source_education_revision && old.source_education_revision === next.source_education_revision && sourceEducationRevision(oldSource) === sourceEducationRevision(newSource) && ["verified", "conflict"].includes(old.source_education_verification ?? "")) return {
		before: structuredClone(before),
		incoming: structuredClone(incoming),
		after: structuredClone(before),
		outcome: "unchanged"
	};
	const known = (s, key) => s?.[key]?.status === "source_explicit" ? s[key].value : "unknown";
	let outcome = "updated";
	const oldProfile = decode(before.research_exemplar);
	const legacyLevel = oldProfile?.education?.verification === "verified" && oldProfile.education.origin === "source_explicit" ? oldProfile.education.value : before.education_level && before.education_basis && before.education_basis !== "[]" ? String(before.education_level) : "unknown";
	if (next.source_education_verification !== "conflict" && (old.source_education_verification === "verified" && ["education_level", "study_mode"].some((k) => known(oldSource, k) !== "unknown" && newSource?.[k]?.status === "unknown") || legacyLevel !== "unknown" && newSource?.education_level?.status === "unknown")) return {
		before: structuredClone(before),
		incoming: structuredClone(incoming),
		after: structuredClone(before),
		outcome: "retained_known"
	};
	if (old.source_education_verification === "conflict" || next.source_education_verification === "conflict" || next.source_education_verification === "verified" && (old.source_education_verification === "verified" && ["education_level", "study_mode"].some((k) => known(oldSource, k) !== "unknown" && known(newSource, k) !== "unknown" && known(oldSource, k) !== known(newSource, k)) || legacyLevel !== "unknown" && known(newSource, "education_level") !== "unknown" && legacyLevel !== known(newSource, "education_level"))) {
		next.source_education_verification = "conflict";
		next.source_education_verified_revision = "";
		next.source_education_verification_reason = "新旧来源存在冲突，完整双方保存在接收CAS账本；有效值暂为未知";
		outcome = "conflict";
	}
	const patch = Object.fromEntries(Object.entries(next).map(([k, v]) => [k, k === "source_education" ? JSON.stringify(v) : v]));
	return {
		before: structuredClone(before),
		incoming: structuredClone(incoming),
		after: {
			...before,
			...patch
		},
		outcome
	};
}
//#endregion
//#region src/research/exemplars/rag.ts
const stableHash = (value) => (0, node_crypto.createHash)("sha256").update(typeof value === "string" ? value : JSON.stringify(value)).digest("hex");
const exemplarRubricVersion = "1.1.1";
const assessmentSnapshot = (p) => {
	const parts = [
		p.source_version,
		p.rubric_version,
		p.taxonomy_version,
		p.pack_id,
		p.pack_version,
		p.evaluation.id,
		p.semantic_review,
		p.education,
		p.applicable_levels,
		p.evidence,
		p.units,
		p.method_bases,
		p.relations
	];
	if (p.source_education !== void 0) parts.push(readSourceEducation(p));
	return stableHash(parts);
};
const layoutText = (s) => s.replace(/[\s\u200b\ufeff]/gu, "");
/** Covers a verbatim author quote across already-read excerpts; only layout whitespace differs. */
function evidenceRead(quote, excerpts) {
	const wanted = layoutText(quote), texts = excerpts.map(layoutText);
	if (!wanted.length) return false;
	// Native slices can cut through a short heading or the quote's opening anchor.
	// The caller supplies source-ordered excerpts; verify the complete contiguous text.
	if (texts.some((text) => text.includes(wanted)) || texts.join("").includes(wanted)) return true;
	let offset = 0;
	while (offset < wanted.length) {
		const anchor = wanted.slice(offset, offset + Math.min(16, wanted.length - offset));
		let covered = 0;
		for (const text of texts) {
			let at = text.indexOf(anchor);
			while (at >= 0) {
				let size = anchor.length;
				while (offset + size < wanted.length && text[at + size] === wanted[offset + size]) size++;
				covered = Math.max(covered, size);
				at = text.indexOf(anchor, at + 1);
			}
		}
		if (!covered) return false;
		offset += covered;
	}
	return true;
}
function evidenceChunkIds(quote, chunks) {
	const wanted = layoutText(quote), texts = chunks.map((c) => ({
		id: c.id,
		text: layoutText(c.content ?? c.content_with_weight ?? "")
	}));
	if (!wanted.length) return [];
	const full = texts.filter((c) => c.text.includes(wanted));
	if (full.length) return [...new Set(full.map((c) => c.id))].slice(0, 3);
	const joined = texts.map((c) => c.text).join("");
	const start = joined.indexOf(wanted);
	if (start >= 0) {
		let position = 0;
		const matched = [];
		for (const c of texts) {
			const end = position + c.text.length;
			if (end > start && position < start + wanted.length) matched.push(c.id);
			position = end;
		}
		if (matched.length <= 80) return [...new Set(matched)];
	}
	const selected = /* @__PURE__ */ new Set();
	let offset = 0;
	while (offset < wanted.length && selected.size < 80) {
		const anchor = wanted.slice(offset, offset + Math.min(24, wanted.length - offset));
		let best;
		for (const c of texts) {
			let at = c.text.indexOf(anchor);
			while (at >= 0) {
				let size = anchor.length;
				while (offset + size < wanted.length && c.text[at + size] === wanted[offset + size]) size++;
				if (!best || size > best.size) best = {
					id: c.id,
					size
				};
				at = c.text.indexOf(anchor, at + 1);
			}
		}
		if (!best) break;
		selected.add(best.id);
		offset += best.size;
	}
	return [...selected];
}
/** The ingestion handoff and generated table recaps are not the author's research text. */
function exemplarReviewText(original) {
	return original.split(/^# 图表\/表格写作参考结构\s*$/m)[0].replace(/\[PAPER_PIPELINE_METADATA_V1\][\s\S]*?\[\/PAPER_PIPELINE_METADATA_V1\]/g, "").replace(/^资料关联（程序生成）：.*$/gm, "").replace(/^图像描述（generated，AI 生成）：.*$/gm, "");
}
/** Same exclusions as review text, preserving offsets into the immutable source. */
function exemplarAuthorText(original) {
	return original.split(/^# 图表\/表格写作参考结构\s*$/m)[0].replace(/\[PAPER_PIPELINE_METADATA_V1\][\s\S]*?\[\/PAPER_PIPELINE_METADATA_V1\]/g, (v) => " ".repeat(v.length)).replace(/^(?:资料关联（程序生成）：|图像描述（generated，AI 生成）：).*$/gm, (v) => " ".repeat(v.length));
}
/** The rule is per unit x aspect, never a whole-document veto. */
function decideUse(judgments, allowedScope) {
	if (judgments.some((j) => j.content === "invalid" && j.verification === "verified" && j.processing === "succeeded" && j.evidence_ids.length)) return "disabled";
	if (!judgments.length || judgments.some((j) => j.verification !== "verified" || j.processing !== "succeeded" || !j.content || !j.evidence_ids.length)) return "pending";
	if (judgments.some((j) => j.content === "defective")) return allowedScope.trim() ? "limited" : "pending";
	return "recommended";
}
function verifiedAbsence(aspect) {
	return {
		aspect,
		content: null,
		finding: "absent_in_scope",
		verification: "verified",
		processing: "succeeded",
		decision: null
	};
}
function canReuseAssessment(a, b) {
	return [
		"source_version",
		"rubric_version",
		"taxonomy_version",
		"pack_version"
	].every((k) => !!a[k] && a[k] === b[k]);
}
/** Cover/declaration only: references to other dissertations do not identify this paper. */
function extractEducation(source) {
	const stop = source.search(/(?:^|\n)\s*#{0,6}\s*(?:摘\s*要|Abstract|目\s*录|参考文献|第一章|1\s*[.、]?\s*绪论)/im);
	const cover = source.slice(0, Math.min(stop >= 0 ? stop : 6e3, 6e3)).replace(/\[PAPER_PIPELINE_METADATA_V1\][\s\S]*?\[\/PAPER_PIPELINE_METADATA_V1\]/g, (value) => " ".repeat(value.length)).replace(/^(?:资料关联（程序生成）：|图像描述（generated，AI 生成）：).*$/gm, (value) => " ".repeat(value.length));
	const hits = [
		["doctor", /博士(?:研究生)?(?:学位|毕业)(?:设计|论文)/g],
		["master", /硕士(?:研究生)?(?:学位|毕业)(?:设计|论文)/g],
		["bachelor", /本科(?:生)?\s*毕业\s*(?:设计(?:[（(]论文[）)])?|论文)|学士学位论文/g],
		["associate", /(?:专科|大专)(?:生)?\s*毕业\s*(?:设计|论文)/g]
	].flatMap(([value, regex]) => [...cover.matchAll(regex)].map((m) => ({
		value,
		quote: m[0],
		start: m.index,
		end: m.index + m[0].length
	})));
	if (new Set(hits.map((h) => h.value)).size !== 1) return {
		value: "unknown",
		origin: "unknown",
		verification: "pending",
		evidence: [],
		reason: hits.length ? "封面出现冲突层次，待核" : "未找到本篇封面/学位明示，正文水平另评"
	};
	return {
		value: hits[0].value,
		origin: "source_explicit",
		verification: "verified",
		evidence: [hits[0]],
		reason: "本篇封面/学位说明明示"
	};
}
function articleSummary(profile) {
	const core = profile.units.filter((u) => u.core.value === true && u.core.verification === "verified");
	const decisions = [...core.flatMap((u) => u.assessments.filter((a) => ["execution_steps", "result_interpretation"].includes(a.aspect)).map((a) => a.decision)), ...profile.relations.map((r) => r.assessment.decision)];
	const uncertain = profile.evaluation.coverage !== "full_text" || profile.semantic_review && profile.semantic_review.coverage !== "full_text" || !core.length || profile.units.some((u) => u.core.verification !== "verified") || core.some((u) => ["execution_steps", "result_interpretation"].some((aspect) => !u.assessments.some((a) => a.aspect === aspect && a.finding === "present" && a.processing === "succeeded" && a.verification === "verified"))) || core.length > 1 && core.some((u) => !profile.relations.some((r) => [...r.from, ...r.to].includes(u.id)));
	const decision = decisions.includes("disabled") ? "disabled" : uncertain || decisions.includes("pending") || decisions.includes(null) ? "pending" : decisions.includes("limited") ? "limited" : "recommended";
	return {
		scope: "complete_research_route",
		decision,
		reason: decision === "recommended" ? "核心实施、解释及单元衔接已评价；局部用途仍按对应记录读取" : "全篇研究路径存在缺项或限制；可用局部按单元×面向判断"
	};
}
/** Structural validation plus exact provenance. Semantic review remains an explicit evaluator duty. */
function validateProfile(input, original) {
	const parsed = librechat_data_provider.exemplarProfileSchema.parse(input);
	const p = parsed.source_education === void 0 ? parsed : withSourceEducation(parsed, parsed);
	if (p.source_version !== stableHash(original)) throw Error("source_version_mismatch");
	const review = exemplarReviewText(original);
	if (p.evaluation.chars_total !== review.length || p.evaluation.chars_read > review.length || p.evaluation.coverage === "full_text" && p.evaluation.chars_read !== review.length) throw Error("coverage_mismatch");
	const evidence = new Map(p.evidence.map((e) => [e.id, e]));
	if (evidence.size !== p.evidence.length) throw Error("duplicate_evidence");
	const author = exemplarAuthorText(original);
	for (const e of p.evidence) if (e.start >= e.end || original.slice(e.start, e.end) !== e.quote || author.slice(e.start, e.end) !== e.quote) throw Error("evidence_not_in_original");
	const checkEvidence = (ids, needed = true) => {
		if (needed && !ids.length || ids.some((id) => !evidence.has(id))) throw Error("missing_evidence");
	};
	const unitIds = new Set(p.units.map((u) => u.id));
	if (unitIds.size !== p.units.length) throw Error("duplicate_unit");
	if (p.rubric_version === "1.1.1") enforceProfileReview(p, original);
	const assess = (a) => {
		checkEvidence(a.evidence_ids, a.verification === "verified");
		if (a.finding === "absent_in_scope") {
			if (a.content !== null || a.processing !== "succeeded" || a.decision !== (a.verification === "verified" ? null : "pending")) throw Error("absence_state_invalid");
			return;
		}
		if (new Set(a.dimensions.map((d) => d.dimension)).size !== a.dimensions.length) throw Error("duplicate_dimension");
		a.dimensions.forEach((d) => checkEvidence(d.evidence_ids, d.verification === "verified"));
		const complete = librechat_data_provider.exemplarDimensions.every((d) => a.dimensions.some((j) => j.dimension === d));
		const expected = a.finding === "present" && a.processing === "succeeded" && a.verification === "verified" ? decideUse(a.dimensions, a.allowed_scope) : "pending";
		a.decision = !complete && expected !== "disabled" ? "pending" : expected;
		a.verification = a.decision === "pending" ? "pending" : "verified";
	};
	for (const u of p.units) {
		checkEvidence(u.evidence_ids, false);
		if (new Set(u.assessments.map((a) => a.aspect)).size !== u.assessments.length) throw Error("duplicate_aspect");
		u.assessments.forEach(assess);
		for (const f of u.features) {
			checkEvidence(f.evidence_ids, f.verification === "verified");
			if (f.role === "method_requirement" && f.verification === "verified" && (!f.method_basis_ids.length || f.method_basis_ids.some((id) => !p.method_bases.some((b) => b.id === id && b.verification === "verified" && b.reference && b.variant)))) throw Error("method_basis_required");
		}
	}
	for (const r of p.relations) {
		if (!r.from.length || !r.to.length || [...r.from, ...r.to].some((id) => !unitIds.has(id))) throw Error("relation_scope_invalid");
		assess(r.assessment);
	}
	for (const level of p.applicable_levels) {
		checkEvidence(level.evidence_ids, level.verification === "verified");
		if (!level.scope.length || level.scope.some((id) => !unitIds.has(id))) throw Error("level_scope_invalid");
	}
	if (p.source_education === void 0 && p.education.value !== "unknown") {
		checkEvidence(p.education.evidence_ids);
		const cover = extractEducation(original);
		if (p.education.origin === "source_explicit" && cover.value !== p.education.value) throw Error("education_not_explicit");
	}
	p.summary = articleSummary(p);
	return p;
}
//#endregion
//#region src/research/exemplars/agent.ts
const levelNames = {
	associate: "专科",
	bachelor: "本科",
	master: "硕士",
	doctor: "博士"
};
/** Academic origin and fit remain separate; suitability is scoped to the units being read. */
function sourceLearningLevel(profile, target, unitIds) {
	const desired = /本科/.test(target) ? "本科" : /专科/.test(target) ? "专科" : /硕士|研究生/.test(target) ? "硕士" : /博士/.test(target) ? "博士" : target;
	if (!unitIds.length) return null;
	return profile.applicable_levels?.find((l) => levelNames[l.value] === desired && l.verification === "verified" && l.evidence_ids.length && unitIds.every((id) => l.scope.includes(id))) ? desired : null;
}
/** Uses task facts and pack only. Results never redefine their own success criterion. */
function compileNeeds(task, pack) {
	const query = `${task.topic} ${task.method ?? ""}`;
	const matched = pack.agent.task_patterns.filter((p) => p.aliases.some((alias) => query.toLowerCase().includes(alias.toLowerCase())));
	const needs = matched.length ? matched.flatMap((p) => p.needs) : pack.agent.default_needs ?? [];
	const items = [...new Map(needs.map((n) => [`${n.action}:${n.aspect}`, {
		...n,
		id: `${n.action}:${n.aspect}`,
		reason: n.reason ?? "当前任务要求"
	}])).values()];
	return {
		version: "1.0.0",
		task_key: stableHash({
			task,
			pack_id: pack.id,
			pack_version: pack.version,
			items
		}),
		pack_id: pack.id,
		pack_version: pack.version,
		items
	};
}
function transferClaim(t) {
	const { semantic_check, check_reason, ...claim } = librechat_data_provider.exemplarTransferSchema.parse(t);
	return claim;
}
const transferClaimHash = (t) => stableHash(transferClaim(t));
function dependencyRestricted(profile, unitId, aspect) {
	if (!["execution_steps", "result_interpretation"].includes(aspect)) return false;
	const blocked = new Set(profile.units.filter(u => u.assessments.some(a => a.verification === "verified" && a.decision === "disabled" && ["execution_steps", "result_interpretation"].includes(a.aspect))).map(u => u.id));
	for (const r of profile.relations ?? []) if (r.assessment?.verification === "verified" && r.assessment?.decision === "disabled") for (const id of r.to ?? []) blocked.add(id);
	for (let i=0;i<=profile.units.length;i++) {
		const before=blocked.size;
		for (const r of profile.relations ?? []) if ((r.from ?? []).some(id => blocked.has(id))) for (const id of r.to ?? []) blocked.add(id);
		if (blocked.size===before) break;
	}
	return blocked.has(unitId);
}
function assessTransfer(needs, transfers, profiles, reviewed = /* @__PURE__ */ new Set()) {
	const covered = /* @__PURE__ */ new Set();
	const issues = [];
	for (const t of transfers) {
		const n = needs.items.find((n) => n.id === t.need_id && n.aspect === t.aspect);
		const p = profiles.find((p) => p.document_id === t.document_id);
		const u = p?.units.find((u) => u.id === t.unit_id && u.action === n?.action);
		const a = u?.assessments.find((a) => a.aspect === t.aspect);
		let code = "";
		if (!n || !p || !u || !a) code = "not_assessed";
		else if (p.source_version !== t.source_version || p.rubric_version !== t.rubric_version || p.pack_version !== t.pack_version || p.taxonomy_version !== t.taxonomy_version || assessmentSnapshot(p) !== t.assessment_snapshot) code = "assessment_changed";
		else if (a.finding === "absent_in_scope") code = "no_content_in_scope";
		else if (a.decision === "disabled") code = "use_disabled";
		else if (dependencyRestricted(p,u.id,t.aspect)) code = "dependency_disabled";
		else if (!["recommended", "limited"].includes(a.decision ?? "") || a.verification !== "verified" || a.processing !== "succeeded" || a.finding !== "present") code = "verification_pending";
		else if (!reviewed.has(transferClaimHash(t))) code = "independent_review_required";
		else if (t.semantic_check !== "supported") code = t.semantic_check === "contradicted" ? "source_contradicted" : "source_incomplete";
		else if (!t.evidence_ids.length || t.evidence_ids.some((id) => !u.evidence_ids.includes(id) || !p.evidence.some((e) => e.id === id))) code = "evidence_mismatch";
		if (code) issues.push({
			need_id: t.need_id,
			code
		});
		else covered.add(t.need_id);
	}
	const missing = needs.items.filter((n) => n.priority === "required" && !covered.has(n.id)).map((n) => n.id);
	return {
		status: missing.length || issues.length ? "partial" : "ready",
		covered: [...covered],
		missing,
		issues,
		material_gaps: [...new Set(transfers.flatMap((t) => t.material_gaps))]
	};
}
function exemplarReferences(state) {
	return (state.library?.exemplarNeeds?.items ?? []).flatMap((need) => (state.library?.exemplarProfiles ?? []).flatMap((p) => {
		const snapshot = assessmentSnapshot(p);
		return p.units.filter((u) => u.action === need.action).map((u) => {
			const a = u.assessments.find((a) => a.aspect === need.aspect);
			const sources = state.library.sources.filter((s) => s.documentId === p.document_id && s.exemplarUnitIds?.includes(u.id) && s.assessmentSnapshot === snapshot);
			const relevant = p.evidence.filter((e) => u.evidence_ids.includes(e.id));
			const read = relevant.filter((e) => evidenceRead(e.quote, sources.map((s) => s.excerpt)));
			const reference = {
				need_id: need.id,
				document_id: p.document_id,
				unit_id: u.id,
				aspect: need.aspect,
				source_version: p.source_version,
				rubric_version: p.rubric_version,
				taxonomy_version: p.taxonomy_version,
				pack_version: p.pack_version,
				assessment_snapshot: snapshot,
				source_ids: sources.map((s) => s.sourceId),
				evidence_ids: read.map((e) => e.id)
			};
			return {
				reference_id: stableHash([
					state.library.exemplarNeeds.task_key,
					need.id,
					p.document_id,
					u.id,
					need.aspect,
					snapshot
				]).slice(0, 24),
				reference,
				read_request: {
					mode: "sources",
					document_id: p.document_id,
					...relevant.length ? {
						unit_id: u.id,
						evidence_ids: (relevant.some((e) => !read.includes(e)) ? relevant.filter((e) => !read.includes(e)) : relevant).map((e) => e.id).slice(0, 24),
						source_version: p.source_version
					} : {},
					action_types: [need.action],
					aspects: [need.aspect],
					question: `${u.title}：${u.problem}`.slice(0, 1e3)
				},
				title: u.title,
				decision: a?.decision ?? "pending",
				allowed_scope: a?.allowed_scope ?? "",
				limitations: a?.limitations ?? [],
				linkage_ready: read.length > 0 && a?.verification === "verified" && ["recommended", "limited"].includes(a?.decision ?? "") && !dependencyRestricted(p,u.id,need.aspect) && !!sourceLearningLevel(p, state.facts?.level?.value ?? "本科", [u.id]),
				missing_evidence: relevant.filter((e) => !read.includes(e)).map((e) => ({
					evidence_id: e.id,
					section: e.section,
					chunk_ids: e.chunk_ids,
					read_hint: e.quote.slice(0, 160)
				}))
			};
		});
	}));
}
function transferBindingIssue(state, t) {
	const p = state.library?.exemplarProfiles?.find((p) => p.document_id === t.document_id);
	const sources = t.source_ids.map((id) => state.library?.sources.find((s) => s.sourceId === id));
	if (!p || sources.some((s) => !s || s.documentId !== t.document_id || !s.exemplarUnitIds?.includes(t.unit_id) || s.assessmentSnapshot !== t.assessment_snapshot)) return "source_unlinked";
	if (state.facts?.level?.value && !sourceLearningLevel(p, state.facts.level.value, [t.unit_id])) return "level_outside_scope";
	if (t.evidence_ids.some((id) => !p.evidence.some((e) => e.id === id && evidenceRead(e.quote, sources.flatMap((s) => s ? [s.excerpt] : []))))) return "evidence_unlinked";
	return assessTransfer(state.library.exemplarNeeds, [t], [p], new Set([transferClaimHash(t)])).issues[0]?.code;
}
const exemplarLearningInstructions = `研究范例通用协议1.0：先按当前任务的 exemplarNeeds 查缺项，使用 mode=profiles 比较可迁移研究动作和面向，再用 document_id 回读真实正文。exemplar 是带版本的评价，旧 profile 只是候选线索。决定按单元×面向；全文限用不等于所有局部停用。原始学历未知不能覆盖已核实适用层次。材料年份、样本和图表设计仅供参考，真实数字由本题后续取数取得；材料尚未取得不妨碍已支持的方法学习。
learn.guide.exemplar 保存 contract_version=1.0.0、task_key、selections 和 composition_checks。从 exemplarOptions 选择 linkage_ready 的条目，selections 中只传 reference_id、learned_process、adoption、adjustments、material_gaps、boundaries，服务器会补齐身份和版本并独立核实，无须复制长哈希和自行填写 semantic_check。多条 selections 是多个已学用途，references 不得猜造。涉及结果性语句时另填 result_claims{text,mode,basis_ids}，mode 为 source_finding/hypothesis/target_result；方法步骤本身不用结果标签。标签和文字必须相符，缺乏本题独立结果时不能声明 target_result。原文的发现可以归属于原文叙述，假设必须明确待检验。guide 的 structure/patterns/styleRules/transferNotes 也会被独立检查；不要在表达规则里预设新题结果。exemplarReview.accepted 是已核实且真实保存的用途，pending 说明被拒或待核部分；有问题的内容保留为草稿，通过的部分继续保存，按缺项定向修正。缺少引用则按 missing_evidence 的位置补读，read_hint 不代表已经读到。多单元组合记录兼容性与实际限制，硬条件未满足或冲突不得标 compatible。覆盖不足保存已学部分并补缺项，不能删需求凑完成。数据缺口与学习覆盖分开，事实/数字/结论不能从范文迁移。未知条件不伪装已知硬条件。发现画像和原文不符时，可用原检索工具mode=feedback、document_id与feedback{unit_id,aspect,source_version,chunk_id,quote,reason}记录纠错线索；quote逐字引用实际读到的切片。该动作只保存待核记录，不改变共享库评级。`;
//#endregion
//#region src/research/exemplars/transferReview.ts
const transferReviewVersion = "1.0.0";
const transferReviewLimits = {
	initialChars: 64e3,
	supplementalChars: 6e3,
	supplementsPerClaim: 1,
	calls: 6,
	attemptsPerUse: 2,
	attemptsPerNeed: 4,
	searches: 12
};
const useKey = (t) => stableHash([
	t.need_id,
	t.unit_id,
	t.aspect
]);
const fail = (code) => {
	throw Object.assign(Error(code), {
		code,
		status: 409
	});
};
function ledger(state) {
	const needs = state.library?.exemplarNeeds;
	if (!needs) return fail("exemplar_transfer_required");
	const old = state.library?.exemplarLearning;
	return old?.task_key === needs.task_key && old.version === "1.0.0" ? old : {
		version: transferReviewVersion,
		task_key: needs.task_key,
		calls: 0,
		attempts: {},
		accepted: [],
		receipts: [],
		searched: []
	};
}
function contextHash(state, bindings) {
	return stableHash({
		facts: state.facts,
		needs: state.library?.exemplarNeeds,
		profiles: bindings.documents.map((id) => {
			const p = state.library?.exemplarProfiles?.find((p) => p.document_id === id);
			return p && [id, assessmentSnapshot(p)];
		}),
		sources: bindings.sources.map((id) => {
			const s = state.library?.sources.find((s) => s.sourceId === id);
			return s && [
				id,
				s.documentId,
				s.excerpt,
				s.assessmentSnapshot
			];
		})
	});
}
function canonical(value) {
	if (Array.isArray(value)) return value.map(canonical);
	if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => [k, canonical(v)]));
	return value;
}
function guideHash(guide) {
	const { selections, ...exemplar } = guide.exemplar;
	return stableHash(canonical({
		...guide,
		exemplar: {
			...exemplar,
			transfers: exemplar.transfers.map(transferClaim)
		}
	}));
}
function reviewedTransferHashes(state) {
	const l = ledger(state);
	return new Set(l.receipts.filter((r) => r.version === "1.0.0" && r.context_hash === contextHash(state, r.bindings)).flatMap((r) => r.accepted_hashes));
}
function isIndependentlyReviewedGuide(state, guide) {
	if (!guide.exemplar || !state.library?.exemplarNeeds) return false;
	return ledger(state).receipts.some((r) => r.version === "1.0.0" && r.guide_hash === guideHash(guide) && r.context_hash === contextHash(state, r.bindings));
}
/** No generator reasons or model self-rating enter the independent request. */
function prepareTransferReview(state, value) {
	const guide = structuredClone(value), eg = librechat_data_provider.exemplarGuideSchema.parse(guide.exemplar), needs = state.library?.exemplarNeeds;
	if (!needs || eg.task_key !== needs.task_key) return fail("exemplar_task_changed");
	const references = exemplarReferences(state);
	const selected = (eg.selections ?? []).map((s) => {
		const option = references.find((o) => o.reference_id === s.reference_id);
		if (!option?.linkage_ready) return fail("exemplar_reference_not_ready");
		const { reference_id, ...claim } = s;
		return {
			...option.reference,
			...claim,
			semantic_check: "supported",
			check_reason: "等待服务端独立核实"
		};
	});
	const submitted = [...eg.transfers, ...selected];
	if (!submitted.length || new Set(submitted.map(useKey)).size !== submitted.length) return fail("exemplar_unique_transfer_required");
	const submittedKeys = new Set(submitted.map(useKey)), trusted = reviewedTransferHashes(state);
	const carried = ledger(state).accepted.filter((t) => !submittedKeys.has(useKey(t)) && trusted.has(transferClaimHash(t)) && !transferBindingIssue(state, t));
	const transfers = [...carried, ...submitted].map((t) => ({
		...t,
		semantic_check: "supported",
		check_reason: "等待服务端独立核实"
	}));
	for (const t of transfers) {
		const code = transferBindingIssue(state, t);
		if (code) return fail("exemplar_" + code);
		const p = state.library.exemplarProfiles.find((p) => p.document_id === t.document_id);
		const bases = new Set([
			...t.source_ids,
			...t.evidence_ids,
			...p.method_bases.filter((b) => b.verification === "verified").map((b) => b.id)
		]);
		for (const c of t.result_claims ?? []) {
			if (c.mode === "target_result") return fail("exemplar_target_result_unverified");
			if (!c.basis_ids.length || c.basis_ids.some((id) => !bases.has(id))) return fail("exemplar_claim_basis_missing");
		}
	}
	const priorGuide = state.library?.guide;
	const oldPatterns = carried.length && priorGuide && isIndependentlyReviewedGuide(state, priorGuide) ? priorGuide.patterns.filter((p) => p.sourceIds.every((id) => carried.some((t) => t.source_ids.includes(id)))) : [];
	guide.patterns = [...new Map([...oldPatterns, ...guide.patterns].map((p) => {
		const normalized = {
			...p,
			sourceIds: [...new Set(p.sourceIds)]
		};
		return [stableHash(canonical(normalized)), normalized];
	})).values()];
	const used = [...new Set(transfers.map((t) => t.unit_id))];
	const composition = [...eg.composition_checks];
	if (used.length > 1 && !composition.some((c) => used.every((id) => c.unit_ids.includes(id)))) composition.push({
		unit_ids: used,
		status: "pending",
		reason: "合并先前已核实用途与本次补学用途，待独立核实整体兼容性"
	});
	guide.exemplar = {
		contract_version: eg.contract_version,
		task_key: eg.task_key,
		transfers,
		composition_checks: composition
	};
	const documents = [...new Set(transfers.map((t) => t.document_id))];
	const context = state.library.sources.filter((s) => documents.includes(s.documentId));
	const bindings = {
		documents,
		sources: context.map((s) => s.sourceId)
	};
	const context_hash = contextHash(state, bindings);
	const targets = transfers.map((t) => ({
		id: "transfer:" + transferClaimHash(t).slice(0, 24),
		kind: "transfer",
		claim: transferClaim(t),
		criterion: librechat_data_provider.exemplarAspectCriteria[t.aspect]
	}));
	const append = (kind, rows) => rows.forEach((claim, i) => targets.push({
		id: `guide:${kind}:${i}`,
		kind,
		claim,
		criterion: "仅迁移原文实际展示的步骤、组织或解释方式，保留限制；不预设本题事实、系数方向、显著性或结论。"
	}));
	append("structure", guide.structure);
	append("pattern", guide.patterns);
	append("style", guide.styleRules);
	append("notes", [guide.transferNotes]);
	append("outline", guide.outline ?? []);
	append("composition", composition);
	targets.filter((t) => t.kind === "composition" && t.claim.status === "pending").forEach((t) => {
		t.criterion = "判断所列单元能否按当前迁移方案共同使用。仅确认适用条件兼容且缺陷隔离后返回 supported；信息不足为 insufficient，存在不能隔离的冲突为 contradicted。这是待核问题，不是已有兼容结论。";
	});
	if (targets.length > 240) return fail("exemplar_review_capacity");
	let remaining = transferReviewLimits.initialChars;
	const read_scopes = context.flatMap((s) => {
		const end = Math.min(s.excerpt.length, remaining);
		remaining -= end;
		return end ? [{
			source_id: s.sourceId,
			start: 0,
			end,
			supplemental: false
		}] : [];
	});
	const payload = {
		task: state.facts,
		needs,
		targets,
		units: transfers.map((t) => {
			const p = state.library.exemplarProfiles.find((p) => p.document_id === t.document_id);
			const u = p.units.find((u) => u.id === t.unit_id);
			return {
				document_id: p.document_id,
				unit_id: u.id,
				action: u.action,
				aspect: t.aspect,
				allowed_scope: u.assessments.find((a) => a.aspect === t.aspect)?.allowed_scope,
				limitations: u.assessments.find((a) => a.aspect === t.aspect)?.limitations,
				features: u.features,
				evidence: p.evidence.filter((e) => t.evidence_ids.includes(e.id)).map((e) => ({
					id: e.id,
					section: e.section,
					chunk_ids: e.chunk_ids
				}))
			};
		}),
		source_context: read_scopes.map((r) => ({
			...r,
			text: context.find((s) => s.sourceId === r.source_id).excerpt.slice(r.start, r.end)
		})),
		available_context: context.map((s) => ({
			source_id: s.sourceId,
			document_id: s.documentId,
			chunk_id: s.chunkId,
			chars: s.excerpt.length,
			truncated: s.truncated === true
		})),
		scope: "retrieved_source_context",
		supplemental: false
	};
	const input_hash = stableHash([
		transferReviewVersion,
		context_hash,
		guideHash(guide),
		payload
	]);
	const l = ledger(state), cached = l.receipts.some((r) => r.input_hash === input_hash && r.context_hash === context_hash);
	if (!cached) {
		if (l.calls >= transferReviewLimits.calls) return fail("exemplar_review_budget");
		for (const t of transfers) {
			if (reviewedTransferHashes(state).has(transferClaimHash(t))) continue;
			if ((l.attempts["use:" + useKey(t)] ?? 0) >= transferReviewLimits.attemptsPerUse || (l.attempts["need:" + t.need_id] ?? 0) >= transferReviewLimits.attemptsPerNeed) return fail("exemplar_repair_limit");
		}
	}
	return {
		guide,
		input_hash,
		context_hash,
		bindings,
		read_scopes,
		payload,
		cached
	};
}
/** This function is called only by the trusted runtime, never with tool-supplied reviews. */
function applyTransferReview(state, request, raw, reviewer, scopes = request.read_scopes, usage) {
	if (contextHash(state, request.bindings) !== request.context_hash) return fail("exemplar_review_input_changed");
	const checks = librechat_data_provider.exemplarReviewResultSchema.parse(raw).checks;
	if (checks.length !== request.payload.targets.length || new Set(checks.map((c) => c.target)).size !== checks.length || checks.some((c) => !request.payload.targets.some((t) => t.id === c.target) || c.status === "supported" !== (c.problem === "none"))) return fail("exemplar_review_targets_invalid");
	const initial = scopes.filter((s) => !s.supplemental), extra = scopes.filter((s) => s.supplemental);
	if (stableHash(initial) !== stableHash(request.read_scopes) || extra.reduce((n, s) => n + s.end - s.start, 0) > transferReviewLimits.supplementalChars || extra.some((r) => !request.bindings.sources.includes(r.source_id) || r.start < 0 || r.end <= r.start || r.end > (state.library?.sources.find((s) => s.sourceId === r.source_id)?.excerpt.length ?? 0))) return fail("exemplar_review_scope_invalid");
	const next = structuredClone(state), l = structuredClone(ledger(state));
	const prior = l.receipts.find((r) => r.input_hash === request.input_hash);
	if (!prior) l.calls++;
	const supported = (id) => checks.some((c) => c.target === id && c.status === "supported");
	const passed = request.guide.exemplar.transfers.filter((t) => supported("transfer:" + transferClaimHash(t).slice(0, 24))).map((t) => ({
		...t,
		semantic_check: "supported",
		check_reason: checks.find((c) => c.target === "transfer:" + transferClaimHash(t).slice(0, 24)).reason
	}));
	if (!prior) for (const t of request.guide.exemplar.transfers) {
		const c = checks.find((c) => c.target === "transfer:" + transferClaimHash(t).slice(0, 24));
		if (c.status === "supported") continue;
		for (const key of [
			"use:" + useKey(t),
			"need:" + t.need_id,
			"problem:" + stableHash([
				request.guide.exemplar.task_key,
				t.need_id,
				t.unit_id,
				t.aspect,
				c.problem
			])
		]) l.attempts[key] = (l.attempts[key] ?? 0) + 1;
	}
	const accepted = new Map(l.accepted.map((t) => [useKey(t), t]));
	passed.forEach((t) => accepted.set(useKey(t), t));
	l.accepted = [...accepted.values()].slice(-64);
	l.draft = request.guide;
	const filtered = {
		...request.guide,
		structure: request.guide.structure.filter((_, i) => supported("guide:structure:" + i)),
		patterns: request.guide.patterns.filter((p, i) => supported("guide:pattern:" + i) && p.sourceIds.every((id) => passed.some((t) => t.source_ids.includes(id)))),
		styleRules: request.guide.styleRules.filter((_, i) => supported("guide:style:" + i)),
		transferNotes: supported("guide:notes:0") ? request.guide.transferNotes : "",
		outline: request.guide.outline?.filter((_, i) => supported("guide:outline:" + i)),
		exemplar: {
			...request.guide.exemplar,
			transfers: passed,
			composition_checks: request.guide.exemplar.composition_checks.flatMap((c, i) => supported("guide:composition:" + i) ? [{
				...c,
				...c.status === "pending" ? {
					status: "compatible",
					reason: checks.find((check) => check.target === "guide:composition:" + i).reason
				} : {}
			}] : [])
		}
	};
	const used = [...new Set(passed.map((t) => t.unit_id))];
	filtered.exemplar.composition_checks = filtered.exemplar.composition_checks.map((c) => ({
		...c,
		unit_ids: c.unit_ids.filter((id) => used.includes(id))
	})).filter((c) => c.unit_ids.length > 1);
	const composed = used.length <= 1 || filtered.exemplar.composition_checks.some((c) => c.status === "compatible" && used.every((id) => c.unit_ids.includes(id)));
	const parsed = (0, librechat_data_provider.researchGuideSchema)(!!state.facts?.scope && state.facts.scope.value !== "article").safeParse(filtered);
	const guide = passed.length && composed && parsed.success ? parsed.data : void 0;
	const receipt = {
		version: transferReviewVersion,
		input_hash: request.input_hash,
		context_hash: request.context_hash,
		bindings: request.bindings,
		reviewer,
		created_at: (/* @__PURE__ */ new Date()).toISOString(),
		accepted_hashes: passed.map(transferClaimHash),
		checks,
		read_scopes: scopes,
		...guide ? { guide_hash: guideHash(guide) } : {},
		...usage ? { usage } : {}
	};
	l.receipts = [...l.receipts.filter((r) => r.input_hash !== request.input_hash), receipt].slice(-12);
	delete l.reservation;
	next.library.exemplarLearning = l;
	return {
		state: next,
		guide,
		receipt
	};
}
async function reviewTransferGuide(state, guide, invoke, reviewer, signal) {
	const request = prepareTransferReview(state, guide), cached = ledger(state).receipts.find((r) => r.input_hash === request.input_hash);
	if (cached) return applyTransferReview(state, request, cached, cached.reviewer, cached.read_scopes, cached.usage);
	let raw = await invoke(request.payload, signal);
	const scopes = [...request.read_scopes];
	const requests = raw?.read_requests;
	if (Array.isArray(requests) && requests.length) {
		const targets = /* @__PURE__ */ new Set();
		let remaining = transferReviewLimits.supplementalChars;
		for (const r of requests) {
			const source = state.library.sources.find((s) => s.sourceId === r.source_id);
			if (!request.payload.targets.some((t) => t.id === r.target) || targets.has(r.target) || !source || !request.bindings.sources.includes(r.source_id) || !Number.isInteger(r.start) || !Number.isInteger(r.end) || r.start < 0 || r.end <= r.start || r.end > source.excerpt.length || r.end - r.start > remaining) continue;
			targets.add(r.target);
			remaining -= r.end - r.start;
			scopes.push({
				source_id: r.source_id,
				start: r.start,
				end: r.end,
				supplemental: true
			});
		}
		if (targets.size) {
			const follow = {
				...request.payload,
				supplemental: true,
				targets: request.payload.targets.filter((t) => targets.has(t.id)),
				source_context: scopes.map((r) => ({
					...r,
					text: state.library.sources.find((s) => s.sourceId === r.source_id).excerpt.slice(r.start, r.end)
				}))
			};
			const extra = librechat_data_provider.exemplarReviewResultSchema.parse(await invoke(follow, signal));
			raw = { checks: librechat_data_provider.exemplarReviewResultSchema.parse(raw).checks.map((c) => targets.has(c.target) ? extra.checks.find((e) => e.target === c.target) ?? c : c) };
		}
	}
	return applyTransferReview(state, request, raw, reviewer, scopes);
}
function transferReviewStatus(state) {
	const l = ledger(state), hashes = reviewedTransferHashes(state);
	return {
		scope: "retrieved_source_context",
		activation_error: l.activation_error,
		calls: l.calls,
		limits: transferReviewLimits,
		accepted: l.accepted.filter((t) => hashes.has(transferClaimHash(t))),
		pending: l.receipts.at(-1)?.checks.filter((c) => c.status !== "supported") ?? [],
		searched: l.searched ?? [],
		exhausted: l.calls >= transferReviewLimits.calls || (l.searched?.length ?? 0) >= transferReviewLimits.searches,
		instruction: "通过的迁移项已保存；只修正 pending 指明的内容。预算用尽仅表示本轮已读/已检索范围未覆盖，不代表整个库不存在。"
	};
}
//#endregion
//#region src/research/exemplars/ingestion.ts
const decoded = (v) => {
	if (typeof v === "string") try {
		return JSON.parse(v);
	} catch {
		return {};
	}
	return v && typeof v === "object" ? v : {};
};
function readExemplarPack(value) {
	return librechat_data_provider.exemplarPackSchema.parse(value);
}
/** Lossless old metadata stays outside the additive contract; old quality is never upgraded. */
function importExemplarCandidate(doc, pack, original) {
	const m = doc.meta_fields ?? {}, a = decoded(m[pack.rag.annotation_field]);
	const old = decoded(m[pack.rag.profile_field]);
	let methods = m[pack.rag.methods_field] ?? a.research_methods;
	if (typeof methods === "string") try {
		methods = JSON.parse(methods);
	} catch {
		methods = [];
	}
	const sourceVersion = original === void 0 ? String(m[pack.rag.source_hash_field] ?? "unknown") : stableHash(original);
	const units = [];
	for (const method of Array.isArray(methods) ? methods : []) {
		if (!method?.name || !method?.implementation) continue;
		const match = pack.methods.find((p) => p.aliases.some((alias) => String(method.name).toLowerCase().includes(alias.toLowerCase())));
		for (const action of match?.actions ?? ["interpretation"]) units.push({
			id: `${doc.id}:${stableHash([
				sourceVersion,
				method.name,
				method.implementation,
				action
			]).slice(0, 16)}`,
			action,
			title: String(method.name).slice(0, 1800),
			problem: String(old.research_path ?? "").slice(0, 1800),
			inputs: [],
			steps: [String(method.implementation).slice(0, 1800)],
			output: "",
			evidence_ids: [],
			features: [],
			core: {
				value: null,
				verification: "pending",
				reason: "旧画像仅用于候选发现，尚未按通用标准评价"
			},
			assessments: []
		});
	}
	const education = original === void 0 || m.source_education !== void 0 ? {
		value: "unknown",
		origin: "unknown",
		verification: "pending",
		evidence: [],
		reason: "尚未读取封面；原文件来源对象单独核验"
	} : extractEducation(original);
	const evidence = education.evidence.map((e, i) => ({
		...e,
		id: `cover:${i}`,
		section: "封面/学位说明",
		chunk_ids: []
	}));
	return withSourceEducation({
		contract_version: librechat_data_provider.exemplarVersion,
		dataset_id: doc.dataset_id,
		document_id: doc.id,
		document_name: doc.name,
		source_version: sourceVersion,
		rubric_version: exemplarRubricVersion,
		taxonomy_version: librechat_data_provider.exemplarVersion,
		pack_id: pack.id,
		pack_version: pack.version,
		evaluation: {
			id: `import:${stableHash([
				doc.id,
				sourceVersion,
				pack.version
			]).slice(0, 24)}`,
			evaluator: "legacy-adapter",
			created_at: (/* @__PURE__ */ new Date()).toISOString(),
			coverage: original === void 0 ? "unknown" : "partial",
			chars_read: original === void 0 ? 0 : Math.min(6e3, exemplarReviewText(original).length),
			chars_total: original === void 0 ? 0 : exemplarReviewText(original).length
		},
		education: {
			value: education.value,
			origin: education.origin,
			verification: education.verification,
			evidence_ids: evidence.map((e) => e.id),
			reason: education.reason
		},
		applicable_levels: [],
		evidence,
		units: units.slice(0, 24),
		method_bases: [],
		relations: [],
		summary: {
			scope: "complete_research_route",
			decision: "pending",
			reason: "候选导入，不把旧暂缓、旧档位当作新质量/学历评价"
		}
	}, m);
}
/** Caller injects the configured evaluator; this module never chooses or changes a model. */
async function processExemplarDocument(doc, pack, original, evaluate) {
	const candidate = importExemplarCandidate(doc, pack, original);
	if (!evaluate) return validateProfile(candidate, original);
	return validateProfile(withSourceEducation({
		...await evaluate({
			profile: candidate,
			original,
			pack
		}),
		rubric_version: candidate.rubric_version,
		semantic_review: void 0
	}, doc.meta_fields ?? {}), original);
}
/** Additive metadata payload. The writer must CAS against the captured current metadata. */
function exemplarMetadataPatch(profile, metadata) {
	const projected = sourceEducationProjection(profile);
	const stored = structuredClone(profile);
	if (profile.source_education !== void 0) {
		if (metadata?.source_education === void 0) throw Error("source_metadata_context_required");
		const authoritative = withSourceEducation(profile, metadata);
		if (JSON.stringify(sourceEducationProjection(authoritative)) !== JSON.stringify(projected)) throw Error("source_metadata_projection_changed");
		const prior = decoded(metadata.research_exemplar);
		const priorIds = prior.education?.evidence_ids;
		const legacyEvidenceKept = Array.isArray(priorIds) && (prior.education.value === "unknown" || priorIds.length > 0) && priorIds.every((id) => {
			const old = (prior.evidence ?? []).find((e) => e.id === id), next = stored.evidence.find((e) => e.id === id);
			return old && next && old.start === next.start && old.end === next.end && old.quote === next.quote;
		});
		stored.education = prior.source_version === profile.source_version && legacyEvidenceKept ? prior.education : {
			value: "unknown",
			origin: "unknown",
			verification: "pending",
			evidence_ids: [],
			reason: "原文件来源标注保存在独立元数据，读取时应用有效学历投影"
		};
		for (const key of sourceEducationKeys) delete stored[key];
	}
	return {
		research_exemplar: JSON.stringify(stored),
		research_exemplar_version: profile.contract_version,
		research_exemplar_source_sha256: profile.source_version,
		education_level: String(projected.education_level),
		education_basis: JSON.stringify(projected.education_basis)
	};
}
//#endregion
exports.applyProfileReview = applyProfileReview;
exports.applyTransferReview = applyTransferReview;
exports.articleSummary = articleSummary;
exports.assessTransfer = assessTransfer;
exports.assessmentSnapshot = assessmentSnapshot;
exports.canReuseAssessment = canReuseAssessment;
exports.compileNeeds = compileNeeds;
exports.decideUse = decideUse;
exports.dependencyRestricted = dependencyRestricted;
exports.enforceProfileReview = enforceProfileReview;
exports.evidenceChunkIds = evidenceChunkIds;
exports.evidenceRead = evidenceRead;
exports.exemplarAuthorText = exemplarAuthorText;
exports.exemplarLearningInstructions = exemplarLearningInstructions;
exports.exemplarMetadataPatch = exemplarMetadataPatch;
exports.exemplarReferences = exemplarReferences;
exports.exemplarReviewText = exemplarReviewText;
exports.exemplarRubricVersion = exemplarRubricVersion;
exports.extractEducation = extractEducation;
exports.importExemplarCandidate = importExemplarCandidate;
exports.isIndependentlyReviewedGuide = isIndependentlyReviewedGuide;
exports.prepareTransferReview = prepareTransferReview;
exports.processExemplarDocument = processExemplarDocument;
exports.profileReviewBudget = profileReviewBudget;
exports.profileReviewInput = profileReviewInput;
exports.profileReviewVersion = profileReviewVersion;
exports.readExemplarPack = readExemplarPack;
exports.readSourceEducation = readSourceEducation;
exports.reviewTransferGuide = reviewTransferGuide;
exports.reviewedTransferHashes = reviewedTransferHashes;
exports.sourceEducationKeys = sourceEducationKeys;
exports.sourceEducationProjection = sourceEducationProjection;
exports.sourceEducationRevision = sourceEducationRevision;
exports.sourceEducationUpdate = sourceEducationUpdate;
exports.sourceLearningLevel = sourceLearningLevel;
exports.stableHash = stableHash;
exports.transferBindingIssue = transferBindingIssue;
exports.transferClaim = transferClaim;
exports.transferClaimHash = transferClaimHash;
exports.transferReviewLimits = transferReviewLimits;
exports.transferReviewStatus = transferReviewStatus;
exports.transferReviewVersion = transferReviewVersion;
exports.validateProfile = validateProfile;
exports.validateProfileReadScopes = validateProfileReadScopes;
exports.validateSourceEducation = validateSourceEducation;
exports.verifiedAbsence = verifiedAbsence;
exports.withSourceEducation = withSourceEducation;

//# sourceMappingURL=exemplars.cjs.map
