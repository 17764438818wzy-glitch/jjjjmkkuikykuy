# 原始论文库整理 Implementation Plan

> For agentic workers: use executing-plans for this one-time data operation. User has explicitly authorized execution; do not request another approval.

**Goal:** 将总论文整理为独立目录，保留原始文件不变，并保留每份资料的来源。

**Architecture:** 原目录只读；标准库和现有纯判别函数生成清单；在 F 盘新目录复制正文及附件，原始压缩包另存，排除材料只记来源；歧义单独保留。

**Tech Stack:** Python 3.14, zipfile/XML/PyMuPDF, existing NVIDIA 7-Zip CLI, existing screening rules.

**Spec:** 本聊天用户已同意的“学校/个人/正文、附件”结构，以及“原始文件不要动，整理后的单独放一个文件夹”。

## Global Constraints

- Source: C:\Users\Administrator\Desktop\总论文; never write, rename, delete, or link source files.
- Output: F:\总论文_整理后_20261028; no overwriting preexisting output.
- No production code, service, LLM, RAG or NAS operations.
- Keep source identity and SHA256; selected file copies must match source/member hashes.
- Only exact duplicates are merged automatically. Ambiguous competing bodies remain available for review.

## Review Focus

- Mixed process archives may contain the only manuscript.
- A defense/report keyword may occur in a real thesis filename.
- Letter-bearing student IDs and different directory depths must retain separate people.
- Archives can have legacy Chinese encodings, unsafe paths, nested files, or unsupported encryption.
- Original DOC may be named DOCX; use file signatures, avoid rewriting document contents.

## Execution

- [x] Inventory source files and directory hierarchy; choose F drive based on 64 GiB input.
- [x] Reuse review: inspect pure role/screening rules and available extraction tools; do not import production entry points.
- [x] Inspect archive inventories, assign all files to personal folders, classify using bounded content evidence.
- [x] Exercise representative real cases (body ZIP, exact duplicate, mixed process archive, drawing PDF, defense manuscript).
- [x] Materialize new directory with exclusive writes, provenance and SHA256; retain uncertain materials separately.
- [x] Audit output hashes, coverage, source inventory/content, and independent reviewer findings; deliver actual counts and exceptions.

## Rulings

- This is a one-time data organization task in an empty workspace, not a product branch. No Git initialization, service import, or production code edits.
- User already authorized the proposed separate-copy structure. Continue execution without a second plan approval.
- A suspected auxiliary document is not discarded on a broad filename keyword alone.

## Delivered Evidence

- Output: F:\总论文_整理后_20261028.
- 19,787 groups: 19,516 selected bodies, 251 unresolved competing-body groups, 20 excluded-only groups.
- Original 22,767 files: unchanged path set, size, mtime and SHA256; every original has a recorded disposition.
- 24,755 output data files and all originals passed 47,522 SHA256 checks. Missing, extra and mismatch counts are zero.
- Final attachment audit: 517 generated ZIPs / 291,601 members; 827 unchanged attachment archives; 330 standalone attachments. Explicit process-artifact audit has zero issues.
- Independent review found HTML report leakage and confirmed exact-member correction scope. Real-case verification preserved 14 DWG files and genuine body documents.
- Two 7z directory-parsing failures were repaired from Attributes metadata, then affected copies rebuilt. Final unresolved archive notes: zero.
- Final reports distinguish byte integrity from semantic classification; uncertain body/type material remains available separately.
