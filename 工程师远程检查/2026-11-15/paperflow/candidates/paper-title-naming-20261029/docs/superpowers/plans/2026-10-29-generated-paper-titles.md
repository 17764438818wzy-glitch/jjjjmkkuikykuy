# Generated Paper Titles Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task.

**Goal:** Give the twelve already identified title-less papers one separate model request each to generate a suitable paper title from its existing body text.

**Architecture:** Reuse the live native connection, throttle, Prefect concurrency limits and single-request journal, but keep supplementary request records outside `papers/`. Bind each request to the paper, received file ID and source hash; store each generated title as generated rather than an original-source assertion. A later isolated naming adapter may consume this separate record without changing the original document or previous receipts.

**Tech Stack:** Python, Prefect 3, filelock, native `preextract_request.request_once`.

**Spec:** The user's 2026-10-29 instructions: only the twelve previously identified papers; at most one additional title request each; name directly from the article without requiring supporting quotes.

## Global Constraints

- Preserve all existing bodies, profile requests, RAG/NAS objects and receipts.
- Use the configured route and its live shared body/total concurrency limits.
- Reuse the saved text only after its original source and text hashes match.
- Never automatically repeat an uncertain, interrupted, failed or partial request.
- Do not change live application code while the current batch runs.

## Tasks

1. Add tests for stable identity, saved-source and capacity checks, response parsing, and no repeat on uncertain journal; run them red.
2. Implement a standalone, independently journaled title-request runner and run its tests green.
3. Submit one real authorized request, verify its complete persisted response and title, then process the other eleven once each while sharing existing quotas.
4. Add and test isolated naming support for generated titles, recording them as generated, and state accurately whether the live program has been updated.
