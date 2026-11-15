"""Candidate batching that preserves the direct chunk vector calculation.

The caller provides the existing model/context wrapper and a verified model
token counter. No network, credentials, or production configuration is loaded.
"""
import math


def encode_batches(model, document_name, requests, *, max_chunks, max_batch_tokens,
                   max_input_tokens, count_tokens, normalize_user, encode_with_user):
    if any(type(n) is not int or n < 1 for n in (max_chunks, max_batch_tokens, max_input_tokens)):
        raise ValueError("batch and token limits must be positive integers")
    if not isinstance(document_name, str) or not isinstance(requests, list):
        raise ValueError("document name and chunk requests are required")
    if not callable(count_tokens) or not callable(normalize_user) or not callable(encode_with_user):
        raise ValueError("verified counter and existing model context wrapper are required")
    if not requests:
        return {"items": [], "token_count": 0}
    if any(not isinstance(req, dict) for req in requests):
        raise ValueError("chunk requests must be objects")
    if len({normalize_user(req.get("user_id")) for req in requests}) != 1:
        raise ValueError("a batch cannot mix model user identities")

    def tokens(text):
        value = count_tokens(text)
        if type(value) is not int or value < 0 or value > max_input_tokens:
            raise ValueError("input exceeds the verified model token limit")
        return value

    title_tokens = tokens(document_name)
    prepared = []
    # Validate every input before the first model call, avoiding partial work.
    for index, req in enumerate(requests):
        content = req.get("content")
        questions = req.get("questions", [])
        if not isinstance(content, str) or not content.strip():
            raise ValueError("non-empty chunk content is required")
        if not isinstance(questions, list) or any(not isinstance(q, str) for q in questions):
            raise ValueError("questions must be a list of strings")
        questions = [q.strip() for q in questions if q.strip()]
        body = "\n".join(questions) if questions else content
        cost = title_tokens + tokens(body)
        if cost > max_batch_tokens:
            raise ValueError("a single chunk exceeds the batch token limit")
        prepared.append((index, req, body, cost))

    groups, group, cost = [], [], 0
    for item in prepared:
        if group and (len(group) == max_chunks or cost + item[3] > max_batch_tokens):
            groups.append(group)
            group, cost = [], 0
        group.append(item)
        cost += item[3]
    if group:
        groups.append(group)

    result, total, dimension = [], 0, None
    for group in groups:
        texts = [text for _, _, body, _ in group for text in (document_name, body)]
        vectors, token_count = encode_with_user(model, texts, group[0][1])
        if len(vectors) != len(texts) or type(token_count) is not int or token_count < 0:
            raise ValueError("embedding provider returned incomplete vectors or token count")
        total += token_count
        for i, (index, _, _, _) in enumerate(group):
            title_vector, body_vector = vectors[2 * i:2 * i + 2]
            if not len(title_vector) or len(title_vector) != len(body_vector):
                raise ValueError("embedding vector dimensions differ")
            vector = (0.1 * title_vector + 0.9 * body_vector).tolist()
            if dimension is not None and len(vector) != dimension:
                raise ValueError("embedding vector dimensions changed between chunks")
            if not all(math.isfinite(value) for value in vector):
                raise ValueError("embedding vector contains a non-finite value")
            dimension = len(vector)
            result.append({"index": index, "vector": vector})
    return {"items": result, "token_count": total}
