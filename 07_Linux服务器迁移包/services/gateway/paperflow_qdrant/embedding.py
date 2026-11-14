"""Use the existing Ollama model with its native multi-input endpoint."""
from threading import BoundedSemaphore
import math
import httpx


class OllamaEmbedder:
    def __init__(self, url, model='bge-m3', *, concurrency=4, dimensions=1024):
        self.url, self.model, self.dimensions = url.rstrip('/'), model, dimensions
        self.slots = BoundedSemaphore(concurrency)
        self.client = httpx.Client(timeout=httpx.Timeout(180, connect=10), trust_env=False,
                                   limits=httpx.Limits(max_connections=concurrency))

    def encode(self, texts):
        with self.slots:
            response = self.client.post(self.url + '/api/embed', json={
                'model': self.model, 'input': texts, 'truncate': False,
                'options': {'num_ctx': 2048}, 'keep_alive': '30m'})
            response.raise_for_status()
            vectors = response.json()['embeddings']
        if (len(vectors) != len(texts) or any(len(v) != self.dimensions or
                any(not math.isfinite(x) for x in v) for v in vectors)):
            raise ValueError('embedding dimensions or batch count changed')
        return vectors
