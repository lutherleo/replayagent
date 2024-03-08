"""A tiny offline document corpus with BM25 ranking.

The research example searches this bundled corpus instead of the web so the demo and
the tests run without network access. Ranking is Okapi BM25 (k1=1.5, b=0.75).
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Any

_TOKEN = re.compile(r"[a-z0-9]+")
_SENTENCE = re.compile(r"(?<=[.!?])\s+")
_STOPWORD_TEXT = (
    "a an and are as at be because by can do does for from has have how if in into is "
    "it its of on or so such that the their then there these this to was what when "
    "which while who why will with without"
)
_STOPWORDS = frozenset(_STOPWORD_TEXT.split())


def tokenize(text: str) -> list[str]:
    return [t for t in _TOKEN.findall(text.lower()) if t not in _STOPWORDS]


@dataclass(frozen=True)
class Document:
    doc_id: str
    title: str
    text: str

    @property
    def sentences(self) -> list[str]:
        body = " ".join(line for line in self.text.splitlines() if not line.startswith("#"))
        return [s.strip() for s in _SENTENCE.split(body) if s.strip()]


class Corpus:
    def __init__(self, documents: list[Document], *, k1: float = 1.5, b: float = 0.75) -> None:
        if not documents:
            raise ValueError("corpus is empty")
        self.documents = {d.doc_id: d for d in documents}
        self._k1, self._b = k1, b
        self._tf = {d.doc_id: Counter(tokenize(d.text)) for d in documents}
        self._len = {doc_id: sum(tf.values()) for doc_id, tf in self._tf.items()}
        self._avg_len = sum(self._len.values()) / len(self._len)
        df: Counter[str] = Counter()
        for tf in self._tf.values():
            df.update(tf.keys())
        n = len(documents)
        self._idf = {term: math.log(1 + (n - f + 0.5) / (f + 0.5)) for term, f in df.items()}

    def score(self, query_terms: list[str], doc_id: str) -> float:
        tf, length = self._tf[doc_id], self._len[doc_id]
        total = 0.0
        for term in query_terms:
            f = tf.get(term, 0)
            if not f:
                continue
            norm = f + self._k1 * (1 - self._b + self._b * length / self._avg_len)
            total += self._idf.get(term, 0.0) * f * (self._k1 + 1) / norm
        return total

    def search(self, query: str, k: int = 3) -> list[dict[str, Any]]:
        terms = tokenize(query)
        scored = [(self.score(terms, doc_id), doc_id) for doc_id in self.documents]
        ranked = sorted((s for s in scored if s[0] > 0), key=lambda s: (-s[0], s[1]))[: max(k, 0)]
        hits = []
        for score, doc_id in ranked:
            doc = self.documents[doc_id]
            hits.append(
                {
                    "doc_id": doc_id,
                    "title": doc.title,
                    "score": round(score, 4),
                    "snippet": self.snippet(doc, terms),
                }
            )
        return hits

    @staticmethod
    def snippet(doc: Document, terms: list[str]) -> str:
        wanted = set(terms)
        best, best_hits = "", -1
        for sentence in doc.sentences:
            hits = len(wanted.intersection(tokenize(sentence)))
            if hits > best_hits:
                best, best_hits = sentence, hits
        return best

    def get(self, doc_id: str) -> Document | None:
        return self.documents.get(doc_id)


@cache
def default_corpus() -> Corpus:
    """Load the bundled markdown documents (document id = file stem)."""
    root = resources.files("replayagent.examples.research").joinpath("corpus")
    docs = []
    for entry in sorted(root.iterdir(), key=lambda e: e.name):
        if not entry.name.endswith(".md"):
            continue
        text = entry.read_text(encoding="utf-8")
        first = text.splitlines()[0] if text else entry.name
        docs.append(Document(entry.name[:-3], first.lstrip("# ").strip(), text))
    return Corpus(docs)
