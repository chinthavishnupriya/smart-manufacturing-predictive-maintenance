"""Lightweight TF-IDF retrieval over local maintenance documents."""

from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

BASE_DIR = Path(__file__).resolve().parent
KNOWLEDGE_DIR = BASE_DIR / "knowledge_base"


def load_documents():
    documents = []

    for path in sorted(KNOWLEDGE_DIR.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        sections = re.split(r"(?=^## )", text, flags=re.MULTILINE)

        for section in sections:
            section = section.strip()
            if section:
                title = section.splitlines()[0].lstrip("# ").strip()
                documents.append({
                    "source": path.name,
                    "title": title,
                    "text": section
                })

    if not documents:
        raise RuntimeError(f"No knowledge documents found in {KNOWLEDGE_DIR}")

    return documents


def retrieve(query, top_k=3):
    query = query.strip()
    if not query:
        return []

    documents = load_documents()
    texts = [doc["text"] for doc in documents]

    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words="english",
        ngram_range=(1, 2)
    )
    matrix = vectorizer.fit_transform(texts)
    query_vector = vectorizer.transform([query])
    scores = cosine_similarity(query_vector, matrix).ravel()

    ranked = scores.argsort()[::-1][:top_k]
    results = []

    for index in ranked:
        if scores[index] <= 0:
            continue
        results.append({
            **documents[index],
            "score": round(float(scores[index]), 4)
        })

    return results


def answer_question(query, top_k=3):
    results = retrieve(query, top_k=top_k)

    if not results:
        return {
            "answer": (
                "No relevant passage was found in the local knowledge base. "
                "Consult the approved equipment manual or maintenance team."
            ),
            "sources": []
        }

    excerpts = "\n\n".join(
        f"**{item['title']}**\n{item['text']}"
        for item in results
    )

    return {
        "answer": (
            "Retrieved guidance is shown below. This prototype retrieves "
            "existing text; it does not independently diagnose faults or "
            "generate verified manufacturer instructions.\n\n"
            + excerpts
        ),
        "sources": [
            {
                "file": item["source"],
                "section": item["title"],
                "similarity": item["score"]
            }
            for item in results
        ]
    }
