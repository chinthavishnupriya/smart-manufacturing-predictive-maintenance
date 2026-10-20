"""TF-IDF retrieval with Gemini-powered, evidence-grounded answers."""

import os
import re
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from prompt_engine import SYSTEM_PROMPT, build_user_prompt

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
                documents.append({
                    "source": path.name,
                    "title": section.splitlines()[0].lstrip("# ").strip(),
                    "text": section,
                })

    if not documents:
        raise RuntimeError(f"No Markdown documents found in {KNOWLEDGE_DIR}")

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
        ngram_range=(1, 2),
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
            "score": round(float(scores[index]), 4),
        })

    return results


def answer_question(query, top_k=3):
    results = retrieve(query, top_k=top_k)

    sources = [
        {
            "file": item["source"],
            "section": item["title"],
            "similarity": item["score"],
        }
        for item in results
    ]

    if not results:
        return {
            "answer": (
                "No sufficiently relevant passage was found in the knowledge "
                "base. Consult the approved equipment manual or qualified "
                "maintenance personnel."
            ),
            "sources": [],
            "mode": "retrieval_only",
        }

    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        excerpts = "\\n\\n".join(
            f"**{item['title']}**\\n{item['text']}" for item in results
        )
        return {
            "answer": (
                "Retrieved guidance (retrieval-only mode). "
                "Gemini is not configured. The following is retrieved "
                "reference text, not a generated diagnosis.\\n\\n"
                + excerpts
            ),
            "sources": sources,
            "mode": "retrieval_only",
        }

    try:
        from google import genai

        client = genai.Client(api_key=api_key)
        model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")

        response = client.models.generate_content(
            model=model,
            contents=build_user_prompt(query, results),
            config={
                "system_instruction": SYSTEM_PROMPT,
                "temperature": 0.2,
            },
        )

        answer = (response.text or "").strip()
        if answer:
            answer = "Retrieved guidance\n\n" + answer
        if not answer:
            raise RuntimeError("Gemini returned an empty answer.")

        return {
            "answer": answer,
            "sources": sources,
            "mode": "gemini_rag",
        }

    except Exception as exc:
        return {
            "answer": (
                "Gemini generation failed; no generated diagnosis was "
                "produced. Error type: "
                f"{type(exc).__name__}. Check the API configuration, model "
                "availability, quota, and network connection. Retrieved "
                "source passages are listed below."
            ),
            "sources": sources,
            "mode": "llm_error",
        }
