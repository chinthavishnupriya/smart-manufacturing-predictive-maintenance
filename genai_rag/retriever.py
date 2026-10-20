"""TF-IDF retrieval with Gemini-powered, evidence-grounded answers."""

import os
import re
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from .prompt_engine import SYSTEM_PROMPT, build_user_prompt
except ImportError:
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
    min_similarity = float(os.getenv("RAG_MIN_SIMILARITY", "0.20"))

    for index in ranked:
        if scores[index] < min_similarity:
            continue

        results.append({
            **documents[index],
            "score": round(float(scores[index]), 4),
        })

    return results



def _generation_error_message(exc):
    """Classify Gemini failures without exposing exception details."""
    try:
        from google.genai import errors as gemini_errors
    except ImportError:
        gemini_errors = None

    if gemini_errors is not None:
        if isinstance(exc, gemini_errors.ServerError):
            detail = "Gemini's server is temporarily unavailable. Try again later."
        elif isinstance(exc, gemini_errors.ClientError):
            code = getattr(exc, "code", None)
            if code == 429:
                detail = "Gemini rate limit or quota reached. Check quota and retry later."
            elif code in (401, 403):
                detail = "Gemini authentication or permission failed. Check API credentials and access."
            elif code == 400:
                detail = "Gemini rejected the request. Check the model and request configuration."
            else:
                detail = "Gemini rejected the request. Check API configuration and request settings."
        elif isinstance(exc, gemini_errors.APIError):
            detail = "Gemini returned an API error. Check configuration and try again later."
        else:
            detail = None
    else:
        detail = None

    if detail is None:
        try:
            import httpx

            if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
                detail = "The request to Gemini timed out. Check the connection and try again."
            elif isinstance(exc, (httpx.NetworkError, ConnectionError)):
                detail = "Could not connect to Gemini. Check the network connection and try again."
        except ImportError:
            if isinstance(exc, (TimeoutError, ConnectionError)):
                detail = "Could not reach Gemini. Check the connection and try again."

    if detail is None:
        detail = (
            "Check the API configuration, model availability, quota, "
            "and network connection."
        )

    return f"Gemini generation failed: {detail}"


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
        if not answer:
            return {
                "answer": (
                    "Gemini returned an empty response. No generated "
                    "diagnosis was produced. Try again later or consult "
                    "approved maintenance documentation."
                ),
                "sources": sources,
                "mode": "llm_error",
            }

        return {
            "answer": "Retrieved guidance\\n\\n" + answer,
            "sources": sources,
            "mode": "gemini_rag",
        }

    except Exception as exc:
        return {
            "answer": (
                _generation_error_message(exc)
                + " No generated diagnosis was produced. "
                "Retrieved source passages are listed below."
            ),
            "sources": sources,
            "mode": "llm_error",
        }
