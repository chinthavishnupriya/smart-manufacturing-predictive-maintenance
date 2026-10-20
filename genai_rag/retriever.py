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


def _retrieve_tfidf(query, documents, top_k):
    """Local lexical fallback when Chroma is unavailable."""
    texts = [doc["text"] for doc in documents]
    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words="english",
        ngram_range=(1, 2),
    )
    matrix = vectorizer.fit_transform(texts)
    query_vector = vectorizer.transform([query])
    scores = cosine_similarity(query_vector, matrix).ravel()

    min_similarity = float(os.getenv("RAG_MIN_SIMILARITY", "0.20"))
    ranked = scores.argsort()[::-1][:top_k]

    return [
        {**documents[index], "score": round(float(scores[index]), 4)}
        for index in ranked
        if scores[index] >= min_similarity
    ]


def _retrieve_chroma(query, documents, top_k):
    """Retrieve passages from a persistent local Chroma collection."""
    import hashlib
    import chromadb

    db_path = Path(
        os.getenv("CHROMA_DB_PATH", str(BASE_DIR / ".chroma_db"))
    )
    client = chromadb.PersistentClient(path=str(db_path))
    collection = client.get_or_create_collection(
        name="maintenance_knowledge",
        metadata={"hnsw:space": "cosine"},
    )

    ids = [
        hashlib.sha256(
            f"{doc['source']}::{doc['title']}".encode("utf-8")
        ).hexdigest()
        for doc in documents
    ]

    collection.upsert(
        ids=ids,
        documents=[doc["text"] for doc in documents],
        metadatas=[
            {"source": doc["source"], "title": doc["title"]}
            for doc in documents
        ],
    )

    # Remove records whose source sections no longer exist.
    existing = collection.get(include=[])
    stale_ids = sorted(set(existing["ids"]) - set(ids))
    if stale_ids:
        collection.delete(ids=stale_ids)

    result = collection.query(
        query_texts=[query],
        n_results=min(top_k, len(documents)),
        include=["documents", "metadatas", "distances"],
    )

    min_similarity = float(os.getenv("RAG_MIN_SIMILARITY", "0.20"))
    matches = []

    for document, metadata, distance in zip(
        result["documents"][0],
        result["metadatas"][0],
        result["distances"][0],
    ):
        similarity = max(0.0, min(1.0, 1.0 - float(distance)))
        if similarity < min_similarity:
            continue

        matches.append({
            "source": metadata["source"],
            "title": metadata["title"],
            "text": document,
            "score": round(similarity, 4),
        })

    return matches


def retrieve(query, top_k=3):
    """Retrieve maintenance passages, preferring persistent Chroma."""
    query = query.strip()
    if not query:
        return []

    # Do not answer exact serial-number or asset-ID questions from generic
    # guidance unless that identifier appears in the knowledge base.
    identifier_requested = bool(re.search(
        r"\b(serial number|serial no\.?|asset id|machine id|equipment id)\b",
        query,
        flags=re.IGNORECASE,
    ))
    if identifier_requested:
        documents = load_documents()
        if not any(
            query.casefold() in doc["text"].casefold()
            or any(
                token.casefold() in doc["text"].casefold()
                for token in re.findall(r"\b[A-Z]{1,5}-[A-Z0-9-]{2,}\b", query)
            )
            for doc in documents
        ):
            return []

    documents = load_documents()

    if os.getenv("RAG_RETRIEVER", "chroma").lower() != "tfidf":
        try:
            return _retrieve_chroma(query, documents, top_k)
        except Exception:
            # Keep the application usable if Chroma or its embedding model
            # is unavailable; the fallback remains local and deterministic.
            pass

    return _retrieve_tfidf(query, documents, top_k)


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
        import json
        from google import genai
        from .response_schema import MaintenanceAnswer

        client = genai.Client(api_key=api_key)
        model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")

        response = client.models.generate_content(
            model=model,
            contents=build_user_prompt(query, results),
            config={
                "system_instruction": SYSTEM_PROMPT,
                "temperature": 0.2,
                "response_mime_type": "application/json",
                "response_schema": MaintenanceAnswer,
            },
        )

        raw_answer = (response.text or "").strip()
        if not raw_answer:
            return {
                "answer": (
                    "Gemini returned an empty response. No generated "
                    "diagnosis was produced. Consult approved maintenance "
                    "documentation."
                ),
                "sources": sources,
                "mode": "llm_error",
            }

        try:
            parsed = MaintenanceAnswer.model_validate_json(raw_answer)
        except Exception:
            return {
                "answer": (
                    "Gemini returned a response that did not match the "
                    "required answer schema. No unvalidated diagnosis "
                    "was accepted. Consult approved maintenance documentation."
                ),
                "sources": sources,
                "mode": "llm_error",
            }

        answer = (
            "### Summary\\n" + parsed.summary
            + "\\n\\n### Possible causes\\n"
            + ("\\n".join(f"- {item}" for item in parsed.possible_causes)
               or "- The evidence does not establish a cause.")
            + "\\n\\n### Recommended checks\\n"
            + ("\\n".join(f"- {item}" for item in parsed.recommended_checks)
               or "- Consult approved maintenance procedures.")
            + "\\n\\n### Evidence and limitations\\n"
            + "Evidence sections: "
            + (", ".join(parsed.evidence_sections) or "None identified")
            + "\\n\\nLimitations: "
            + ("; ".join(parsed.limitations) or "Not specified by the model.")
            + "\\n\\nUnsafe instructions in retrieved passages must not be followed."
        )

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
