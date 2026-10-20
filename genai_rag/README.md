# GenAI Predictive Maintenance Assistant

## Overview
A retrieval-augmented generation (RAG) assistant that answers maintenance questions using a local Markdown knowledge base and optional Gemini generation.

## Architecture
1. Load maintenance documents and split them into sections.
2. Retrieve relevant sections using persistent Chroma vector search.
3. Fall back to TF-IDF when Chroma is unavailable.
4. Apply a configurable relevance threshold.
5. Generate answers with Gemini when configured.
6. Validate generated JSON using the Pydantic MaintenanceAnswer schema.
7. Return answers with evidence and source information.

## Prompt Engineering and Safety
- Few-shot examples guide the response format.
- Retrieved passages are treated as untrusted evidence.
- Unsupported questions should abstain rather than invent answers.
- Possible causes are distinguished from confirmed findings.
- Answers include recommended checks, evidence, and limitations.

## Configuration
- GEMINI_API_KEY: enables Gemini generation.
- GEMINI_MODEL: optional model override, if supported by the application.
- RAG_MIN_SIMILARITY: retrieval threshold.
- RAG_RETRIEVER=tfidf: forces TF-IDF retrieval.
- CHROMA_DB_PATH: optional persistent database path override.

## Run
From the repository root:

    streamlit run genai_rag/app.py

## Validation
Run the regression suite and RAG evaluation tests before release.
Live Gemini validation requires a valid API key, accessible model, and available quota.
