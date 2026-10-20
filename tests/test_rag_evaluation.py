import json
from pathlib import Path

from genai_rag.retriever import retrieve


CASES_FILE = Path(__file__).with_name("rag_evaluation_cases.json")


def load_cases():
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))


def test_supported_queries_retrieve_expected_sections():
    cases = [case for case in load_cases() if case["expected_retrieval"]]

    for case in cases:
        results = retrieve(case["query"], top_k=3)
        sections = [result["title"] for result in results]

        assert case["expected_section"] in sections, (
            f'{case["id"]}: expected section '
            f'{case["expected_section"]!r}; retrieved {sections!r}'
        )


def test_unsupported_queries_return_no_evidence():
    cases = [case for case in load_cases() if not case["expected_retrieval"]]

    for case in cases:
        results = retrieve(case["query"], top_k=3)

        assert results == [], (
            f'{case["id"]}: expected no results, got '
            f'{[result["title"] for result in results]!r}'
        )
