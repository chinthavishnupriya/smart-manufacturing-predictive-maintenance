import unittest
from unittest.mock import patch
from genai_rag.retriever import retrieve, answer_question


class RAGRetrieverTests(unittest.TestCase):
    def test_vibration_query_retrieves_relevant_section(self):
        results = retrieve("What should I check for a vibration anomaly?")
        self.assertTrue(results)
        self.assertIn("Vibration", results[0]["title"])

    def test_high_failure_probability_query(self):
        results = retrieve("failure probability above 80 percent work order")
        self.assertTrue(results)
        self.assertIn("High Predicted Failure Probability", results[0]["title"])

    def test_empty_query_returns_no_results(self):
        self.assertEqual(retrieve("   "), [])

    def test_answer_includes_source(self):
        with patch.dict("os.environ", {"GEMINI_API_KEY": ""}):
            result = answer_question("How should high machine temperature be handled?")
        self.assertTrue(result["sources"])
        self.assertIn("Retrieved guidance", result["answer"])


if __name__ == "__main__":
    unittest.main()
