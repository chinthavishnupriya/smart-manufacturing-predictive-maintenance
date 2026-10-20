# GenAI Prompt Engineering — Validation Status

## Implemented
- Evidence-grounded prompts using retrieved maintenance passages.
- Instructions to acknowledge insufficient evidence and avoid invented diagnoses.
- Safety rules against bypassing safety systems and recommending hazardous repairs.
- Retrieved passages treated as untrusted reference material.
- Gemini API error classification for rate limits, authentication/permission
  failures, request errors, server failures, timeouts, and network failures.
- Safe fallback responses that preserve retrieved source metadata.
- Empty-response handling without treating an empty answer as successful.

## Automated validation
The regression suite covers retrieval relevance, prompt requirements,
prompt-injection handling, Gemini SDK error cases, timeout handling,
credential-detail non-disclosure, and empty responses.

Run the suite with:

    PYTHONPATH=.:genai_rag python -m pytest -q

## Limitations
- Mocked tests do not prove resistance to every possible prompt injection.
- Live model responses can vary and require separate controlled validation.
- Automatic retries are not implemented.
- Retrieval similarity is a relevance signal, not proof that every generated
  claim is supported by the retrieved evidence.
- Maintenance decisions must follow approved equipment documentation and
  qualified personnel.

## Release checklist
- [x] Automated regression suite passes.
- [x] Error-classification tests pass.
- [ ] Changes pass git diff --check.
- [ ] Documentation and tests are committed and pushed.
- [ ] Live Gemini validation is recorded separately from mocked tests.

## Latest validation — 2026-10-21

- [x] Full automated regression suite: 34 tests passed.
- [x] `git diff --check` passed.
- [x] RAG retrieval evaluation: Hit@1 5/5, Hit@3 5/5,
      unsupported-query abstention 2/2.
- [ ] Commit and push the latest grounding-contract tests.
- [ ] Record any additional live Gemini validation separately.

The retrieval metrics cover five supported and two unsupported queries
against the demonstration maintenance guide. They are not production
accuracy guarantees. Prompt-contract tests do not prove that all generated
answers are grounded or immune to prompt injection.
