import importlib
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]

VALID_PAYLOAD = {
    "machine_id": "M001",
    "timestamp": "2026-10-22 23:00:00",
    "temperature": 74.26,
    "vibration": 3.838,
    "rpm": 1371.31,
    "pressure": 3.496,
    "operating_hours": 514.12,
}


@pytest.fixture
def api_module(monkeypatch):
    """Load the API with its default model configuration."""
    monkeypatch.delenv("PREDICT_MODEL_PATH", raising=False)

    import api.main as api

    api = importlib.reload(api)
    api._model_bundle = None

    yield api

    api._model_bundle = None


def test_predict_endpoint_returns_expected_response(api_module):
    with TestClient(api_module.app) as client:
        response = client.post("/predict", json=VALID_PAYLOAD)

    assert response.status_code == 200
    result = response.json()

    assert result["machine_id"] == "M001"
    assert 0 <= result["failure_probability"] <= 1
    assert result["failure_prediction"] in (0, 1)
    assert result["failure_status"] in ("NORMAL", "FAILURE RISK")
    assert result["failure_threshold"] == 0.5
    assert result["anomaly_status"] in ("NORMAL", "ANOMALY")
    assert isinstance(result["detected_anomalies"], list)


@pytest.mark.parametrize(
    "changes",
    [
        {"temperature": None},
        {"vibration": -1},
        {"rpm": -1},
        {"operating_hours": -1},
    ],
)
def test_predict_endpoint_rejects_invalid_payload(api_module, changes):
    payload = {**VALID_PAYLOAD, **changes}

    with TestClient(api_module.app) as client:
        response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_predict_endpoint_rejects_missing_required_field(api_module):
    payload = {
        key: value
        for key, value in VALID_PAYLOAD.items()
        if key != "temperature"
    }

    with TestClient(api_module.app) as client:
        response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_predict_endpoint_returns_503_when_model_missing(
    api_module, monkeypatch, tmp_path
):
    missing_model = tmp_path / "missing-model.joblib"
    monkeypatch.setattr(api_module, "MODEL_FILE", missing_model)
    api_module._model_bundle = None

    with TestClient(api_module.app) as client:
        response = client.post("/predict", json=VALID_PAYLOAD)

    assert response.status_code == 503
    assert "Model not found" in response.json()["detail"]


def test_predict_endpoint_can_select_experimental_model(monkeypatch):
    monkeypatch.setenv(
        "PREDICT_MODEL_PATH",
        "models/predictive_maintenance_model_no_time_experimental.joblib",
    )

    import api.main as api

    api = importlib.reload(api)
    api._model_bundle = None

    try:
        assert api.MODEL_FILE == (
            ROOT / "models" / "predictive_maintenance_model_no_time_experimental.joblib"
        )

        with TestClient(api.app) as client:
            response = client.post("/predict", json=VALID_PAYLOAD)

        assert response.status_code == 200
        assert response.json()["machine_id"] == "M001"
    finally:
        api._model_bundle = None
        monkeypatch.delenv("PREDICT_MODEL_PATH", raising=False)
        importlib.reload(api)
        api._model_bundle = None
