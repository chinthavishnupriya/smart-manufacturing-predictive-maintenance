import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_api_default_model_path():
    code = """
from pathlib import Path
import os

ROOT = Path("api/main.py").resolve().parents[1]
DEFAULT_MODEL_FILE = ROOT / "models" / "predictive_maintenance_model.joblib"
MODEL_FILE = Path(os.environ.get("PREDICT_MODEL_PATH", str(DEFAULT_MODEL_FILE)))
if not MODEL_FILE.is_absolute():
    MODEL_FILE = ROOT / MODEL_FILE

assert MODEL_FILE == DEFAULT_MODEL_FILE
assert MODEL_FILE.exists()
"""
    env = os.environ.copy()
    env.pop("PREDICT_MODEL_PATH", None)
    subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, check=True)


def test_api_experimental_model_path():
    code = """
from pathlib import Path
import os

ROOT = Path("api/main.py").resolve().parents[1]
DEFAULT_MODEL_FILE = ROOT / "models" / "predictive_maintenance_model.joblib"
MODEL_FILE = Path(os.environ.get("PREDICT_MODEL_PATH", str(DEFAULT_MODEL_FILE)))
if not MODEL_FILE.is_absolute():
    MODEL_FILE = ROOT / MODEL_FILE

expected = ROOT / "models" / "predictive_maintenance_model_no_time_experimental.joblib"
assert MODEL_FILE == expected
assert MODEL_FILE.exists()
"""
    env = os.environ.copy()
    env["PREDICT_MODEL_PATH"] = (
        "models/predictive_maintenance_model_no_time_experimental.joblib"
    )
    subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, check=True)
