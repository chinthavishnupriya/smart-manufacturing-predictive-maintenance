"""Make a prediction for one sensor reading from the command line."""
from pathlib import Path
import argparse, joblib, pandas as pd, sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
from data_processing.feature_engineering import build_features
BUNDLE=joblib.load(ROOT/"models"/"predictive_maintenance_model.joblib")
def predict(payload):
    row=pd.DataFrame([payload]); x=build_features(row)[BUNDLE["features"]]
    p=float(BUNDLE["model"].predict_proba(x)[0,1]); return {"failure_probability":round(p,4),"prediction":int(p>=BUNDLE["threshold"]),"status":"FAILURE RISK" if p>=BUNDLE["threshold"] else "NORMAL"}
if __name__=="__main__":
    a=argparse.ArgumentParser(); a.add_argument("--machine-id",default="M001"); a.add_argument("--timestamp",default="2026-01-01 12:00:00"); a.add_argument("--temperature",type=float,default=65); a.add_argument("--vibration",type=float,default=1.8); a.add_argument("--rpm",type=float,default=1500); a.add_argument("--pressure",type=float,default=4.2); a.add_argument("--operating-hours",type=float,default=1000); q=a.parse_args()
    print(predict(vars(q)))
