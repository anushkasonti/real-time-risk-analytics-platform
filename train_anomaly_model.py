import pandas as pd, numpy as np, pickle
from sklearn.ensemble import IsolationForest

SEED_DATA = "data/seed_trades.csv"
MODEL_PATH = "model_isoforest.pkl"

# Features to train on (drop identifiers/text fields)
FEATURES = ["quantity","price","notional"]

def main():
    df = pd.read_csv(SEED_DATA)
    # Keep only rows that look "benign" (kyc_ok = 1 and aml_flag = 0) for unsupervised baseline
    benign = df[(df["kyc_ok"]==1) & (df["aml_flag"]==0)]
    X = benign[FEATURES].copy()

    model = IsolationForest(n_estimators=200, contamination=0.02, random_state=42)
    model.fit(X)

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    print(f"Model trained on {len(X)} rows and saved to {MODEL_PATH}")

if __name__ == "__main__":
    main()
