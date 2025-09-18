# Real-Time Risk Analytics Demo (SQLite + Python)

End-to-end demo for Problem Statement 3: Real-Time Risk Analytics Platform.
No Kafka; pure Python scripts + SQLite storage + Streamlit dashboard.

## What’s inside
- **SQLite DB** with tables for trades, risk_scores, alerts, counterparties, and rules
- **Synthetic data generator** that simulates streaming trades
- **Risk engine** combining rule-based checks + an IsolationForest anomaly model
- **Processor** that consumes new trades, scores risk, and raises alerts
- **Streamlit dashboard** to visualize exposures, high-risk trades, and simulate stress

## Quick start
1. Create a virtual env (optional) and install requirements
   ```bash
   pip install -r requirements.txt
   ```

2. Initialize the DB and seed metadata
   ```bash
   python db_init.py
   ```

3. (One-time / optional) Train or retrain the anomaly detection model
   ```bash
   python train_anomaly_model.py
   ```

4. Start the **generator** in one terminal
   ```bash
   python generate_data.py
   ```

5. Start the **risk processor** in a second terminal
   ```bash
   python risk_processor.py
   ```

6. Launch the **dashboard** in a third terminal
   ```bash
   streamlit run dashboard.py
   ```

> Tip: You can run generator + processor for a minute, then open the dashboard to see live updates.

## Stress testing in the dashboard
Use the sliders in Streamlit to apply a % shock to FX rates and prices. This recomputes hypothetical exposures on the fly (front-end only for demo).

## Files
- `db_init.py` – Creates tables and seeds counterparties, rules, sanctions
- `generate_data.py` – Simulates trades every second
- `train_anomaly_model.py` – Trains an IsolationForest on benign historical-like trades
- `risk_engine.py` – Contains rule-based checks and ML scoring function
- `risk_processor.py` – Polls DB for new trades, scores, and inserts alerts
- `dashboard.py` – Streamlit UI
- `data/seed_trades.csv` – Historical-like seed used both for model training and generator
- `data/sanctions_list.csv` – Example sanctions/watchlist
