# REAL-TIME RISK ANALYTICS (SQLite + Python)

*Project Overview:* Financial institutions process thousands of trades every day, facing risks such as fraud, money laundering, limit breaches, and sanctions violations. Manual monitoring is slow, error-prone, and costly. To address this, we built a real-time risk analytics system that combines rules with an ML anomaly detection model (Isolation Forest), classifies trades as ALLOW, REVIEW, or BLOCK, and presents results through an interactive dashboard with executive summaries, multi-currency exposure views, stress test simulations, and audit-ready logs.

## What’s inside
- **SQLite DB** with tables for trades, risk_scores, alerts, counterparties, and rules
- **Synthetic data generator** that simulates streaming trades
- **Risk engine** combining rule-based checks + an IsolationForest anomaly model
- **Processor** that consumes new trades, scores risk, and raises alerts
- **Streamlit dashboard** to visualize exposures, high-risk trades, and simulate stress

## How to run?
1. Install requirements
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

## Files
- `db_init.py` – Creates tables and seeds counterparties, rules, sanctions
- `generate_data.py` – Simulates trades every second
- `train_anomaly_model.py` – Trains an IsolationForest on benign historical-like trades
- `risk_engine.py` – Contains rule-based checks and ML scoring function
- `risk_processor.py` – Polls DB for new trades, scores, and inserts alerts
- `dashboard.py` – Streamlit UI
- `data/seed_trades.csv` – Historical-like seed used both for model training and generator
- `data/sanctions_list.csv` – Example sanctions/watchlist
