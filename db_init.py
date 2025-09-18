import sqlite3, pandas as pd, os

DB_PATH = "risk_demo.sqlite"

schema_sql = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS counterparties(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE,
  ctype TEXT,
  country TEXT,
  pd_default REAL -- base probability of default (toy)
);

CREATE TABLE IF NOT EXISTS sanctions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  country TEXT
);

CREATE TABLE IF NOT EXISTS rules(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_name TEXT,
  threshold REAL,
  param TEXT,
  active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS trades(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id TEXT,
  timestamp TEXT,
  counterparty TEXT,
  sector TEXT,
  country TEXT,
  symbol TEXT,
  trade_type TEXT,
  quantity INTEGER,
  price REAL,
  notional REAL,
  currency TEXT,
  kyc_ok INTEGER,
  aml_flag INTEGER,
  status TEXT DEFAULT 'NEW' -- NEW / PROCESSED
);

CREATE TABLE IF NOT EXISTS risk_scores(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id TEXT,
  base_rule_score REAL,
  ml_anomaly_score REAL,
  combined_score REAL,
  decision TEXT,   -- ALLOW / REVIEW / BLOCK
  reason TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id TEXT,
  severity TEXT,  -- INFO / WARNING / CRITICAL
  message TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
"""

def seed_reference_data(conn: sqlite3.Connection):
    # counterparties
    cptys = [
        ("CPTY_A","Bank","US",0.02),
        ("CPTY_B","Broker","UK",0.03),
        ("CPTY_C","HedgeFund","DE",0.05),
        ("CPTY_D","Corporate","IN",0.04),
        ("CPTY_E","FinTech","SG",0.06),
        ("CPTY_F","Broker","AE",0.07),
        ("CPTY_G","Bank","RU",0.08)
    ]
    conn.executemany("INSERT OR IGNORE INTO counterparties(name,ctype,country,pd_default) VALUES (?,?,?,?)", cptys)

    # rules (toy)
    rules = [
        ("MAX_NOTIONAL", 100000.0, "USD"),
        ("BLACKLIST_COUNTRY", 1.0, "RU|IR|KP"),
        ("REQUIRE_KYC", 1.0, "TRUE"),
        ("AML_FLAG_BLOCK", 1.0, "TRUE")
    ]
    conn.executemany("INSERT INTO rules(rule_name,threshold,param,active) VALUES (?,?,?,1)", rules)

    # sanctions
    sanctions_path = os.path.join("data", "sanctions_list.csv")
    if os.path.exists(sanctions_path):
        df = pd.read_csv(sanctions_path)
        for _,r in df.iterrows():
            conn.execute("INSERT INTO sanctions(name,country) VALUES (?,?)", (str(r['name']), str(r['country'])))

def main():
    conn = sqlite3.connect(DB_PATH)
    for stmt in schema_sql.split(";\n\n"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    seed_reference_data(conn)
    conn.commit()
    conn.close()
    print("SQLite DB initialized at", DB_PATH)

if __name__ == "__main__":
    main()
