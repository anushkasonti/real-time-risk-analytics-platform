import time, random, sqlite3, pandas as pd, numpy as np

DB_PATH = "risk_demo.sqlite"
SEED_DATA = "data/seed_trades.csv"

def main():
    df = pd.read_csv(SEED_DATA)
    conn = sqlite3.connect(DB_PATH, isolation_level=None)  # autocommit
    cur = conn.cursor()

    i = 0
    print("Starting trade generation. Ctrl+C to stop.")
    while True:
        r = df.sample(1, replace=True).iloc[0].to_dict()
        # add some randomness
        mult = np.clip(np.random.normal(1.0, 0.2), 0.5, 2.5)
        r["quantity"] = max(1, int(r["quantity"] * mult))
        r["price"] = round(max(0.5, r["price"] * np.clip(np.random.normal(1.0,0.1), 0.7, 1.4)), 2)
        r["notional"] = round(r["quantity"] * r["price"], 2)
        r["trade_id"] = f"SIM{int(time.time())}{random.randint(100,999)}"
        r["timestamp"] = pd.Timestamp.now().isoformat()
        r["status"] = "NEW"

        cur.execute(
            """INSERT INTO trades
            (trade_id,timestamp,counterparty,sector,country,symbol,trade_type,quantity,price,notional,currency,kyc_ok,aml_flag,status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                r["trade_id"], r["timestamp"], r["counterparty"], r["sector"], r["country"],
                r["symbol"], r["trade_type"], int(r["quantity"]), float(r["price"]),
                float(r["notional"]), r["currency"], int(r["kyc_ok"]), int(r["aml_flag"]), r["status"]
            )
        )
        print("Inserted trade", r["trade_id"], "notional", r["notional"])
        time.sleep(5.0)  # one per second

if __name__ == "__main__":
    main()
