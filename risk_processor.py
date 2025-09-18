# risk_processor.py
# ------------------------------------------------------------------
# Polls NEW trades, scores them (rules + ML), writes to DB, logs alerts.
# - Translates "reasons" to banker-friendly phrases
# - Auto-detects risk_scores schema:
#     A) rule_score, ml_score, combined_score, decision, severity, reasons
#     B) base_rule_score, ml_anomaly_score, combined_score, decision, reason
# - Auto-detects alerts schema:
#     A) ... level ...
#     B) ... severity ...
# ------------------------------------------------------------------

import time
import sqlite3
import warnings
from typing import Dict, List, Tuple

from risk_engine import load_model, rule_based_score, ml_anomaly_score

DB_PATH = "risk_demo.sqlite"

# Cosmetic: hide IsolationForest "feature names" warning
warnings.filterwarnings(
    "ignore",
    message="X does not have valid feature names, but IsolationForest was fitted with feature names",
)

# ---------- generic helpers ----------
def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]  # column names

def _decide(combined_score: float) -> Tuple[str, str]:
    if combined_score >= 1.5:
        return "BLOCK", "CRITICAL"
    elif combined_score >= 0.9:
        return "REVIEW", "WARNING"
    else:
        return "ALLOW", "INFO"

def _translate_reasons(raw_reasons: List[str]) -> List[str]:
    TRANSLATE = {
        "Notional {val} > {thr}": "Deal is bigger than our limit",
        "Country in blacklist": "Counterparty country is restricted",
        "KYC missing": "KYC not completed",
        "AML flag present": "AML system has red flags",
        "Sanctions name match": "Possible sanctions list match",
    }
    pretty: List[str] = []
    for r in (raw_reasons or []):
        matched = False
        for k, v in TRANSLATE.items():
            prefix = k.split("{")[0].strip()
            if r.startswith(prefix):
                pretty.append(v)
                matched = True
                break
        if not matched:
            pretty.append(r)  # keep unknown reason as-is (e.g., "Blacklisted country: RU", "KYC not verified")
    return pretty

# ---------- DB I/O ----------
def _fetch_new_trades(conn: sqlite3.Connection, batch: int = 20) -> List[Dict]:
    cur = conn.execute(
        """
        SELECT id, trade_id, timestamp, counterparty, sector, country, symbol, trade_type,
               quantity, price, notional, currency, kyc_ok, aml_flag
        FROM trades
        WHERE status='NEW'
        ORDER BY id
        LIMIT ?
        """,
        (batch,),
    )
    cols = [
        "id","trade_id","timestamp","counterparty","sector","country","symbol","trade_type",
        "quantity","price","notional","currency","kyc_ok","aml_flag"
    ]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def _persist_scores_auto(
    conn: sqlite3.Connection,
    *,
    trade_id: str,
    rule_score: float,
    ml_score: float,
    combined: float,
    decision: str,
    severity: str,
    reasons_str: str,
) -> None:
    cols = set(_table_columns(conn, "risk_scores"))

    if {"rule_score", "ml_score", "combined_score", "decision", "severity", "reasons"}.issubset(cols):
        # Newer schema
        conn.execute(
            """
            INSERT INTO risk_scores(
                trade_id, rule_score, ml_score, combined_score, decision, severity, reasons
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (trade_id, rule_score, ml_score, combined, decision, severity, reasons_str),
        )
    elif {"base_rule_score", "ml_anomaly_score", "combined_score", "decision", "reason"}.issubset(cols):
        # Older schema
        conn.execute(
            """
            INSERT INTO risk_scores(
                trade_id, base_rule_score, ml_anomaly_score, combined_score, decision, reason
            ) VALUES (?,?,?,?,?,?)
            """,
            (trade_id, rule_score, ml_score, combined, decision, reasons_str),
        )
    else:
        # Minimal fallback (last resort)
        conn.execute(
            """
            INSERT INTO risk_scores(trade_id, combined_score, decision)
            VALUES (?,?,?)
            """,
            (trade_id, combined, decision),
        )

def _insert_alert_auto(conn: sqlite3.Connection, trade_id: str, severity_text: str, message: str) -> None:
    cols = set(_table_columns(conn, "alerts"))

    if {"trade_id", "level", "message"}.issubset(cols):
        conn.execute("INSERT INTO alerts(trade_id, level, message) VALUES (?,?,?)",
                     (trade_id, severity_text, message))
    elif {"trade_id", "severity", "message"}.issubset(cols):
        conn.execute("INSERT INTO alerts(trade_id, severity, message) VALUES (?,?,?)",
                     (trade_id, severity_text, message))
    elif {"trade_id", "message"}.issubset(cols):
        # Last resort: no severity column present
        conn.execute("INSERT INTO alerts(trade_id, message) VALUES (?,?)",
                     (trade_id, message))
    else:
        # If schema is unexpected, create a lightweight entry in risk_scores as a log instead
        conn.execute(
            "INSERT INTO risk_scores(trade_id, combined_score, decision) VALUES (?,?,?)",
            (trade_id, 0.0, f"ALERT_FALLBACK:{severity_text}")
        )

# ---------- main processing ----------
def process_once(conn: sqlite3.Connection, model) -> int:
    rows = _fetch_new_trades(conn)
    if not rows:
        return 0

    for tr in rows:
        # 1) Scores
        rule_s, raw_reasons = rule_based_score(tr, conn)
        ml_s = ml_anomaly_score(tr, model)
        combined = float(rule_s) + float(ml_s)

        # 2) Decision
        decision, severity = _decide(combined)

        # 3) Translate reasons
        reasons = _translate_reasons(raw_reasons)
        reasons_str = "; ".join(reasons) if reasons else "No rule violations"

        # 4) Persist scores (schema-aware)
        _persist_scores_auto(
            conn,
            trade_id=tr["trade_id"],
            rule_score=float(rule_s),
            ml_score=float(ml_s),
            combined=combined,
            decision=decision,
            severity=severity,
            reasons_str=reasons_str,
        )

        # 5) Mark processed
        conn.execute("UPDATE trades SET status='PROCESSED' WHERE id=?", (tr["id"],))

        # 6) Insert alert (schema-aware)
        msg = f"{decision} trade {tr['trade_id']} score={combined:.2f} reasons: {reasons_str}"
        _insert_alert_auto(conn, tr["trade_id"], severity_text=severity, message=msg)
        print(msg)

    conn.commit()
    return len(rows)

def main() -> None:
    model = load_model()
    conn = sqlite3.connect(DB_PATH, isolation_level=None)  # autocommit-like
    print("Risk processor started. Polling every 2s. Ctrl+C to stop.")
    try:
        while True:
            n = process_once(conn, model)
            if n == 0:
                time.sleep(2.0)
    except KeyboardInterrupt:
        print("Stopping processor.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
