import re, sqlite3, pickle, numpy as np

MODEL_PATH = "model_isoforest.pkl"
FEATURES = ["quantity","price","notional"]

def load_model():
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)

def rule_based_score(trade: dict, conn: sqlite3.Connection):
    score = 0.0
    reasons = []

    # Load active rules
    cur = conn.execute("SELECT rule_name, threshold, param FROM rules WHERE active=1")
    rules = cur.fetchall()

    for rule_name, threshold, param in rules:
        if rule_name == "MAX_NOTIONAL":
            max_notional = float(threshold)
            if float(trade["notional"]) > max_notional:
                score += 0.6
                reasons.append(f"Notional {trade['notional']} > {max_notional}")
        elif rule_name == "BLACKLIST_COUNTRY":
            pattern = param
            if re.search(pattern, str(trade["country"])):
                score += 0.8
                reasons.append(f"Blacklisted country: {trade['country']}")
        elif rule_name == "REQUIRE_KYC":
            if int(trade.get("kyc_ok",0)) != 1:
                score += 0.7
                reasons.append("KYC not verified")
        elif rule_name == "AML_FLAG_BLOCK":
            if int(trade.get("aml_flag",0)) == 1:
                score += 1.0
                reasons.append("AML system flagged")
        # sanctions name match (toy)
    # quick sanctions join
    cur2 = conn.execute("SELECT name FROM sanctions")
    sanc_names = [r[0].lower() for r in cur2.fetchall()]
    if str(trade["counterparty"]).lower() in sanc_names:
        score += 1.2
        reasons.append("Counterparty on sanctions list")

    return score, reasons

def ml_anomaly_score(trade: dict, model):
    x = np.array([[float(trade["quantity"]), float(trade["price"]), float(trade["notional"])]], dtype=float)
    # IsolationForest: decision_function -> higher is more normal; score_samples lower is more anomalous
    raw = model.score_samples(x)[0]  # negative is more anomalous
    # Convert to [0..1] where 1 = very anomalous
    # min-max like mapping with a sigmoid-ish transform
    anom = 1 / (1 + np.exp(5 * raw))  # raw usually ~[-1.0..0.2]
    return float(anom)
