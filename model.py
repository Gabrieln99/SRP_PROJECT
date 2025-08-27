# %%capture
# Ako nešto nedostaje u Colabu, odkomentiraj:
# !pip install scikit-learn pandas numpy

import os, re
import numpy as np
import pandas as pd
from IPython.display import display

# ============================================================
# 0) Učitavanje CSV-ova
# ============================================================
CSV_NAMES = [
    "dim_airline.csv", "dim_arr_delay.csv", "dim_date.csv", "dim_dep_delay.csv",
    "dim_route.csv", "dim_time.csv", "fact_flight.csv", "dim_aircraft.csv"
]

def existing_path(name):
    for base in ["/content", "/mnt/data", "."]:
        p = os.path.join(base, name)
        if os.path.exists(p):
            return p
    return None

missing = [n for n in CSV_NAMES if existing_path(n) is None]
if len(missing) > 0:
    print("Nedostaju datoteke:", missing)
    try:
        from google.colab import files  # type: ignore
        print("Molim vas upload-ajte tražene CSV-ove (sve odjednom):")
        uploaded = files.upload()
    except Exception:
        print("Ako ste izvan Colaba, osigurajte da su CSV-ovi u trenutni direktorij.")
else:
    print("Sve datoteke pronađene.")

def read_csv_any(name):
    for base in ["/content", "/mnt/data", "."]:
        p = os.path.join(base, name)
        if os.path.exists(p):
            return pd.read_csv(p)
    raise FileNotFoundError(f"Nisam našao {name}.")

# Standardizacija ključeva
dim_airline = read_csv_any("dim_airline.csv").rename(columns={"airline_tk":"airline_id"})
dim_arr_delay = read_csv_any("dim_arr_delay.csv").rename(columns={"arr_delay_tk":"arr_delay_id",
                                                                 "reason":"arr_reason",
                                                                 "delay_time":"arr_delay_minutes"})
dim_date = read_csv_any("dim_date.csv").rename(columns={"date_tk":"date_id"})
dim_dep_delay = read_csv_any("dim_dep_delay.csv").rename(columns={"dep_delay_tk":"dep_delay_id",
                                                                 "reason":"dep_reason",
                                                                 "delay_time":"dep_delay_minutes"})
dim_route = read_csv_any("dim_route.csv").rename(columns={"route_tk":"route_id"})
dim_time = read_csv_any("dim_time.csv").rename(columns={"time_tk":"time_id"})
fact_flight = read_csv_any("fact_flight.csv")
dim_aircraft = read_csv_any("dim_aircraft.csv").rename(columns={"aircraft_tk":"aircraft_id"})

# Dedup gdje treba
dim_airline_u = dim_airline.drop_duplicates(subset=["airline_id"], keep="first")
dim_route_u   = dim_route.drop_duplicates(subset=["route_id"], keep="first")
dim_date_u    = dim_date.drop_duplicates(subset=["date_id"], keep="first")
dim_time_u    = dim_time.drop_duplicates(subset=["time_id"], keep="first")

# ============================================================
# 1) Join + robustno dodavanje dep_hour (sat polijetanja)
# ============================================================
df = (fact_flight
      .merge(dim_airline_u, on="airline_id", how="left")
      .merge(dim_aircraft, on="aircraft_id", how="left")
      .merge(dim_route_u, on="route_id", how="left")
      .merge(dim_date_u, on="date_id", how="left")
      .merge(dim_arr_delay, on="arr_delay_id", how="left")
      .merge(dim_dep_delay, on="dep_delay_id", how="left"))

def add_dep_hour_from_dim_time(df, fact, dim_time_u):
    """
    Dodaje 'dep_hour' iz dim_time bez sudara kolona:
    - koristi pomoćni ključ '_dep_time_id'
    - radi i kad FK postoji samo u fact_flight
    """
    candidates = ["dep_time_id", "departure_time_id", "sched_dep_time_id",
                  "time_id", "dep_time_tk", "sched_dep_time_tk"]
    fk_in_df   = next((c for c in candidates if c in df.columns), None)
    fk_in_fact = next((c for c in candidates if c in fact.columns), None)
    if fk_in_df is None and fk_in_fact is None: return df
    if "time_id" not in dim_time_u.columns: return df

    t = dim_time_u.copy()
    if "hour" in t.columns:
        t2 = t[["time_id","hour"]].rename(columns={"hour":"dep_hour"})
    elif "time" in t.columns:
        tt = pd.to_datetime(t["time"], errors="coerce")
        t2 = pd.DataFrame({"time_id": t["time_id"], "dep_hour": tt.dt.hour.fillna(0).astype(int)})
    else:
        return df
    t2 = t2.rename(columns={"time_id":"_dep_time_id"})

    out = df.copy()
    if fk_in_df is not None:
        out = out.rename(columns={fk_in_df:"_dep_time_id"})
        out = out.merge(t2, on="_dep_time_id", how="left")
        out = out.rename(columns={"_dep_time_id": fk_in_df})
    else:
        out["_dep_time_id"] = fact[fk_in_fact].values
        out = out.merge(t2, on="_dep_time_id", how="left").drop(columns=["_dep_time_id"])

    out["dep_hour"] = pd.to_numeric(out["dep_hour"], errors="coerce").fillna(0).astype(int).clip(0,23)
    return out

df = add_dep_hour_from_dim_time(df, fact_flight, dim_time_u)
print("Dimenzije nakon spajanja:", df.shape)
display(df.head(3))

# ============================================================
# 2) Priprema podataka i pomoćne značajke
# ============================================================
data = df.dropna(subset=["arr_delay_minutes"]).copy()
y_raw = data["arr_delay_minutes"].astype(float)
y_cls = (y_raw >= 15).astype(int)   # labela za klasifikaciju

base_features = [
    "carrier","origin","destination",
    "departure_city","departure_country",
    "destination_city","destination_country",
    "month","day","day_of_week","quarter","is_weekend",
    # "month_name",  # duplicira 'month', izbacimo radi stabilnosti
    "scheduled_duration","distance","flight_num"
]
if "dep_hour" in data.columns:
    base_features.append("dep_hour")

X_base = data[base_features].copy()
X_base["route"] = X_base["origin"] + "_" + X_base["destination"]
X_base["carrier_route"] = X_base["carrier"] + "_" + X_base["route"]

# ============================================================
# 3) Train/test split
# ============================================================
from sklearn.model_selection import train_test_split
Xtr_base, Xte_base, ytr, yte = train_test_split(
    X_base, y_cls, test_size=0.2, random_state=42, stratify=y_cls
)

# ============================================================
# 4) Target Encoding (smoothed) za route/origin/destination (samo na train)
# ============================================================
def fit_te_map(X, y, col, m=50):
    """Smoothed mean encoding: (count*mean + m*global) / (count + m)"""
    dfm = pd.DataFrame({col: X[col].astype(str), "y": y})
    global_mean = float(dfm["y"].mean())
    g = dfm.groupby(col)["y"].agg(["count","mean"]).reset_index()
    enc = (g["count"]*g["mean"] + m*global_mean) / (g["count"] + m)
    te_map = dict(zip(g[col], enc))
    return te_map, global_mean

def apply_te(X, col, te_map, global_mean):
    return X[col].astype(str).map(te_map).fillna(global_mean)

te_cols = ["route","carrier_route","origin","destination"]
te_maps, te_globals = {}, {}
for c in te_cols:
    te_maps[c], te_globals[c] = fit_te_map(Xtr_base, ytr, c, m=50)

def add_te_columns(X):
    out = X.copy()
    for c in te_cols:
        out[f"te_{c}"] = apply_te(out, c, te_maps[c], te_globals[c])
    return out

Xtr = add_te_columns(Xtr_base)
Xte = add_te_columns(Xte_base)

# ============================================================
# 5) KLASIFIKACIJA s kalibracijom (fiksni skup kolona)
# ============================================================
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import (roc_auc_score, average_precision_score, accuracy_score,
                             precision_recall_fscore_support, confusion_matrix)
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.inspection import permutation_importance

# 1) numeric jezgra
num_core = [c for c in ["month","day","quarter","is_weekend",
                        "scheduled_duration","distance","flight_num","dep_hour"]
            if c in Xtr.columns]

# 2) target-encoding kolone (numeričke)
te_feats = [c for c in ["te_route","te_carrier_route","te_origin","te_destination"]
            if c in Xtr.columns]

# 3) nisko-kardinalne kategorije -> OneHot
cat_low = [c for c in ["carrier","day_of_week"] if c in Xtr.columns]

# 4) konačan skup za klasifikator
sel_cols_cls = num_core + te_feats + cat_low

# 5) priprema matrica
Xtr_cls = Xtr[sel_cols_cls].copy()
Xte_cls = Xte[sel_cols_cls].copy()

prep_cls = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_low),
    ("num", "passthrough", num_core + te_feats)
], remainder="drop")

base_clf = LogisticRegression(max_iter=1000, class_weight="balanced", solver="lbfgs")

clf = CalibratedClassifierCV(
    estimator=Pipeline([("prep", prep_cls), ("lr", base_clf)]),
    method="isotonic", cv=5
)
clf.fit(Xtr_cls, ytr)

p_te = clf.predict_proba(Xte_cls)[:,1]
yhat05 = (p_te >= 0.5).astype(int)
auc = roc_auc_score(yte, p_te)
ap  = average_precision_score(yte, p_te)
acc = accuracy_score(yte, yhat05)
prec, rec, f1, _ = precision_recall_fscore_support(yte, yhat05, average="binary", zero_division=0)
cm = confusion_matrix(yte, yhat05)
print(f"[KLASA] AUC: {auc:.3f}  AP: {ap:.3f}  Acc: {acc:.3f}  Prec: {prec:.3f}  Rec: {rec:.3f}  F1: {f1:.3f}")
print("Confusion matrix @0.5:\n", cm)

# prag po ciljanoj recall (npr. >=0.60), fallback na max-F1
def find_threshold(p, y_true, target_recall=0.60):
    grid = np.linspace(0.01, 0.99, 99)
    best_f1, best_t = -1.0, 0.5
    chosen = None
    for t in grid:
        yhat = (p >= t).astype(int)
        pr, rc, f1, _ = precision_recall_fscore_support(y_true, yhat, average="binary", zero_division=0)
        if f1 > best_f1: best_f1, best_t = f1, t
        if rc >= target_recall and chosen is None:
            chosen = t
    return chosen if chosen is not None else best_t

best_thresh = find_threshold(p_te, yte, target_recall=0.60)
yhat_bt = (p_te >= best_thresh).astype(int)
prec_b, rec_b, f1_b, _ = precision_recall_fscore_support(yte, yhat_bt, average="binary", zero_division=0)
print(f"Odabrani prag: {best_thresh:.3f}  -> Prec: {prec_b:.3f}  Rec: {rec_b:.3f}  F1: {f1_b:.3f}")

# važnost značajki (perm. importance na istim kolonama)
perm = permutation_importance(clf, Xte_cls, yte, n_repeats=10, random_state=42, scoring="average_precision")
imp = pd.DataFrame({"feature": Xte_cls.columns, "importance": perm.importances_mean}).sort_values("importance", ascending=False)
print("Top značajke (avg precision gain):")
display(imp.head(15))

# ============================================================
# 6) REGRESIJA (kasni-only): modelira minute iznad 15
# ============================================================
late_mask = (y_raw >= 15).values
data_reg = data.loc[late_mask].copy()
data_reg["delay_over15"] = (data_reg["arr_delay_minutes"] - 15).clip(lower=0, upper=225)
y_reg = data_reg["delay_over15"].astype(float)

reg_features = base_features[:]  # bez TE
X_reg = data_reg[reg_features].copy()

from sklearn.model_selection import train_test_split
Xr_train, Xr_test, yr_train, yr_test = train_test_split(X_reg, y_reg, test_size=0.2, random_state=42)

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score

prep_reg = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False),
        [c for c in ["carrier","origin","destination","day_of_week"] if c in X_reg.columns]),
    ("num", "passthrough",
        [c for c in ["month","day","quarter","is_weekend","scheduled_duration","distance","flight_num","dep_hour"] if c in X_reg.columns])
], remainder="drop")

reg = Pipeline([
    ("prep", prep_reg),
    ("hgb", HistGradientBoostingRegressor(learning_rate=0.06, max_iter=600, random_state=42))
])

reg.fit(Xr_train, yr_train)
pred_over = reg.predict(Xr_test)

rmse = np.sqrt(mean_squared_error(yr_test, pred_over))  # RMSE ručno
mae  = np.mean(np.abs(yr_test - pred_over))
r2   = r2_score(yr_test, pred_over)
print(f"[REG | over15] MAE: {mae:.2f}  RMSE: {rmse:.2f}  R^2: {r2:.3f}")

# ============================================================
# 7) Konačni risk i očekivano kašnjenje za sve letove
# ============================================================
# pripremi X_all s TE + istim setom kolona kao za train klase
X_all_base = X_base.copy()
X_all = add_te_columns(X_all_base)
X_all_cls = X_all[sel_cols_cls].copy()

p_all = clf.predict_proba(X_all_cls)[:,1]                  # vjerojatnost kasnjenja ≥15
pred_over_all = reg.predict(data[reg_features])            # oček. minute iznad 15
expected_delay = p_all * (15.0 + pred_over_all)            # E[delay] = p*(15 + over15)
risk_score = (p_all * 100).round(2)

rank_df = (data[["flight_tk","carrier","origin","destination","scheduled_duration","distance"] +
                 (["dep_hour"] if "dep_hour" in data.columns else [])]
           .assign(p_late_15=p_all,
                   expected_delay_min=expected_delay,
                   risk_score_0_100=risk_score))

print("Top 15 letova po vjerojatnosti kašnjenja:")
display(rank_df.sort_values("p_late_15", ascending=False).head(15))

print("Top 15 ruta po prosječnoj vjerojatnosti kašnjenja:")
top_routes_prob = (rank_df.groupby(["origin","destination"], as_index=False)
                   .agg(avg_p_late_15=("p_late_15","mean"),
                        avg_expected_delay=("expected_delay_min","mean"),
                        n=("flight_tk","count"))
                   .sort_values("avg_p_late_15", ascending=False)
                   .head(15))
display(top_routes_prob)

rank_df.to_csv("all_flight_risk.csv", index=False)
top_routes_prob.to_csv("top_routes_prob.csv", index=False)
print("CSV-ovi: all_flight_risk.csv, top_routes_prob.csv")

# ============================================================
# 8) Scoring novih letova
# ============================================================
route_lite = (dim_route_u[["origin","destination","departure_city","departure_country",
                           "destination_city","destination_country","distance"]]
              .drop_duplicates(subset=["origin","destination"], keep="first"))

def parse_dep_hour_from_new(df_new: pd.DataFrame):
    if "dep_hour" in df_new.columns:
        return pd.to_numeric(df_new["dep_hour"], errors="coerce").fillna(0).astype(int).clip(0,23)
    for col in ["dep_time", "scheduled_dep_time", "sched_dep_time"]:
        if col in df_new.columns:
            dtt = pd.to_datetime(df_new[col], errors="coerce")
            if dtt.notna().any():
                return dtt.dt.hour.fillna(0).astype(int).clip(0,23)
            c = pd.to_numeric(df_new[col], errors="coerce")
            if c.notna().any():
                return (c // 100).astype(int).clip(0,23)
    return pd.Series([0]*len(df_new), index=df_new.index)

def prepare_new_base(df_new: pd.DataFrame) -> pd.DataFrame:
    df2 = df_new.copy()
    d = pd.to_datetime(df2["date"])
    df2["month"] = d.dt.month
    df2["day"] = d.dt.day
    df2["day_of_week"] = d.dt.day_name()
    df2["quarter"] = d.dt.quarter
    df2["is_weekend"] = (d.dt.dayofweek >= 5).astype(int)
    df2 = df2.merge(route_lite, on=["origin","destination"], how="left")
    if "flight_num" not in df2.columns:
        df2["flight_num"] = 0
    if "dep_hour" in base_features:
        df2["dep_hour"] = parse_dep_hour_from_new(df2)
    df2["route"] = df2["origin"] + "_" + df2["destination"]
    df2["carrier_route"] = df2["carrier"] + "_" + df2["route"]
    return df2

def score_new_flights(df_new: pd.DataFrame, threshold=None):
    base = prepare_new_base(df_new)

    # TE kolone (primjena treniranih mapa)
    base_te = base.copy()
    for c in te_cols:
        base_te[f"te_{c}"] = apply_te(base_te, c, te_maps[c], te_globals[c])

    # koristi identičan skup kolona kao u treningu klasifikatora
    base_te_cls = base_te[sel_cols_cls].copy()
    p = clf.predict_proba(base_te_cls)[:,1]

    # regresija -> over15
    pred_over = reg.predict(base[reg_features])
    expected = p * (15.0 + pred_over)

    if threshold is None:
        threshold = float(best_thresh)
    out = df_new.copy()
    out["p_late_15"] = p
    out["risk_score_0_100"] = (p*100).round(2)
    out["expected_delay_min"] = expected
    out["late_15_flag"] = (p >= threshold).astype(int)
    return out

# Primjer
example_new = pd.DataFrame({
    "carrier":["AA","UA","B6"],
    "origin":["EWR","EWR","EWR"],
    "destination":["ATL","BOS","BDL"],
    "scheduled_duration":[150, 115, 65],
    "date":["2025-09-15","2025-09-15","2025-09-15"],
    "flight_num":[100, 200, 300],
    "dep_time":["08:30","17:45","22:10"]  # ili dep_hour npr. 8, 17, 22
})
display(score_new_flights(example_new))

# ============================================================
# 9) Spremanje artefakata
# ============================================================
import joblib, json
ARTIFACT_DIR = "./artifacts"; os.makedirs(ARTIFACT_DIR, exist_ok=True)

joblib.dump(clf, os.path.join(ARTIFACT_DIR, "late15_classifier_calibrated.joblib"))
joblib.dump(reg, os.path.join(ARTIFACT_DIR, "late_over15_regressor.joblib"))
joblib.dump(
    {"te_maps": te_maps, "te_globals": te_globals, "sel_cols_cls": sel_cols_cls,
     "reg_features": reg_features, "best_thresh": best_thresh},
    os.path.join(ARTIFACT_DIR, "te_and_meta.joblib")
)
print("Artefakti spremljeni u ./artifacts")
