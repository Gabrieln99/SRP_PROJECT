# -- coding: utf-8 -- 
"""
Ovo je model za predikciju kašnjenja letova koji iz povijesnih podataka procjenjuje hoće li let kasniti više od 15 minuta (DOT prag).
 Uči obrasce iz značajki poput mjeseca, dana u tjednu, sata polaska, prijevoznika, rute/udaljenosti i razloga kašnjenja 
 te daje vjerojatnost i konačnu klasu (“kasnio”/“na vrijeme”). Prag odluke optimiziran je F2 mjerom kako bi se naglasio 
 veći odziv (uhvatiti što više stvarnih kašnjenja), uz prikaz matrica zabune i ključnih grafova za razumijevanje performansi.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report, precision_recall_curve,
    confusion_matrix, roc_auc_score, average_precision_score,
    fbeta_score  # za F2 @ 0.5
)
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

# pomoć: gradijent paleta + barplot
def _palette_from_cmap(cmap_name: str, n: int):
    cmap = plt.get_cmap(cmap_name)
    if n <= 1: return [cmap(0.5)]
    return [cmap(i / (n - 1)) for i in range(n)]

def barplot_gradient(data: pd.DataFrame, x: str, y: str, hue: str,
                     cmap: str = "viridis", order=None, hue_order=None,
                     rotate_xticks: bool = False, title: str = "",
                     xlabel: str = "", ylabel: str = ""):
    if hue_order is None:
        hue_order = list(data[hue].astype(str).unique())
    pal = {val: col for val, col in zip(hue_order, _palette_from_cmap(cmap, len(hue_order)))}
    ax = sns.barplot(
        data=data, x=x, y=y, hue=hue,
        order=order, hue_order=hue_order,
        palette=pal, dodge=False, legend=False
    )
    if rotate_xticks: plt.xticks(rotation=45, ha="right")
    if title: plt.title(title)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.tight_layout(); plt.show()
    return ax

# pomoć: heatmap matrice zabune s anotacijama
def plot_cm_heatmap(y_true, y_pred, labels=("Na vrijeme", "Kasnio"), title="Confusion matrix"):
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(6.5, 5.5))
    ax = sns.heatmap(cm, annot=True, fmt="d", cmap="viridis", cbar=True,
                     xticklabels=labels, yticklabels=labels)
    ax.set_xlabel("Predicted label"); ax.set_ylabel("True label"); ax.set_title(title)
    plt.tight_layout(); plt.show()

# Konfiguracija
PATH = "flights_main.csv"
THRESHOLD_MINUTES = 15
TEST_SIZE = 0.20
RANDOM_STATE = 42
BEST_THRESHOLD = 0.5

# Učitavanje podataka
usecols = ["month","day_of_week","sched_dep_time","reason_dep_delay",
           "carrier","origin","destination","distance","arr_delay_time"]
df = pd.read_csv(PATH, usecols=usecols)

# Feature engineering
def parse_time(val):
    try:
        if isinstance(val, str) and ":" in val:
            h, m = val.split(":"); return int(h)*60 + int(m)
        val = str(int(val)).zfill(4); return int(val[:2])*60 + int(val[2:])
    except Exception:
        return np.nan

df["sched_dep_time"] = df["sched_dep_time"].apply(parse_time)
df["sched_dep_hour"] = df["sched_dep_time"] // 60

def is_weekend_val(x):
    if isinstance(x, str): return 1 if x.lower() in {"sat","saturday","sun","sunday"} else 0
    return 1 if x in [6,7,"6","7"] else 0

df["is_weekend"] = df["day_of_week"].apply(is_weekend_val).astype(int)

def distance_cat(dist):
    if pd.isna(dist): return "unknown"
    if dist < 500: return "short"
    elif dist < 1500: return "medium"
    else: return "long"

df["route_distance_cat"] = df["distance"].apply(distance_cat)

# Target: kasni > 15 min
df["delayed"] = (pd.to_numeric(df["arr_delay_time"], errors="coerce") > THRESHOLD_MINUTES).astype(int)

# Label encoding
cat_cols = ["reason_dep_delay","carrier","origin","destination","day_of_week","route_distance_cat"]
le_dict = {}
for col in cat_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    le_dict[col] = le

# Značajke + target
features = ["month","day_of_week","sched_dep_hour","reason_dep_delay",
            "carrier","origin","destination","distance","is_weekend","route_distance_cat"]
X = df[features]
y = df["delayed"]

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
)

# Model
rf = RandomForestClassifier(
    n_estimators=300, max_depth=None, min_samples_leaf=2,
    class_weight="balanced_subsample", n_jobs=-1, random_state=RANDOM_STATE
)
rf.fit(X_train, y_train)

# Procjena @0.5
y_proba = rf.predict_proba(X_test)[:, 1]
y_pred_05 = (y_proba >= 0.5).astype(int)
print("\n--- Report @0.5 ---")
print(classification_report(y_test, y_pred_05, zero_division=0, target_names=["na_vrijeme","kasnio"]))
plot_cm_heatmap(y_test, y_pred_05, title="Confusion matrix — RF @0.5")

# Tuning praga: F2 (preciznost–odziv → F2)
precision, recall, thresholds = precision_recall_curve(y_test, y_proba)
beta = 2.0
beta2 = beta**2
f2 = (1 + beta2) * (precision[:-1] * recall[:-1]) / (beta2 * precision[:-1] + recall[:-1] + 1e-12)
best_idx = int(np.nanargmax(f2))
BEST_THRESHOLD = float(thresholds[best_idx])
y_pred_best = (y_proba >= BEST_THRESHOLD).astype(int)

# Procjena @F2-opt
print(f"\nOdabrani prag (F2): {BEST_THRESHOLD:.3f}")
print("\n--- Report @F2-opt ---")
print(classification_report(y_test, y_pred_best, zero_division=0, target_names=["na_vrijeme","kasnio"]))
plot_cm_heatmap(y_test, y_pred_best, title="Confusion matrix — RF @F2-opt")

# Dodatne metrike
print("ROC AUC:", roc_auc_score(y_test, y_proba))
print("PR  AUC:", average_precision_score(y_test, y_proba))

# Krivulja F2 (F2 vs. threshold) + oznake točaka @0.5 i @F2-opt
f2_at_05 = fbeta_score(y_test, y_pred_05, beta=2)                 # F2 točno pri 0.5
f2_best = float(np.nanmax(f2))                                    # maksimum na krivulji
plt.figure(figsize=(8,5))
plt.plot(thresholds, f2, lw=2)
plt.axvline(0.5, color="gray", ls=":", label="threshold=0.50")
plt.axvline(BEST_THRESHOLD, color="purple", ls="--", label=f"F2-opt={BEST_THRESHOLD:.3f}")
plt.scatter([0.5], [f2_at_05], color="gray", zorder=5)
plt.scatter([BEST_THRESHOLD], [f2_best], color="purple", zorder=5)
plt.title("Krivulja F2 u funkciji praga")
plt.xlabel("Prag odluke"); plt.ylabel("F2")
plt.legend()
plt.tight_layout(); plt.show()

# PR krivulja s označenim točkama rada
idx_05 = int(np.argmin(np.abs(thresholds - 0.5)))                 # najbliži indeks za 0.5
plt.figure(figsize=(8,5))
plt.plot(recall, precision, lw=2)
plt.scatter([recall[idx_05]], [precision[idx_05]], color="gray", label="@0.50", zorder=5)
plt.scatter([recall[best_idx]], [precision[best_idx]], color="purple", label=f"@F2-opt={BEST_THRESHOLD:.3f}", zorder=5)
plt.title("Preciznost–odziv (s točkama @0.5 i @F2-opt)")
plt.xlabel("Recall"); plt.ylabel("Precision")
plt.legend()
plt.tight_layout(); plt.show()

# Važnost značajki (gradijent)
importances = pd.Series(rf.feature_importances_, index=features).sort_values(ascending=False)
imp_df = importances.reset_index(); imp_df.columns = ["feature","importance"]
barplot_gradient(
    imp_df, x="importance", y="feature", hue="feature", cmap="magma",
    title="Feature Importance (Random Forest)", xlabel="Važnost", ylabel="Značajka"
)

# EDA grafovi
df["reason_name"]  = le_dict["reason_dep_delay"].inverse_transform(df["reason_dep_delay"])
df["carrier_name"] = le_dict["carrier"].inverse_transform(df["carrier"])
rdc_names          = le_dict["route_distance_cat"].inverse_transform(df["route_distance_cat"])

delayed_flights = df[df["delayed"] == 1].copy()
problematic_reasons = delayed_flights[~delayed_flights["reason_name"].isin(["On Time", "Early Departure"])]
reason_pct = (problematic_reasons["reason_name"].value_counts(normalize=True) * 100).sort_values(ascending=False)
reason_pct_df = reason_pct.reset_index(); reason_pct_df.columns = ["reason","percent"]
barplot_gradient(
    reason_pct_df, x="reason", y="percent", hue="reason", cmap="viridis",
    rotate_xticks=True, title="Postotak kašnjenja po stvarnom razlogu",
    xlabel="Razlog kašnjenja", ylabel="Postotak od ukupnih kašnjenja (%)"
)

plt.figure(figsize=(10,6))
sns.histplot(df["arr_delay_time"], bins=50, kde=True, color="tomato")
plt.title("Distribucija stvarnih kašnjenja letova")
plt.xlabel("Kašnjenje (minute)"); plt.ylabel("Broj letova")
plt.tight_layout(); plt.show()

monthly_delay = df.groupby("month")["arr_delay_time"].mean().reset_index()
monthly_delay.columns = ["month","avg_delay"]
barplot_gradient(
    monthly_delay, x="month", y="avg_delay", hue="month", cmap="coolwarm",
    title="Prosječno kašnjenje po mjesecu", xlabel="Mjesec", ylabel="Prosječno kašnjenje (min)"
)

dow_delay = df.groupby("day_of_week")["arr_delay_time"].mean().reset_index()
dow_delay.columns = ["day_of_week","avg_delay"]
barplot_gradient(
    dow_delay, x="day_of_week", y="avg_delay", hue="day_of_week", cmap="plasma",
    title="Prosječno kašnjenje po danu u tjednu", xlabel="Dan u tjednu (1=Monday)", ylabel="Prosječno kašnjenje (min)"
)

dist_delay = pd.DataFrame({"rdc": rdc_names, "arr_delay_time": df["arr_delay_time"]}) \
    .groupby("rdc")["arr_delay_time"].mean().reset_index().sort_values("arr_delay_time")
dist_delay.columns = ["rdc","avg_delay"]
barplot_gradient(
    dist_delay, x="rdc", y="avg_delay", hue="rdc", cmap="magma",
    title="Prosječno kašnjenje po udaljenosti leta", xlabel="Kategorija udaljenosti", ylabel="Prosječno kašnjenje (min)"
)

heat_df = df.pivot_table(index="carrier_name", columns="month", values="arr_delay_time", aggfunc="mean")
plt.figure(figsize=(12,6))
sns.heatmap(heat_df, annot=True, fmt=".1f", cmap="coolwarm")
plt.title("Prosječno kašnjenje po carrieru i mjesecu")
plt.xlabel("Mjesec"); plt.ylabel("Carrier")
plt.tight_layout(); plt.show()

# Helperi za predikciju novih letova
def encode_row(row: pd.DataFrame) -> pd.DataFrame:
    row = row.copy()
    if "sched_dep_time" in row:
        row["sched_dep_time"] = row["sched_dep_time"].apply(parse_time)
        row["sched_dep_hour"] = row["sched_dep_time"] // 60
    if "day_of_week" in row:
        row["is_weekend"] = row["day_of_week"].apply(
            lambda x: 1 if (str(x).lower() in {"sat","saturday","sun","sunday"} or x in [6,7,"6","7"]) else 0
        )
    if "distance" in row:
        def _dc(v):
            if pd.isna(v): return "unknown"
            if v < 500: return "short"
            elif v < 1500: return "medium"
            else: return "long"
        row["route_distance_cat"] = row["distance"].apply(_dc)
    for col in ["reason_dep_delay","carrier","origin","destination","day_of_week","route_distance_cat"]:
        if col in row:
            le = le_dict[col]
            known = set(le.classes_)
            vals = row[col].astype(str).apply(lambda x: x if x in known else le.classes_[0])
            row[col] = le.transform(vals)
    return row

def predict_delay_percent(new_data: dict, threshold: float = None):
    row = pd.DataFrame([new_data]); row_enc = encode_row(row)
    proba = rf.predict_proba(row_enc[features])[0][1]
    thr = BEST_THRESHOLD if threshold is None else threshold
    return round(proba * 100, 2), int(proba >= thr)

# Primjeri
new_flights = [
    {"month":5,"day_of_week":"Monday","sched_dep_time":"08:30","reason_dep_delay":"Air Traffic Congestion","carrier":"UA","origin":"JFK","destination":"MIA","distance":1089},
    {"month":12,"day_of_week":"Sunday","sched_dep_time":"18:40","reason_dep_delay":"Early Departure","carrier":"DL","origin":"LGA","destination":"MYR","distance":50},
    {"month":12,"day_of_week":"Sunday","sched_dep_time":"18:40","reason_dep_delay":"On Time","carrier":"DL","origin":"LGA","destination":"MYR","distance":800},
]

print(f"\n>>> Tuned threshold (F2-opt): {BEST_THRESHOLD:.3f}")

labels, probs = [], []
for i, f in enumerate(new_flights, 1):
    p, k = predict_delay_percent(f)
    labels.append(f"Flight {i}"); probs.append(p)
    print(f"Flight {i}: P(delay>15) = {p}% | class@F2 = {k}")

plot_df = pd.DataFrame({"label": labels, "prob": probs})
barplot_gradient(plot_df, x="label", y="prob", hue="label", cmap="cool",
                 title="Predviđena vjerojatnost kašnjenja (>15 min) — tuned",
                 ylabel="Vjerojatnost (%)")
plt.ylim(0, 100)
