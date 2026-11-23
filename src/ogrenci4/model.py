"""
Öğrenci 4 - Oturum bazlı satın alma olasılığı modeli (Spark MLlib)

Modeller:
  - LogisticRegression (baseline, özellikler StandardScaler ile ölçeklenir)
  - GBTClassifier (ağaç tabanlı, featureImportances verir)
  - LR_sizintili: SADECE gösterim, n_purchases + revenue eklenince AUC ~1.0 olur

Dengesizlik: satın alan oturum ~%3; weightCol ile sınıf ağırlığı veriliyor
(pozitif ve negatif sınıfın toplam ağırlığı eşit). Metrikler: AUC-ROC, AUC-PR,
precision, recall, F1 (pozitif sınıf, eşik 0.5). Accuracy yanıltıcı olduğu için
"hep 0 de" tahmininin accuracy'si de yanına yazılıyor.

Çalıştırma (repo kökünden):
  python -m src.ogrenci4.model                     # sadece 100k
  python -m src.ogrenci4.model --sizes 1m 5m       # diğer boyutlar (sonuçlar CSV'ye eklenir)
"""
import argparse
import time

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.ogrenci1.common import get_spark
from src.ogrenci4.features import FEATURES, LABEL, LEAKY, REPORTS, load_dataset
from src.ogrenci4.plots import make_plots

SEED = 42


def add_class_weights(train):
    """Pozitif sınıf az olduğu için ağırlığı büyük: her sınıfın toplam ağırlığı yarı yarıya."""
    pos_rate = train.agg(F.avg(LABEL)).first()[0]
    w_pos, w_neg = 0.5 / pos_rate, 0.5 / (1 - pos_rate)
    return train.withColumn("w", F.when(F.col(LABEL) == 1, w_pos).otherwise(w_neg)), pos_rate


def build_models():
    lr = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True),
        LogisticRegression(labelCol=LABEL, weightCol="w", maxIter=100, regParam=0.01),
    ])
    gbt = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="features"),
        GBTClassifier(labelCol=LABEL, weightCol="w", maxIter=50, maxDepth=5, seed=SEED),
    ])
    leaky = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES + LEAKY, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True),
        LogisticRegression(labelCol=LABEL, weightCol="w", maxIter=100, regParam=0.01),
    ])
    return {"LogisticRegression": lr, "GBT": gbt, "LR_sizintili (gosterim)": leaky}


def evaluate(pred):
    """AUC'ler Spark evaluator ile; precision/recall/F1 karışıklık matrisinden."""
    ev = BinaryClassificationEvaluator(labelCol=LABEL, rawPredictionCol="rawPrediction")
    auc_roc = ev.setMetricName("areaUnderROC").evaluate(pred)
    auc_pr = ev.setMetricName("areaUnderPR").evaluate(pred)
    c = pred.agg(
        F.sum(((F.col("prediction") == 1) & (F.col(LABEL) == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("prediction") == 1) & (F.col(LABEL) == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("prediction") == 0) & (F.col(LABEL) == 1)).cast("int")).alias("fn"),
        F.sum(((F.col("prediction") == 0) & (F.col(LABEL) == 0)).cast("int")).alias("tn"),
    ).first()
    tp, fp, fn, tn = c["tp"], c["fp"], c["fn"], c["tn"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"auc_roc": auc_roc, "auc_pr": auc_pr, "precision": precision, "recall": recall,
            "f1": f1, "accuracy": (tp + tn) / (tp + fp + fn + tn), "tp": tp, "fp": fp, "fn": fn, "tn": tn}


def curve_points(pred, size, model_name, n_points=200):
    """ROC ve PR eğrisi noktaları (test seti olasılıkları sürücüye alınıyor, sadece 2 kolon)."""
    pdf = pred.select(vector_to_array("probability")[1].alias("p"), LABEL).toPandas()
    p, y = pdf["p"].to_numpy(), pdf[LABEL].to_numpy()
    order = np.argsort(-p)
    y = y[order]
    tps, fps = np.cumsum(y), np.cumsum(1 - y)
    idx = np.unique(np.linspace(0, len(y) - 1, n_points).astype(int))
    tpr = tps[idx] / max(tps[-1], 1)
    fpr = fps[idx] / max(fps[-1], 1)
    prec = tps[idx] / (idx + 1)
    return pd.DataFrame({"boyut": size, "model": model_name, "fpr": fpr, "tpr": tpr,
                         "recall": tpr, "precision": prec})


def upsert_csv(new: pd.DataFrame, path, sizes):
    """Aynı boyut tekrar çalıştırılırsa eski satırları silip yenilerini ekle."""
    if path.exists():
        old = pd.read_csv(path)
        new = pd.concat([old[~old["boyut"].isin(sizes)], new], ignore_index=True)
    order = {"100k": 0, "1m": 1, "5m": 2}
    new = new.sort_values("boyut", key=lambda s: s.map(order), kind="stable")
    new.to_csv(path, index=False)
    return new


def run_size(spark, size):
    t0 = time.time()
    df, empty_prefix, source = load_dataset(spark, size)
    df = df.cache()
    n = df.count()
    n_empty = empty_prefix.count()
    feat_sec = time.time() - t0
    print(f"\n[{size}] {n:,} oturum, kaynak: {source}, tablo hazırlama {feat_sec:.1f} sn "
          f"(öneki boş {n_empty} oturum çıkarıldı)")

    train, test = df.randomSplit([0.8, 0.2], seed=SEED)
    train, pos_rate = add_class_weights(train)
    train, test = train.cache(), test.cache()
    n_train, n_test = train.count(), test.count()
    test_pos = test.agg(F.avg(LABEL)).first()[0]

    metrics, curves, importances = [], [], []
    for name, pipe in build_models().items():
        t1 = time.time()
        model = pipe.fit(train)
        train_sec = time.time() - t1
        pred = model.transform(test).cache()
        m = evaluate(pred)
        metrics.append({"boyut": size, "model": name, "kaynak": source, "n_oturum": n, "bos_onek_cikarilan": n_empty,
                        "n_train": n_train, "n_test": n_test, "pozitif_oran": round(pos_rate, 4),
                        **{k: round(v, 4) if isinstance(v, float) else v for k, v in m.items()},
                        "hep0_accuracy": round(1 - test_pos, 4),
                        "egitim_sn": round(train_sec, 2), "tablo_hazirlama_sn": round(feat_sec, 2)})
        print(f"  {name:<24} AUC-ROC {m['auc_roc']:.3f}  AUC-PR {m['auc_pr']:.3f}  "
              f"P {m['precision']:.3f}  R {m['recall']:.3f}  F1 {m['f1']:.3f}  ({train_sec:.1f} sn)")
        if name != "LR_sizintili (gosterim)":
            curves.append(curve_points(pred, size, name))
        if name == "GBT":
            imp = model.stages[-1].featureImportances.toArray()
            importances.append(pd.DataFrame({"boyut": size, "ozellik": FEATURES, "onem": imp.round(4)})
                               .sort_values("onem", ascending=False))
        pred.unpersist()
    df.unpersist(); train.unpersist(); test.unpersist()
    return pd.DataFrame(metrics), pd.concat(curves), pd.concat(importances)


def main(sizes):
    REPORTS.mkdir(parents=True, exist_ok=True)
    spark = get_spark("BVA-Proje3-Ogrenci4")
    results = [run_size(spark, s) for s in sizes]
    spark.stop()

    metrics = upsert_csv(pd.concat([r[0] for r in results]), REPORTS / "model_metrikleri.csv", sizes)
    upsert_csv(pd.concat([r[1] for r in results]), REPORTS / "egri_noktalari.csv", sizes)
    upsert_csv(pd.concat([r[2] for r in results]), REPORTS / "ozellik_onemi.csv", sizes)
    make_plots()
    print("\n[ok] reports/ogrenci4/ güncellendi")
    print(metrics[["boyut", "model", "auc_roc", "auc_pr", "precision", "recall", "f1",
                   "accuracy", "hep0_accuracy", "egitim_sn"]].to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=["100k"], choices=["100k", "1m", "5m"])
    main(ap.parse_args().sizes)
