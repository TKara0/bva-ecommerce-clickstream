"""
Öğrenci 5 - Performans Karşılaştırması (Pandas vs PySpark × CSV vs Parquet × 100k/1m/5m).

Aynı 4 analitik sorguyu ve tam tablo yüklemesini iki motorda (Pandas, PySpark) ve
iki dosya formatında (CSV, Parquet) çalıştırır; süre (3 tekrarın medyanı), bellek (RSS ve
DataFrame boyutu), throughput (satır/sn) ve Öğrenci 1'in disk boyutlarını kaydeder.
Her boyutta 4 kombinasyonun ürettiği sayısal sonuçların birebir eşleştiğini doğrular.

Kullanım (repo kökünden):
    python -m src.ogrenci5.benchmark --sizes 100k
    python -m src.ogrenci5.benchmark --sizes 100k 1m 5m --repeat 3
"""
from __future__ import annotations

import argparse
import gc
import math
import os
import platform
import statistics
import time
from pathlib import Path
from typing import Any

import pandas as pd
import psutil
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.ogrenci1.common import PATHS, ROOT, get_spark

# Çıktı dizini ve Öğrenci 1'in format karşılaştırma raporu
REPORTS_DIR = ROOT / "reports" / "ogrenci5"
OG1_FORMAT_CSV = PATHS["reports"] / "format_karsilastirma.csv"

# Temizlenmiş örneklem CSV dosyaları için açık şema (inferSchema kullanılmaz)
EVENTS_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    StructField("event_dow", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("funnel_step", IntegerType(), True),
    StructField("funnel_stage", StringType(), True),
    StructField("is_view", IntegerType(), True),
    StructField("is_cart", IntegerType(), True),
    StructField("is_remove", IntegerType(), True),
    StructField("is_purchase", IntegerType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("category_l1", StringType(), True),
    StructField("category_l2", StringType(), True),
    StructField("category_l3", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
    StructField("event_month", StringType(), True),
])

# Sorgu tanımları: ad, açıklama ve yalnızca gereken kolonlar (column pruning için)
QUERIES: list[dict[str, Any]] = [
    {
        "id": "Q0_tam_okuma",
        "ad": "Tam tablo okuma (22 kolon)",
        "cols": None,  # Tüm kolonlar okunur (RAM etkisini görmek için)
    },
    {
        "id": "Q1_olay_tipi",
        "ad": "Olay tipine göre sayım ve ortalama fiyat",
        "cols": ["event_type", "price"],
    },
    {
        "id": "Q2_saatlik_olay",
        "ad": "Saatlik olay ve satın alma sayısı",
        "cols": ["event_hour", "is_purchase"],
    },
    {
        "id": "Q3_kullanici_satin_alma",
        "ad": "Kullanıcı başına satın alma ve harcama",
        "cols": ["event_type", "user_id", "price"],
    },
    {
        "id": "Q4_oturum_donusum",
        "ad": "Oturum bazlı view->cart->purchase dönüşümü",
        "cols": ["user_id", "user_session", "is_view", "is_cart", "is_purchase"],
    },
]

# Pandas tarafında bellek güvenliği eşiği (MB): boş RAM bunun altına inerse 5m'de atla
MIN_FREE_RAM_MB = 1500.0


def get_process_tree_rss_mb() -> tuple[float, float]:
    """Mevcut Python sürecinin ve alt süreçlerinin (Spark JVM dahil) RSS belleğini MB olarak döndürür."""
    proc = psutil.Process(os.getpid())
    py_rss = proc.memory_info().rss
    child_rss = 0
    for child in proc.children(recursive=True):
        try:
            child_rss += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return py_rss / (1024 ** 2), child_rss / (1024 ** 2)


def load_disk_sizes() -> dict[tuple[str, str], float]:
    """Öğrenci 1'in format_karsilastirma.csv dosyasından disk boyutlarını (MB) okur."""
    disk_map: dict[tuple[str, str], float] = {}
    if OG1_FORMAT_CSV.exists():
        df_fmt = pd.read_csv(OG1_FORMAT_CSV)
        for _, row in df_fmt.iterrows():
            disk_map[(str(row["orneklem"]), str(row["format"]))] = float(row["disk_mb"])
    return disk_map


def sample_path(size: str, fmt: str) -> Path:
    """PATHS['samples'] üzerinden örneklem dosya/klasör yolunu döndürür."""
    ext = "events.csv" if fmt == "CSV" else "events.parquet"
    return PATHS["samples"] / size / ext


# ---------------------------------------------------------------------------
# Pandas sorgu uygulamaları
# ---------------------------------------------------------------------------
def run_pandas_query(
    path: Path, fmt: str, query_id: str, cols: list[str] | None
) -> tuple[dict[str, Any], float, float, int, float]:
    """Verilen sorguyu Pandas ile dosyadan okuyup çalıştırır;
    (doğrulama özeti, sure_sn, df_mem_mb, satir_sayisi, aktif_rss_mb) döner."""
    t0 = time.perf_counter()
    if fmt == "CSV":
        df = pd.read_csv(path, usecols=cols, low_memory=False)
    else:
        df = pd.read_parquet(path, columns=cols)

    n_rows = len(df)

    if query_id == "Q0_tam_okuma":
        result = {"n_rows": int(n_rows), "n_cols": int(df.shape[1])}

    elif query_id == "Q1_olay_tipi":
        agg = (
            df.groupby("event_type", sort=True)
            .agg(n=("price", "size"), avg_price=("price", "mean"))
        )
        result = {
            "groups": {
                str(k): (int(v["n"]), round(float(v["avg_price"]), 4))
                for k, v in agg.iterrows()
            }
        }

    elif query_id == "Q2_saatlik_olay":
        agg = (
            df.groupby("event_hour", sort=True)
            .agg(n=("is_purchase", "size"), purchases=("is_purchase", "sum"))
        )
        result = {
            "hours": {
                int(k): (int(v["n"]), int(v["purchases"]))
                for k, v in agg.iterrows()
            }
        }

    elif query_id == "Q3_kullanici_satin_alma":
        buyers = df[df["event_type"] == "purchase"]
        agg = buyers.groupby("user_id", sort=False)["price"].agg(["count", "sum"])
        result = {
            "n_buyers": int(len(agg)),
            "total_purchases": int(agg["count"].sum()),
            "total_revenue": round(float(agg["sum"].sum()), 2),
        }

    elif query_id == "Q4_oturum_donusum":
        # Spark ile birebir aynı mantık: boş user_session satırları hariç
        valid = df.dropna(subset=["user_session"])
        sess = valid.groupby(["user_id", "user_session"], sort=False).agg(
            has_view=("is_view", "max"),
            has_cart=("is_cart", "max"),
            has_purchase=("is_purchase", "max"),
        )
        n_sess = int(len(sess))
        n_view = int((sess["has_view"] > 0).sum())
        n_view_cart = int(((sess["has_view"] > 0) & (sess["has_cart"] > 0)).sum())
        n_view_cart_buy = int(
            ((sess["has_view"] > 0) & (sess["has_cart"] > 0) & (sess["has_purchase"] > 0)).sum()
        )
        result = {
            "n_sessions": n_sess,
            "n_view": n_view,
            "n_view_cart": n_view_cart,
            "n_view_cart_buy": n_view_cart_buy,
        }
    else:
        raise ValueError(f"Bilinmeyen sorgu: {query_id}")

    # Süreyi bellek taramasından ÖNCE durdur (deep=True taraması süreyi şişirmesin)
    dt = time.perf_counter() - t0
    active_rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
    df_mem_mb = float(df.memory_usage(deep=True).sum() / (1024 ** 2))
    return result, dt, df_mem_mb, n_rows, active_rss_mb


# ---------------------------------------------------------------------------
# PySpark sorgu uygulamaları
# ---------------------------------------------------------------------------
def _spark_read(spark: SparkSession, path: Path, fmt: str) -> SparkDataFrame:
    """Örneklem dosyasını CSV veya Parquet olarak Spark DataFrame'e açar."""
    if fmt == "CSV":
        return spark.read.csv(str(path), header=True, schema=EVENTS_SCHEMA)
    return spark.read.parquet(str(path))


def run_spark_query(spark: SparkSession, path: Path, fmt: str, query_id: str) -> tuple[dict[str, Any], int]:
    """Verilen sorguyu PySpark ile çalıştırır; (doğrulama özeti, satir_sayisi) döner."""
    df = _spark_read(spark, path, fmt)

    if query_id == "Q0_tam_okuma":
        # Tüm kolonların gerçekten okunmasını sağlamak için satır ve kolon sayısı + null olmayan toplam
        n_cols = len(df.columns)
        row = df.select(
            F.count("*").alias("n"),
            F.sum(F.length(F.coalesce(F.col("user_session"), F.lit("")))).alias("chk_s"),
            F.sum(F.length(F.coalesce(F.col("brand"), F.lit("")))).alias("chk_b"),
            F.sum(F.coalesce(F.col("price"), F.lit(0.0))).alias("chk_p"),
        ).collect()[0]
        n_rows = int(row["n"])
        result = {"n_rows": n_rows, "n_cols": n_cols}

    elif query_id == "Q1_olay_tipi":
        rows = (
            df.groupBy("event_type")
            .agg(F.count("*").alias("n"), F.avg("price").alias("avg_price"))
            .orderBy("event_type")
            .collect()
        )
        n_rows = sum(int(r["n"]) for r in rows)
        result = {
            "groups": {
                str(r["event_type"]): (int(r["n"]), round(float(r["avg_price"]), 4))
                for r in rows
            }
        }

    elif query_id == "Q2_saatlik_olay":
        rows = (
            df.groupBy("event_hour")
            .agg(F.count("*").alias("n"), F.sum("is_purchase").alias("purchases"))
            .orderBy("event_hour")
            .collect()
        )
        n_rows = sum(int(r["n"]) for r in rows)
        result = {
            "hours": {
                int(r["event_hour"]): (int(r["n"]), int(r["purchases"]))
                for r in rows
            }
        }

    elif query_id == "Q3_kullanici_satin_alma":
        per_user = (
            df.filter(F.col("event_type") == "purchase")
            .groupBy("user_id")
            .agg(F.count("*").alias("cnt"), F.sum("price").alias("spend"))
        )
        row = per_user.agg(
            F.count("*").alias("n_buyers"),
            F.sum("cnt").alias("total_purchases"),
            F.sum("spend").alias("total_revenue"),
        ).collect()[0]
        n_rows = -1  # Toplam örneklem satır sayısı dışarıdan doldurulur
        result = {
            "n_buyers": int(row["n_buyers"]),
            "total_purchases": int(row["total_purchases"]),
            "total_revenue": round(float(row["total_revenue"]), 2),
        }

    elif query_id == "Q4_oturum_donusum":
        sess = (
            df.filter(F.col("user_session").isNotNull())
            .groupBy("user_id", "user_session")
            .agg(
                F.max("is_view").alias("has_view"),
                F.max("is_cart").alias("has_cart"),
                F.max("is_purchase").alias("has_purchase"),
            )
        )
        row = sess.agg(
            F.count("*").alias("n_sessions"),
            F.sum(F.when(F.col("has_view") > 0, 1).otherwise(0)).alias("n_view"),
            F.sum(F.when((F.col("has_view") > 0) & (F.col("has_cart") > 0), 1).otherwise(0)).alias("n_view_cart"),
            F.sum(
                F.when(
                    (F.col("has_view") > 0) & (F.col("has_cart") > 0) & (F.col("has_purchase") > 0),
                    1,
                ).otherwise(0)
            ).alias("n_view_cart_buy"),
        ).collect()[0]
        n_rows = -1
        result = {
            "n_sessions": int(row["n_sessions"]),
            "n_view": int(row["n_view"]),
            "n_view_cart": int(row["n_view_cart"]),
            "n_view_cart_buy": int(row["n_view_cart_buy"]),
        }
    else:
        raise ValueError(f"Bilinmeyen sorgu: {query_id}")

    return result, n_rows


# ---------------------------------------------------------------------------
# Sonuç eşleşme doğrulaması
# ---------------------------------------------------------------------------
def results_match(a: dict[str, Any], b: dict[str, Any], tol: float = 0.02) -> bool:
    """İki motorun ürettiği özet sonuçların birebir (ondalıklarda küçük toleransla) eşleştiğini kontrol eder."""
    if a.keys() != b.keys():
        return False
    for k in a:
        va, vb = a[k], b[k]
        if isinstance(va, dict) and isinstance(vb, dict):
            if not results_match(va, vb, tol=tol):
                return False
        elif isinstance(va, tuple) and isinstance(vb, tuple):
            if len(va) != len(vb):
                return False
            for xa, xb in zip(va, vb):
                if isinstance(xa, float) or isinstance(xb, float):
                    if not math.isclose(float(xa), float(xb), rel_tol=1e-4, abs_tol=tol):
                        return False
                elif xa != xb:
                    return False
        elif isinstance(va, float) or isinstance(vb, float):
            if not math.isclose(float(va), float(vb), rel_tol=1e-4, abs_tol=tol):
                return False
        elif va != vb:
            return False
    return True


# ---------------------------------------------------------------------------
# Ana benchmark döngüsü
# ---------------------------------------------------------------------------
def benchmark_size(
    size: str,
    repeat: int,
    disk_map: dict[tuple[str, str], float],
    spark: SparkSession,
    spark_startup_sn: float,
) -> list[dict[str, Any]]:
    """Tek bir örneklem boyutu (100k/1m/5m) için tüm motor × format × sorgu kombinasyonlarını ölçer."""
    rows_out: list[dict[str, Any]] = []
    # Doğrulama için her sorgunun referans çıktısını sakla
    ref_results: dict[str, dict[str, Any]] = {}
    sample_row_count: int | None = None

    print(f"\n{'=' * 72}")
    print(f"BOYUT: {size.upper()}  (tekrar: {repeat}, medyan alınıyor)")
    print(f"{'=' * 72}")

    # 1) Pandas (CSV ve Parquet)
    for fmt in ("CSV", "Parquet"):
        path = sample_path(size, fmt)
        disk_mb = disk_map.get((size, fmt), float("nan"))
        for q in QUERIES:
            qid = q["id"]
            gc.collect()
            avail_mb = psutil.virtual_memory().available / (1024 ** 2)
            # 5m tam okumada RAM yetersizse çökmeden atla
            if size == "5m" and avail_mb < MIN_FREE_RAM_MB:
                print(f"  [ATLANDI] Pandas | {fmt:7s} | {qid:24s} -> Yetersiz boş RAM ({avail_mb:.0f} MB)")
                rows_out.append({
                    "boyut": size,
                    "satir_sayisi": sample_row_count or 0,
                    "motor": "Pandas",
                    "format": fmt,
                    "sorgu": qid,
                    "sorgu_adi": q["ad"],
                    "sure_medyan_sn": float("nan"),
                    "sure_min_sn": float("nan"),
                    "sure_max_sn": float("nan"),
                    "throughput_satir_sn": float("nan"),
                    "df_bellek_mb": float("nan"),
                    "rss_artis_mb": float("nan"),
                    "toplam_rss_mb": float("nan"),
                    "disk_mb": disk_mb,
                    "spark_baslatma_sn": 0.0,
                    "dogrulama": "ATLANDI",
                    "durum": f"OOM_ONLEMI (bos RAM {avail_mb:.0f} MB)",
                })
                continue

            times: list[float] = []
            df_mems: list[float] = []
            rss_deltas: list[float] = []
            rss_peaks: list[float] = []
            last_res: dict[str, Any] | None = None
            status = "OK"

            try:
                for _ in range(repeat):
                    gc.collect()
                    py_before, _ = get_process_tree_rss_mb()
                    res, dt, df_mem_mb, n_rows, py_active = run_pandas_query(path, fmt, qid, q["cols"])

                    times.append(dt)
                    df_mems.append(df_mem_mb)
                    rss_deltas.append(max(0.0, py_active - py_before))
                    rss_peaks.append(py_active)
                    last_res = res
                    if sample_row_count is None:
                        sample_row_count = n_rows
                    gc.collect()
            except MemoryError:
                status = "OOM_ATLANDI"
                gc.collect()

            if status != "OK" or not times or last_res is None:
                print(f"  [OOM] Pandas | {fmt:7s} | {qid:24s} -> Bellek sınırını aştı, atlandı.")
                rows_out.append({
                    "boyut": size,
                    "satir_sayisi": sample_row_count or 0,
                    "motor": "Pandas",
                    "format": fmt,
                    "sorgu": qid,
                    "sorgu_adi": q["ad"],
                    "sure_medyan_sn": float("nan"),
                    "sure_min_sn": float("nan"),
                    "sure_max_sn": float("nan"),
                    "throughput_satir_sn": float("nan"),
                    "df_bellek_mb": float("nan"),
                    "rss_artis_mb": float("nan"),
                    "toplam_rss_mb": float("nan"),
                    "disk_mb": disk_mb,
                    "spark_baslatma_sn": 0.0,
                    "dogrulama": "ATLANDI",
                    "durum": status,
                })
                continue

            med_t = statistics.median(times)
            med_df_mem = statistics.median(df_mems)
            med_rss_delta = statistics.median(rss_deltas)
            max_rss = max(rss_peaks)
            throughput = (sample_row_count / med_t) if med_t > 0 and sample_row_count else 0.0

            if qid not in ref_results:
                ref_results[qid] = last_res
                verified = "REFERANS"
            else:
                verified = "ESLESIYOR" if results_match(ref_results[qid], last_res) else "FARKLI!"

            print(
                f"  Pandas | {fmt:7s} | {qid:24s} | {med_t:6.3f} sn "
                f"| DF: {med_df_mem:7.1f} MB | RSS: {max_rss:7.1f} MB | {verified}"
            )
            rows_out.append({
                "boyut": size,
                "satir_sayisi": sample_row_count,
                "motor": "Pandas",
                "format": fmt,
                "sorgu": qid,
                "sorgu_adi": q["ad"],
                "sure_medyan_sn": round(med_t, 4),
                "sure_min_sn": round(min(times), 4),
                "sure_max_sn": round(max(times), 4),
                "throughput_satir_sn": round(throughput, 1),
                "df_bellek_mb": round(med_df_mem, 2),
                "rss_artis_mb": round(med_rss_delta, 2),
                "toplam_rss_mb": round(max_rss, 2),
                "disk_mb": disk_mb,
                "spark_baslatma_sn": 0.0,
                "dogrulama": verified,
                "durum": status,
            })

    # 2) PySpark (CSV ve Parquet)
    for fmt in ("CSV", "Parquet"):
        path = sample_path(size, fmt)
        disk_mb = disk_map.get((size, fmt), float("nan"))
        for q in QUERIES:
            qid = q["id"]
            times = []
            rss_deltas = []
            rss_peaks = []
            last_res = None

            for _ in range(repeat):
                # Önceki önbellek/artıkları temizle ki her tekrar dosyadan bağımsız ölçülsün
                spark.catalog.clearCache()
                py_b, jvm_b = get_process_tree_rss_mb()
                tot_before = py_b + jvm_b

                t0 = time.perf_counter()
                res, n_rows = run_spark_query(spark, path, fmt, qid)
                dt = time.perf_counter() - t0

                py_a, jvm_a = get_process_tree_rss_mb()
                tot_after = py_a + jvm_a

                times.append(dt)
                rss_deltas.append(max(0.0, tot_after - tot_before))
                rss_peaks.append(tot_after)
                last_res = res
                if sample_row_count is None and n_rows > 0:
                    sample_row_count = n_rows

            med_t = statistics.median(times)
            med_rss_delta = statistics.median(rss_deltas)
            max_rss = max(rss_peaks)
            throughput = (sample_row_count / med_t) if med_t > 0 and sample_row_count else 0.0

            if qid not in ref_results:
                ref_results[qid] = last_res  # type: ignore[assignment]
                verified = "REFERANS"
            else:
                verified = "ESLESIYOR" if results_match(ref_results[qid], last_res) else "FARKLI!"  # type: ignore[arg-type]

            print(
                f"  Spark  | {fmt:7s} | {qid:24s} | {med_t:6.3f} sn "
                f"| JVM+Py RSS: {max_rss:7.1f} MB | {verified}"
            )
            rows_out.append({
                "boyut": size,
                "satir_sayisi": sample_row_count,
                "motor": "Spark",
                "format": fmt,
                "sorgu": qid,
                "sorgu_adi": q["ad"],
                "sure_medyan_sn": round(med_t, 4),
                "sure_min_sn": round(min(times), 4),
                "sure_max_sn": round(max(times), 4),
                "throughput_satir_sn": round(throughput, 1),
                "df_bellek_mb": float("nan"),  # Spark dağıtık/lazy; driver DataFrame RAM'i tutmaz
                "rss_artis_mb": round(med_rss_delta, 2),
                "toplam_rss_mb": round(max_rss, 2),
                "disk_mb": disk_mb,
                "spark_baslatma_sn": round(spark_startup_sn, 3),
                "dogrulama": verified,
                "durum": "OK",
            })

    # Eşleşmeyen sonuç varsa hemen uyar
    mismatches = [r for r in rows_out if r["dogrulama"] == "FARKLI!"]
    if mismatches:
        raise RuntimeError(f"{size} boyutunda sonuç uyuşmazlığı bulundu: {mismatches}")

    return rows_out


def save_results(new_rows: list[dict[str, Any]], sizes: list[str]) -> Path:
    """Sonuçları reports/ogrenci5/performans.csv dosyasına yazar (diğer boyutları korur)."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = REPORTS_DIR / "performans.csv"
    df_new = pd.DataFrame(new_rows)

    if out_csv.exists():
        df_old = pd.read_csv(out_csv)
        # Sadece yeni çalıştırılan boyutların satırlarını yenile
        df_old = df_old[~df_old["boyut"].isin(sizes)]
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    size_order = {"100k": 0, "1m": 1, "5m": 2}
    df_all["_ord"] = df_all["boyut"].map(lambda s: size_order.get(str(s), 99))
    df_all = df_all.sort_values(["_ord", "sorgu", "motor", "format"]).drop(columns=["_ord"])
    df_all.to_csv(out_csv, index=False)
    return out_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Öğrenci 5 - Pandas vs Spark Performans Karşılaştırması")
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=["100k"],
        choices=["100k", "1m", "5m"],
        help="Çalıştırılacak örneklem boyutları (varsayılan: 100k)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Her sorgu için tekrar sayısı (varsayılan: 3, medyan raporlanır)",
    )
    args = parser.parse_args()

    ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    print(
        f"Ortam: {platform.system()} {platform.release()} | "
        f"CPU: {os.cpu_count()} mantıksal çekirdek | RAM: {ram_gb:.1f} GB | "
        f"BVA_DRIVER_MEM={os.environ.get('BVA_DRIVER_MEM', 'varsayılan')}"
    )

    disk_map = load_disk_sizes()

    # Spark oturumunu başlat ve başlatma + ısınma maliyetini ayrıca ölç
    t_start = time.perf_counter()
    spark = get_spark("BVA-Proje3-Ogrenci5-Benchmark")
    # Küçük bir iş ile JVM/Spark'ı uyandır (soğuk başlatma süresi ayrı raporlanır)
    spark.range(1000).count()
    spark_startup_sn = time.perf_counter() - t_start
    print(f"Spark JVM başlatma + ısınma süresi: {spark_startup_sn:.2f} sn")

    all_rows: list[dict[str, Any]] = []
    try:
        for size in args.sizes:
            all_rows.extend(benchmark_size(size, args.repeat, disk_map, spark, spark_startup_sn))
    finally:
        spark.stop()

    out_csv = save_results(all_rows, args.sizes)
    print(f"\nKaydedildi: {out_csv} ({len(all_rows)} yeni satır)")

    # Kısa özet tablo yazdır
    df_res = pd.DataFrame(all_rows)
    piv = df_res.pivot_table(
        index=["boyut", "sorgu"],
        columns=["motor", "format"],
        values="sure_medyan_sn",
    )
    print("\nMedyan Süreler (sn):")
    print(piv.to_string())


if __name__ == "__main__":
    main()
