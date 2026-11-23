"""Hazırlanmış olaylardan kullanıcı oturumlarını ve süre ölçümlerini üretir."""

import argparse
import json
from pathlib import Path
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.ogrenci1.common import PATHS, SAMPLE_SIZES, get_spark


SESSIONS_DIR = PATHS["samples"].with_name("sessions")
SESSION_COLUMNS = (
    "user_session", "user_id", "start_time", "end_time", "duration_sec",
    "n_events", "n_views", "n_carts", "n_removes", "n_purchases", "n_products",
    "revenue", "reached_cart", "reached_purchase", "start_hour", "start_dow",
)
EVENT_COLUMNS = (
    "event_time", "user_id", "user_session", "product_id", "price",
    "is_view", "is_cart", "is_remove", "is_purchase",
)
RECOVERED_PREFIX = "__recovered__:"


def assign_sessions(events):
    """Kaynak kimliklerini korur, eksikleri kullanıcının zaman akışından tamamlar."""
    events = events.select(*EVENT_COLUMNS).withColumn(
        "user_session", F.when(F.length(F.trim("user_session")) > 0, F.col("user_session"))
    )
    missing = events.filter(F.col("user_session").isNull())
    missing_users = missing.select("user_id").distinct()

    # Eksik kimliği olan kullanıcıların bilinen olayları da 30 dakikalık arayı belirler.
    # Bir zaman damgasındaki olayları tek satıra toplamak eşit zamanlarda sıra belirsizliğini önler.
    timestamps = (
        events.join(missing_users, "user_id", "left_semi")
        .groupBy("user_id", "event_time")
        .agg(F.min("user_session").alias("min_id"), F.max("user_session").alias("max_id"))
    )
    timeline = Window.partitionBy("user_id").orderBy("event_time")
    previous = F.lag("event_time").over(timeline)
    boundary = previous.isNull() | (F.col("event_time") > previous + F.expr("INTERVAL 30 MINUTES"))
    blocks = timestamps.withColumn(
        "block", F.sum(boundary.cast("int")).over(timeline.rowsBetween(Window.unboundedPreceding, 0))
    )
    block = Window.partitionBy("user_id", "block")
    minimum = F.min("min_id").over(block)
    maximum = F.max("max_id").over(block)
    block_start = F.min("event_time").over(block)
    synthetic = F.concat(F.lit(RECOVERED_PREFIX), F.col("user_id"), F.lit(":"), F.unix_micros(block_start))
    repairs = blocks.select(
        "user_id", "event_time", F.when(minimum == maximum, minimum).otherwise(synthetic).alias("filled_id")
    )
    restored = (
        missing.drop("user_session").join(repairs, ["user_id", "event_time"])
        .withColumnRenamed("filled_id", "user_session")
        .withColumn("was_missing", F.lit(1))
    )
    known = events.filter(F.col("user_session").isNotNull()).withColumn("was_missing", F.lit(0))
    return known.unionByName(restored)


def seconds_between(start, end):
    return (F.unix_micros(end) - F.unix_micros(start)) / 1_000_000


def aggregate_sessions(events):
    """Ortak oturum şeması ve raporlar için gereken ara ölçümler."""
    return (
        assign_sessions(events).groupBy("user_session", "user_id")
        .agg(
            F.min("event_time").alias("start_time"),
            F.max("event_time").alias("end_time"),
            F.count("*").alias("n_events"),
            F.sum("is_view").alias("n_views"),
            F.sum("is_cart").alias("n_carts"),
            F.sum("is_remove").alias("n_removes"),
            F.sum("is_purchase").alias("n_purchases"),
            F.countDistinct("product_id").alias("n_products"),
            F.coalesce(F.sum(F.when(F.col("is_purchase") == 1, F.col("price"))), F.lit(0.0)).alias("revenue"),
            F.min(F.when(F.col("is_purchase") == 1, F.col("event_time"))).alias("first_purchase"),
            F.sum("was_missing").alias("n_recovered"),
        )
        .withColumn("duration_sec", seconds_between("start_time", "end_time"))
        .withColumn("reached_cart", (F.col("n_carts") > 0).cast("int"))
        .withColumn("reached_purchase", (F.col("n_purchases") > 0).cast("int"))
        .withColumn("start_hour", F.hour("start_time"))
        .withColumn("start_dow", F.dayofweek("start_time"))
    )


def build_sample(spark, size, source):
    destination = SESSIONS_DIR / size
    measurement = destination / "olcum.json"
    measurement.unlink(missing_ok=True)
    started = perf_counter()
    details = aggregate_sessions(spark.read.parquet(str(source))).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        details.select(*SESSION_COLUMNS).write.mode("overwrite").parquet(str(destination / "sessions.parquet"))
        elapsed = perf_counter() - started
        purchase_delay = seconds_between("start_time", "first_purchase")
        synthetic = F.col("user_session").startswith(RECOVERED_PREFIX)
        metrics = details.agg(
            F.avg(purchase_delay).alias("satin_almaya_ort_sure_sn"),
            F.percentile_approx(purchase_delay, 0.5, 10000).alias("satin_almaya_medyan_sure_sn"),
            F.coalesce(F.sum("n_recovered"), F.lit(0)).alias("eksik_oturumlu_olay_sayisi"),
            F.coalesce(F.sum(F.when(~synthetic, F.col("n_recovered"))), F.lit(0))
            .alias("mevcut_oturuma_atanan_olay_sayisi"),
            F.count(F.when(synthetic, 1)).alias("turetilen_oturum_sayisi"),
        ).first().asDict()
    finally:
        details.unpersist()

    metrics["hesaplama_suresi_sn"] = round(elapsed, 3)
    pending = measurement.with_suffix(".tmp")
    pending.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    pending.replace(measurement)
    print(f"[{size}] {destination / 'sessions.parquet'} ({elapsed:.2f} sn)", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", choices=list(SAMPLE_SIZES), default=list(SAMPLE_SIZES))
    parser.add_argument("--samples-dir", type=Path, default=PATHS["samples"], help="100k/1m/5m klasörlerinin yolu")
    args = parser.parse_args()
    sizes = list(dict.fromkeys(args.sizes))
    sources = {size: args.samples_dir.resolve() / size / "events.parquet" for size in sizes}
    for source in sources.values():
        if not source.exists():
            parser.error(f"Parquet bulunamadı: {source}. Gerekirse --samples-dir ile örneklem klasörünü belirtin.")

    spark = get_spark("BVA-Ogrenci2-Oturumlar")
    try:
        spark.range(1).count()
        for size, source in sources.items():
            build_sample(spark, size, source)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
