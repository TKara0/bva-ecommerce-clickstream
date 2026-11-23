"""Oturum ve kullanıcı hunilerini, olay oranlarını ve sepet terki profilini üretir."""

import argparse
import csv
import json

from matplotlib.figure import Figure
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.ogrenci1.common import PATHS, SAMPLE_SIZES, get_spark
from src.ogrenci2.sessions import SESSIONS_DIR


REPORT_DIR = PATHS["reports"].with_name("ogrenci2")
FUNNEL_STAGES = (("view", "n_views"), ("cart", "n_carts"), ("purchase", "n_purchases"))


def rate(numerator, denominator):
    return numerator / denominator if denominator else None


def funnel_aggregations():
    # Bağımsız erişimler ve tabanı view olan iç içe huni aynı anda sayılır.
    view, cart, purchase = (F.col(column) > 0 for _, column in FUNNEL_STAGES)
    return [
        F.count(F.when(view, 1)).alias("view"),
        F.count(F.when(cart, 1)).alias("cart"),
        F.count(F.when(purchase, 1)).alias("purchase"),
        F.count(F.when(view & cart, 1)).alias("view_cart"),
        F.count(F.when(view & cart & purchase, 1)).alias("view_cart_purchase"),
    ]


def funnel_rows(size, counts):
    stages = [counts[key] for key in ("view", "view_cart", "view_cart_purchase")]
    return [
        {
            "boyut": size,
            "asama": stage,
            "bagimsiz_sayi": counts[stage],
            "huni_sayisi": count,
            "onceki_asamadan_oran": rate(count, stages[index - 1]) if index else None,
            "view_asamasindan_oran": rate(count, stages[0]),
        }
        for index, ((stage, _), count) in enumerate(zip(FUNNEL_STAGES, stages))
    ]


def user_funnel(sessions):
    # Kullanıcı farklı oturumlarda ulaştığı aşamaları birleştirebilir.
    users = sessions.groupBy("user_id").agg(*(F.max(column).alias(column) for _, column in FUNNEL_STAGES))
    return users.agg(F.count("*").alias("kullanici_sayisi"), *funnel_aggregations()).first().asDict()


def event_rates(size, totals):
    view, cart, purchase, remove = (totals[f"olay_{key}"] for key in ("view", "cart", "purchase", "remove"))
    return {
        "boyut": size, "view": view, "cart": cart, "purchase": purchase, "remove_from_cart": remove,
        "cart_view_orani": rate(cart, view),
        "purchase_cart_orani": rate(purchase, cart),
        "purchase_view_orani": rate(purchase, view),
        "remove_cart_orani": rate(remove, cart),
    }


def cart_profiles(sessions, size, cart_count):
    # Sepeti terk eden oturumda sepete ekleme var ve hiç satın alma yok.
    profiles = sessions.filter(F.col("n_carts") > 0).groupBy("reached_purchase").agg(
        F.count("*").alias("oturum_sayisi"),
        F.avg("duration_sec").alias("ort_sure_sn"),
        F.percentile_approx("duration_sec", 0.5, 10000).alias("medyan_sure_sn"),
        F.avg("n_events").alias("ort_olay_sayisi"),
        F.avg("n_products").alias("ort_urun_sayisi"),
        F.avg("n_carts").alias("ort_sepete_ekleme"),
        F.avg((F.col("n_removes") > 0).cast("int")).alias("sepetten_cikarma_gorulen_oturum_orani"),
    )
    measured = {row.reached_purchase: row.asDict() for row in profiles.collect()}
    fields = [column for column in profiles.columns if column != "reached_purchase"]
    rows = []
    for purchase, label in ((0, "terk_eden"), (1, "satin_alan")):
        values = {field: measured.get(purchase, {}).get(field) for field in fields}
        values["oturum_sayisi"] = values["oturum_sayisi"] or 0
        rows.append({
            "boyut": size, "grup": label, **values,
            "sepetli_oturumlar_icindeki_oran": rate(values["oturum_sayisi"], cart_count),
        })
    return rows


def analyze_sample(spark, size, source, measurement):
    sessions = spark.read.parquet(str(source)).select(
        "user_id", "duration_sec", "n_products", "n_events", "n_views", "n_carts", "n_removes",
        "n_purchases", "reached_purchase",
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        totals = sessions.agg(
            F.count("*").alias("oturum_sayisi"),
            F.avg("duration_sec").alias("ort_oturum_suresi_sn"),
            F.count(F.when((F.col("n_carts") > 0) & (F.col("n_removes") > 0), 1)).alias("sepet_ve_cikarma"),
            *funnel_aggregations(),
            *(F.coalesce(F.sum(column), F.lit(0)).alias(f"olay_{key}") for key, column in (
                ("view", "n_views"), ("cart", "n_carts"), ("purchase", "n_purchases"),
                ("remove", "n_removes"), ("toplam", "n_events"),
            )),
        ).first().asDict()
        users = user_funnel(sessions)
        carts = cart_profiles(sessions, size, totals["cart"])
        distribution = [
            {"boyut": size, "olay_sayisi": row.n_events, "oturum_sayisi": row["count"]}
            for row in sessions.groupBy("n_events").count().orderBy("n_events").collect()
        ]
    finally:
        sessions.unpersist()

    summary = {
        "boyut": size,
        "olay_sayisi": totals["olay_toplam"],
        "oturum_sayisi": totals["oturum_sayisi"],
        "kullanici_sayisi": users["kullanici_sayisi"],
        "ort_oturum_suresi_sn": totals["ort_oturum_suresi_sn"],
        "satin_alan_oturum_orani": rate(totals["purchase"], totals["oturum_sayisi"]),
        "huni_view_satin_alma_orani": rate(totals["view_cart_purchase"], totals["view"]),
        "sepet_terki_orani": carts[0]["sepetli_oturumlar_icindeki_oran"],
        "sepetten_cikarma_gorulen_oturum_orani": rate(totals["sepet_ve_cikarma"], totals["cart"]),
        **measurement,
    }
    print(f"[{size}] {totals['olay_toplam']:,} olay, {totals['oturum_sayisi']:,} oturum", flush=True)
    return {
        "huni_oturum": funnel_rows(size, totals),
        "huni_kullanici": funnel_rows(size, users),
        "olay_oranlari": [event_rates(size, totals)],
        "oturum_ozeti": [summary],
        "sepet_terki": carts,
        "oturum_olay_dagilimi": distribution,
    }


def write_csv(name, rows):
    fields = list(rows[0]) if rows else ["boyut", "olay_sayisi", "oturum_sayisi"]
    with (REPORT_DIR / f"{name}.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def plot_cart_profiles(rows):
    figure = Figure(figsize=(8, 4.5), layout="constrained")
    ax = figure.subplots()
    labels = [row["boyut"] for row in rows if row["grup"] == "terk_eden"]
    bottom = [0] * len(labels)
    groups = (("terk_eden", "Sepeti terk eden", "#9A4329"), ("satin_alan", "Satın alan", "#226181"))
    for group, label, color in groups:
        percents = [100 * (row["sepetli_oturumlar_icindeki_oran"] or 0) for row in rows if row["grup"] == group]
        bars = ax.bar(labels, percents, bottom=bottom, label=label, color=color)
        ax.bar_label(bars, labels=[f"%{p:.1f}".replace(".", ",") if p else "" for p in percents],
                     label_type="center", color="white", weight="bold")
        bottom = [previous + current for previous, current in zip(bottom, percents)]
    ax.set(title="Sepete ürün eklenen oturumların sonucu", ylabel="Sepetli oturumların yüzdesi", ylim=(0, 100))
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.10), ncol=2, frameon=False)
    figure.savefig(REPORT_DIR / "sepet_terki.png", dpi=150)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", choices=list(SAMPLE_SIZES), default=list(SAMPLE_SIZES))
    args = parser.parse_args()
    inputs = {}
    for size in dict.fromkeys(args.sizes):
        source = SESSIONS_DIR / size / "sessions.parquet"
        measurement = source.with_name("olcum.json")
        if not (source / "_SUCCESS").exists() or not measurement.is_file():
            parser.error(f"{size} çıktısı eksik. Önce python -m src.ogrenci2.sessions --sizes {size} çalıştırın.")
        inputs[size] = (source, json.loads(measurement.read_text(encoding="utf-8")))

    reports = {}
    spark = get_spark("BVA-Ogrenci2-Donusum")
    try:
        for size, (source, measurement) in inputs.items():
            for name, rows in analyze_sample(spark, size, source, measurement).items():
                reports.setdefault(name, []).extend(rows)
    finally:
        spark.stop()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for name, rows in reports.items():
        write_csv(name, rows)
    plot_cart_profiles(reports["sepet_terki"])


if __name__ == "__main__":
    main()
