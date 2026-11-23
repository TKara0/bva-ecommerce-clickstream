"""
Adım 1: Ham CSV -> temizlenmiş, olay modeli eklenmiş Parquet

Yapılanlar:
  1. data/raw/*.csv dosyalarını açık şemayla oku (tüm aylar tek DataFrame)
  2. event_time'ı timestamp'e çevir, türetilmiş zaman kolonlarını ekle
  3. Olay modelini uygula (funnel_step, is_view/is_cart/... bayrakları)
  4. category_code'u seviyelere böl (ör. "appliances.kitchen.blender")
  5. Temel kalite kontrolleri: geçersiz olay tipi, boş oturum, negatif fiyat,
     birebir tekrar eden satır -> sayıları rapora yazılır
  6. Çıktılar:
       data/processed/events_full/   (event_month'a göre partition'lı Parquet)
       data/processed/dim_product/   (ürün boyut tablosu)
       reports/ogrenci1/kalite_ozeti.csv

Çalıştırma:  python -m src.ogrenci1.prepare_data
"""
import time

from pyspark.sql import functions as F

from src.ogrenci1.common import EVENT_MODEL, PATHS, RAW_SCHEMA, VALID_EVENTS, get_spark


def read_raw(spark):
    files = sorted(str(p) for p in PATHS["raw"].glob("*.csv"))
    if not files:
        raise SystemExit(
            f"{PATHS['raw']} altında CSV yok. Önce scripts/download_data.sh çalıştır."
        )
    print(f"[oku] {len(files)} dosya:", *[f.split('/')[-1] for f in files])
    return spark.read.csv(files, header=True, schema=RAW_SCHEMA)


def add_time_columns(df):
    # "2019-10-01 00:00:00 UTC" -> timestamp
    ts = F.to_timestamp(F.regexp_replace("event_time", " UTC$", ""), "yyyy-MM-dd HH:mm:ss")
    return (
        df.withColumn("event_time", ts)
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_month", F.date_format("event_time", "yyyy-MM"))
        .withColumn("event_hour", F.hour("event_time"))
        # dayofweek: 1=Pazar ... 7=Cumartesi (Spark varsayılanı)
        .withColumn("event_dow", F.dayofweek("event_time"))
    )


def add_event_model(df):
    step_map = F.create_map(*[x for k, v in EVENT_MODEL.items() for x in (F.lit(k), F.lit(v[0]))])
    stage_map = F.create_map(*[x for k, v in EVENT_MODEL.items() for x in (F.lit(k), F.lit(v[1]))])
    return (
        df.withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("funnel_step", step_map[F.col("event_type")].cast("int"))
        .withColumn("funnel_stage", stage_map[F.col("event_type")])
        .withColumn("is_view", (F.col("event_type") == "view").cast("int"))
        .withColumn("is_cart", (F.col("event_type") == "cart").cast("int"))
        .withColumn("is_remove", (F.col("event_type") == "remove_from_cart").cast("int"))
        .withColumn("is_purchase", (F.col("event_type") == "purchase").cast("int"))
    )


def add_category_levels(df):
    # "accessories.bag" gibi 2 seviyeli kodlar da var; get() eksik seviyede NULL döner
    # (Spark 4'te ANSI modu açık, getItem taşınca hata veriyor)
    parts = F.split(F.col("category_code"), r"\.")
    return (
        df.withColumn("category_l1", F.get(parts, 0))
        .withColumn("category_l2", F.get(parts, 1))
        .withColumn("category_l3", F.get(parts, 2))
        .withColumn("brand", F.lower(F.trim("brand")))
    )


def quality_flags(df):
    """Satırı silmeden önce neden silineceğini işaretliyoruz; böylece raporda
    kaç satırın hangi sebeple gittiği görülebiliyor."""
    return (
        df.withColumn("q_bad_time", F.col("event_time").isNull())
        .withColumn("q_bad_event", ~F.col("event_type").isin(VALID_EVENTS) | F.col("event_type").isNull())
        .withColumn("q_null_user", F.col("user_id").isNull() | F.col("product_id").isNull())
        .withColumn("q_null_session", F.col("user_session").isNull())
        .withColumn("q_neg_price", F.col("price") < 0)
    )


def main():
    t0 = time.time()
    spark = get_spark()
    PATHS["reports"].mkdir(parents=True, exist_ok=True)

    raw = read_raw(spark)
    df = quality_flags(add_category_levels(add_event_model(add_time_columns(raw))))
    df = df.cache()

    q = df.agg(
        F.count("*").alias("toplam_satir"),
        F.sum(F.col("q_bad_time").cast("int")).alias("gecersiz_zaman"),
        F.sum(F.col("q_bad_event").cast("int")).alias("gecersiz_olay_tipi"),
        F.sum(F.col("q_null_user").cast("int")).alias("bos_user_veya_urun"),
        F.sum(F.col("q_null_session").cast("int")).alias("bos_oturum"),
        F.sum(F.col("q_neg_price").cast("int")).alias("negatif_fiyat"),
    ).first().asDict()

    # Silme kuralları:
    #  - zaman / olay tipi / user / ürün yoksa satır analiz edilemez -> sil
    #  - negatif fiyat -> sil (az sayıda, hatalı kayıt)
    #  - boş oturum -> SİLMİYORUZ, user_id ile oturum yeniden kurulabilir (Öğrenci 2)
    clean = df.filter(~(F.col("q_bad_time") | F.col("q_bad_event") | F.col("q_null_user") | F.col("q_neg_price")))
    before_dedup = clean.count()
    clean = clean.dropDuplicates(["event_time", "event_type", "product_id", "user_id", "user_session"])
    after_dedup = clean.count()

    q["tekrar_eden_satir"] = before_dedup - after_dedup
    q["temiz_satir"] = after_dedup
    q["silinen_oran_%"] = round(100 * (q["toplam_satir"] - after_dedup) / max(q["toplam_satir"], 1), 3)

    final_cols = [
        "event_time", "event_date", "event_month", "event_hour", "event_dow",
        "event_type", "funnel_step", "funnel_stage",
        "is_view", "is_cart", "is_remove", "is_purchase",
        "product_id", "category_id", "category_code",
        "category_l1", "category_l2", "category_l3", "brand", "price",
        "user_id", "user_session",
    ]
    events = clean.select(*final_cols)

    print("[yaz] events_full (Parquet, event_month partition)")
    events.repartition("event_month").write.mode("overwrite").partitionBy("event_month").parquet(str(PATHS["events"]))

    # Ürün boyut tablosu: her ürün için en son görülen kategori/marka/fiyat.
    # Öğrenci 3'ün ürün-kategori analizlerinde join için kullanılabilir.
    dim_product = (
        events.groupBy("product_id")
        .agg(
            F.max_by("category_id", "event_time").alias("category_id"),
            F.max_by("category_code", "event_time").alias("category_code"),
            F.max_by("brand", "event_time").alias("brand"),
            F.max_by("price", "event_time").alias("last_price"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.count("*").alias("event_count"),
        )
    )
    dim_product.write.mode("overwrite").parquet(str(PATHS["dim_product"]))

    q["sure_sn"] = round(time.time() - t0, 1)
    spark.createDataFrame([q]).toPandas().T.rename(columns={0: "deger"}).to_csv(
        PATHS["reports"] / "kalite_ozeti.csv", index_label="metrik"
    )
    print("[kalite]", q)
    spark.stop()


if __name__ == "__main__":
    main()
