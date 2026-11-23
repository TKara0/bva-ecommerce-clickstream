"""
Adım 2: 100K / 1M / 5M kayıtlık deney örneklemleri

Neden rastgele satır örneklemi DEĞİL?
  df.sample() satırları tek tek seçer; bir kullanıcının "view" olayı örnekleme
  girip aynı oturumdaki "purchase" olayı girmeyebilir. Bu durumda Öğrenci 2'nin
  oturum / dönüşüm hesapları bozulur (dönüşüm oranı yapay olarak düşer).

Bunun yerine KULLANICI BAZLI örneklem alıyoruz:
  1. Her kullanıcının olay sayısı çıkarılır.
  2. Kullanıcılar sabit tohumlu (seed) bir hash'e göre karıştırılır.
  3. Bu sırayla kümülatif olay sayısı N'yi geçmeyene kadar kullanıcı alınır.
  -> Seçilen kullanıcının TÜM geçmişi örneklemde olur, oturumlar bütün kalır.
  -> Örneklemler iç içedir: 100k ⊂ 1m ⊂ 5m. Performans karşılaştırmasında
     (Öğrenci 5) farkın sadece veri boyutundan geldiğini söyleyebiliriz.
  -> Aynı seed ile herkes aynı örneklemi üretir (tekrarlanabilirlik).

Çıktılar (her boyut için):
  data/processed/samples/<boyut>/events.parquet/   (tek dosyalı Parquet klasörü)
  data/processed/samples/<boyut>/events.csv        (tek CSV dosyası, Pandas için)
  reports/ogrenci1/orneklem_ozeti.csv

Çalıştırma:  python -m src.ogrenci1.make_samples [--seed 42]
"""
import argparse
import shutil
import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.ogrenci1.common import PATHS, SAMPLE_SIZES, get_spark


def write_single_csv(df, target: Path):
    """Spark CSV'yi klasör olarak yazar; Pandas kullanacaklar için tek dosyaya indiriyoruz."""
    tmp = target.with_suffix(".tmp_dir")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp))
    part = next(tmp.glob("part-*.csv"))
    if target.exists():
        target.unlink()
    shutil.move(str(part), str(target))
    shutil.rmtree(tmp)


def main(seed: int):
    spark = get_spark()
    events = spark.read.parquet(str(PATHS["events"]))
    total = events.count()
    print(f"[bilgi] tam veri: {total:,} satır")

    # Kullanıcı başına olay sayısı + sabit tohumlu karıştırma anahtarı
    users = (
        events.groupBy("user_id").agg(F.count("*").alias("n_events"))
        .withColumn("shuffle_key", F.xxhash64(F.col("user_id"), F.lit(seed)))
    )
    # Kullanıcı tablosu olay tablosundan çok küçük, tek partition'da pencere almak sorun değil
    w = Window.orderBy("shuffle_key", "user_id").rowsBetween(Window.unboundedPreceding, 0)
    users = users.withColumn("cum_events", F.sum("n_events").over(w)).cache()
    users.count()

    rows = []
    for name, n in SAMPLE_SIZES.items():
        t0 = time.time()
        if n > total:
            print(f"[uyarı] {name}: tam veri ({total:,}) hedeften küçük, atlandı. "
                  f"Daha fazla ay indir ya da Multi Category setini ekle.")
            continue

        chosen = users.filter(F.col("cum_events") <= n).select("user_id")
        sample = events.join(F.broadcast(chosen), "user_id").select(events.columns).cache()
        cnt = sample.count()

        out_dir = PATHS["samples"] / name
        out_dir.mkdir(parents=True, exist_ok=True)
        sample.coalesce(1).write.mode("overwrite").parquet(str(out_dir / "events.parquet"))
        write_single_csv(sample, out_dir / "events.csv")

        stats = sample.agg(
            F.countDistinct("user_id").alias("kullanici"),
            F.countDistinct("user_session").alias("oturum"),
            F.countDistinct("product_id").alias("urun"),
            F.sum("is_view").alias("view"),
            F.sum("is_cart").alias("cart"),
            F.sum("is_remove").alias("remove_from_cart"),
            F.sum("is_purchase").alias("purchase"),
        ).first().asDict()
        rows.append({"orneklem": name, "hedef": n, "gercek_satir": cnt, **stats,
                     "sure_sn": round(time.time() - t0, 1)})
        print(f"[ok] {name}: {cnt:,} satır, {stats['kullanici']:,} kullanıcı")
        sample.unpersist()

    import pandas as pd
    PATHS["reports"].mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(PATHS["reports"] / "orneklem_ozeti.csv", index=False)
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    main(ap.parse_args().seed)
