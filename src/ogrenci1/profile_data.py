"""
Adım 3: Veri sözlüğü, olay dağılımı ve CSV/Parquet boyut-okuma karşılaştırması

Çıktılar:
  docs/veri_sozlugu.md                       (otomatik üretilen veri sözlüğü)
  reports/ogrenci1/veri_sozlugu.csv
  reports/ogrenci1/olay_dagilimi_aylik.csv   + olay_dagilimi.png
  reports/ogrenci1/format_karsilastirma.csv  + format_boyut.png, format_sure.png

Not: Buradaki süre ölçümü veri hazırlama aşamasının "ilk bakış" ölçümüdür.
Pandas/Spark ve farklı boyutlardaki ayrıntılı performans karşılaştırması
Öğrenci 5'in kısmında yapılıyor.

Çalıştırma:  python -m src.ogrenci1.profile_data
"""
import statistics
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from src.ogrenci1.common import PATHS, SAMPLE_SIZES, get_spark

ACIKLAMA = {
    "event_time": "Olayın zamanı (UTC)",
    "event_date": "Olay tarihi (türetilmiş)",
    "event_month": "Olay ayı yyyy-MM (türetilmiş, partition kolonu)",
    "event_hour": "Olay saati 0-23, UTC (türetilmiş)",
    "event_dow": "Haftanın günü, 1=Pazar ... 7=Cumartesi (türetilmiş)",
    "event_type": "Olay tipi: view / cart / remove_from_cart / purchase",
    "funnel_step": "Huni adımı: 1=görüntüleme, 2=sepet, 3=satın alma (olay modeli)",
    "funnel_stage": "Huni aşamasının adı (olay modeli)",
    "is_view": "Olay görüntüleme mi? 0/1",
    "is_cart": "Olay sepete ekleme mi? 0/1",
    "is_remove": "Olay sepetten çıkarma mı? 0/1",
    "is_purchase": "Olay satın alma mı? 0/1",
    "product_id": "Ürün kimliği",
    "category_id": "Kategori kimliği",
    "category_code": "Kategori ağacı kodu (ör. appliances.kitchen.blender); çoğu satırda boş",
    "category_l1": "Kategori 1. seviye (category_code'dan türetilmiş)",
    "category_l2": "Kategori 2. seviye (türetilmiş)",
    "category_l3": "Kategori 3. seviye (türetilmiş)",
    "brand": "Marka adı (küçük harfe çevrilmiş); boş olabilir",
    "price": "Ürün fiyatı",
    "user_id": "Kalıcı kullanıcı kimliği",
    "user_session": "Geçici oturum kimliği; kullanıcı uzun aradan sonra dönünce değişir",
}


def data_dictionary(df) -> pd.DataFrame:
    total = df.count()
    cols = [c for c in df.columns]
    agg_exprs = []
    for c in cols:
        agg_exprs += [
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__null"),
            F.approx_count_distinct(c).alias(f"{c}__distinct"),
            F.min(c).cast("string").alias(f"{c}__min"),
            F.max(c).cast("string").alias(f"{c}__max"),
        ]
    r = df.agg(*agg_exprs).first().asDict()
    sample = df.limit(1000).toPandas()
    dtypes = dict(df.dtypes)

    rows = []
    for c in cols:
        ornek = sample[c].dropna()
        rows.append({
            "kolon": c,
            "tip": dtypes[c],
            "bos_%": round(100 * r[f"{c}__null"] / total, 2),
            "farkli_deger_yaklasik": r[f"{c}__distinct"],
            "min": r[f"{c}__min"],
            "max": r[f"{c}__max"],
            "ornek": str(ornek.iloc[0]) if len(ornek) else "",
            "aciklama": ACIKLAMA.get(c, ""),
        })
    return pd.DataFrame(rows), total


def dir_size_mb(p: Path) -> float:
    if p.is_file():
        return p.stat().st_size / 1e6
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e6


def timed(fn, repeat=3):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return round(statistics.median(times), 3)


def format_comparison(spark) -> pd.DataFrame:
    from src.ogrenci1.common import RAW_SCHEMA  # noqa: F401  (şema referansı için)
    rows = []
    for name in SAMPLE_SIZES:
        d = PATHS["samples"] / name
        if not d.exists():
            continue
        csv_path, pq_path = d / "events.csv", d / "events.parquet"
        # CSV için şemayı Parquet'ten alıyoruz ki iki taraf da inferSchema'sız, adil okunsun
        schema = spark.read.parquet(str(pq_path)).schema
        readers = {
            "CSV": lambda: spark.read.csv(str(csv_path), header=True, schema=schema),
            "Parquet": lambda: spark.read.parquet(str(pq_path)),
        }
        for fmt, rd in readers.items():
            spark.catalog.clearCache()
            rows.append({
                "orneklem": name,
                "format": fmt,
                "disk_mb": round(dir_size_mb(csv_path if fmt == "CSV" else pq_path), 2),
                "count_sn": timed(lambda: rd().count()),
                # tek kolon sorgusu: Parquet'te column pruning etkisini görmek için
                "tek_kolon_ort_fiyat_sn": timed(lambda: rd().agg(F.avg("price")).collect()),
                "groupby_olay_tipi_sn": timed(lambda: rd().groupBy("event_type").count().collect()),
            })
            print("[format]", rows[-1])
    return pd.DataFrame(rows)


def plot_bars(df, value, title, ylabel, out):
    piv = df.pivot(index="orneklem", columns="format", values=value).reindex(
        [s for s in SAMPLE_SIZES if s in df["orneklem"].unique()]
    )
    ax = piv.plot(kind="bar", figsize=(7, 4), rot=0, color=["#9aa0a6", "#1a73e8"])
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Örneklem")
    for c in ax.containers:
        ax.bar_label(c, fmt="%.2f", fontsize=8)
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()


def main():
    spark = get_spark()
    rep = PATHS["reports"]
    rep.mkdir(parents=True, exist_ok=True)
    events = spark.read.parquet(str(PATHS["events"]))

    # 1) Veri sözlüğü
    dd, total = data_dictionary(events)
    dd.to_csv(rep / "veri_sozlugu.csv", index=False)
    md = ["# Veri Sözlüğü (otomatik üretildi)\n",
          f"Kaynak: Kaggle – eCommerce Events History in Cosmetics Shop (REES46). "
          f"Temizlenmiş toplam satır: **{total:,}**\n",
          dd.to_markdown(index=False)]
    (PATHS["docs"] / "veri_sozlugu.md").write_text("\n".join(md), encoding="utf-8")

    # 2) Aylık olay tipi dağılımı
    dist = (
        events.groupBy("event_month", "event_type").count()
        .toPandas().pivot(index="event_month", columns="event_type", values="count")
        .fillna(0).astype(int).sort_index()
    )
    dist["toplam"] = dist.sum(axis=1)
    dist.to_csv(rep / "olay_dagilimi_aylik.csv")
    order = [c for c in ["view", "cart", "remove_from_cart", "purchase"] if c in dist.columns]
    ax = (dist[order] / 1e6).plot(kind="bar", figsize=(8, 4), rot=0,
                                  color=["#1a73e8", "#34a853", "#fbbc04", "#ea4335"])
    ax.set_title("Aylara göre olay tipi dağılımı", pad=28)
    ax.set_ylabel("Olay sayısı (milyon)")
    ax.set_xlabel("Ay")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=4, frameon=False, fontsize=9)
    plt.tight_layout()
    plt.savefig(rep / "olay_dagilimi.png", dpi=150)
    plt.close()

    # 3) Ham CSV vs temiz Parquet (tam veri) boyutu
    raw_mb = sum(f.stat().st_size for f in PATHS["raw"].glob("*.csv")) / 1e6
    full_pq_mb = dir_size_mb(PATHS["events"])
    pd.DataFrame([{
        "ham_csv_mb": round(raw_mb, 1),
        "temiz_parquet_mb": round(full_pq_mb, 1),
        "sikistirma_orani": round(raw_mb / full_pq_mb, 2) if full_pq_mb else None,
        "satir": total,
    }]).to_csv(rep / "tam_veri_boyut.csv", index=False)

    # 4) Örneklemlerde CSV vs Parquet
    fc = format_comparison(spark)
    if not fc.empty:
        fc.to_csv(rep / "format_karsilastirma.csv", index=False)
        plot_bars(fc, "disk_mb", "Disk boyutu: CSV vs Parquet", "MB", rep / "format_boyut.png")
        plot_bars(fc, "tek_kolon_ort_fiyat_sn", "Tek kolon sorgusu (avg(price)) süresi",
                  "saniye (3 tekrarın medyanı)", rep / "format_sure.png")
    spark.stop()
    print("[bitti] raporlar:", rep)


if __name__ == "__main__":
    main()
