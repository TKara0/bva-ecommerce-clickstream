"""Öğrenci 3 – Spark ile ürün, kategori, saat ve müşteri segmenti (RFM) analizi.

Araştırma sorusuna katkı: dönüşümü hangi ürün, kategori, marka, saat ve müşteri segmenti
taşıyor, hangisi kaybettiriyor?

Çalıştırma (repo kökünden):
    python -m src.ogrenci3.analiz                              # geliştirme: sadece 100k
    python -m src.ogrenci3.analiz --sizes 100k 1m 5m --repeat 3

Çıktılar:
    reports/ogrenci3/top_urunler.csv, firsat_urunleri.csv, kategori_donusum.csv,
                     marka_donusum.csv, saat_gun.csv, rfm_segmentler.csv, sureler.csv,
                     join_plani.txt ve grafikler (*.png)
    data/processed/rfm/<boyut>/rfm.parquet   (Öğrenci 4 için: user_id, recency, frequency,
                                               monetary, segment + R/F/M skorları)

Kullanılan ileri analitik: aggregation, join (ürün boyut tablosu), window
(rank ile top-N, percent_rank ile RFM çeyrek skorları, sum over() ile paylar).
"""
import argparse
import contextlib
import io
import statistics
import time

import pandas as pd
from matplotlib.figure import Figure
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.ogrenci1.common import PATHS, SAMPLE_SIZES, get_spark

REPORT_DIR = PATHS["reports"].with_name("ogrenci3")
RFM_DIR = PATHS["samples"].with_name("rfm")
REFERANS_TARIH = "2020-02-29"          # verinin son günü; recency buna göre hesaplanır
TOP_N = 10
MIN_VIEW_HUCRE = 100                   # ısı haritasında dönüşüm gösterilecek en az görüntüleme
GUNLER = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]

# Sadece gereken kolonlar okunur (Parquet column pruning: diğer kolonlar diskten hiç okunmaz)
KOLONLAR = ["event_time", "event_hour", "event_dow", "is_view", "is_cart", "is_remove",
            "is_purchase", "product_id", "category_id", "category_code", "brand", "price",
            "user_id", "user_session"]

# Grafik renkleri (açık zemin)
MAVI, TURUNCU, MUREKKEP, MUREKKEP_2, CIZGI = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df"


# --------------------------------------------------------------------------------------
# Ortak metrikler
# --------------------------------------------------------------------------------------
def huni_metrikleri(kullanici_say=True):
    """Her boyutta aynı metrik seti, tek groupBy geçişinde (tek shuffle) hesaplanır.
    is_* bayrakları Öğrenci 1'in olay modelinden geliyor: sum(is_purchase) = satın alma sayısı."""
    metrikler = [
        F.sum("is_view").alias("view"),
        F.sum("is_cart").alias("cart"),
        F.sum("is_remove").alias("remove_from_cart"),
        F.sum("is_purchase").alias("purchase"),
        F.round(F.sum(F.when(F.col("is_purchase") == 1, F.col("price")).otherwise(0.0)), 2).alias("gelir"),
    ]
    if kullanici_say:
        # Tekil kullanıcı sayımı pahalıdır (her grup için kullanıcı kümesi tutulur);
        # kardinalitesi düşük boyutlarda (kategori, marka) yapılır, üründe yapılmaz.
        metrikler += [
            F.countDistinct(F.when(F.col("is_view") == 1, F.col("user_id"))).alias("goruntuleyen_kullanici"),
            F.countDistinct(F.when(F.col("is_purchase") == 1, F.col("user_id"))).alias("alan_kullanici"),
        ]
    return metrikler


def oranlar(df, kullanici_say=True):
    def oran(pay, payda):
        return F.when(F.col(payda) > 0, F.col(pay) / F.col(payda))

    df = (df.withColumn("sepet_orani", oran("cart", "view"))                  # view -> cart
            .withColumn("sepetten_alim_orani", oran("purchase", "cart"))      # cart -> purchase
            .withColumn("donusum", oran("purchase", "view")))                 # view -> purchase
    if kullanici_say:
        df = df.withColumn("kullanici_donusumu", oran("alan_kullanici", "goruntuleyen_kullanici"))
    # Window (bölümsüz): toplam içindeki gelir payı. Küçük, toplanmış tablo üzerinde çalışır.
    return df.withColumn("gelir_payi", F.col("gelir") / F.sum("gelir").over(Window.partitionBy()))


# --------------------------------------------------------------------------------------
# 1) Ürün
# --------------------------------------------------------------------------------------
def urun_boyut_tablosu(spark, events):
    """Ürün boyut tablosu. Öğrenci 1'in dim_product'ı varsa o, yoksa aynı tanımla örneklemden üretilir
    (ürünün en son görülen kategori / marka / fiyatı)."""
    if (PATHS["dim_product"] / "_SUCCESS").exists():
        dim = spark.read.parquet(str(PATHS["dim_product"]))
    else:
        dim = events.groupBy("product_id").agg(
            F.max_by("category_id", "event_time").alias("category_id"),
            F.max_by("category_code", "event_time").alias("category_code"),
            F.max_by("brand", "event_time").alias("brand"),
            F.max_by("price", "event_time").alias("last_price"),
        )
    return dim.select("product_id", "category_id", "category_code", "brand", "last_price")


def urun_analizi(events, dim):
    urun = oranlar(events.groupBy("product_id").agg(*huni_metrikleri(False)), False)
    # JOIN: ürün metrikleri + ürün boyut tablosu (kategori, marka, fiyat etiketleri)
    urun = urun.join(dim, on="product_id", how="left")

    # WINDOW: dört farklı listede rank ile top-N. Listeler alt alta konup "liste" ile
    # bölümlenir; böylece tek window işi yeterli olur.
    listeler = {"en_cok_goruntulenen": "view", "en_cok_sepete_eklenen": "cart",
                "en_cok_satin_alinan": "purchase", "en_cok_gelir": "gelir"}
    uzun = None
    for liste, kolon in listeler.items():
        parca = urun.withColumn("liste", F.lit(liste)).withColumn("deger", F.col(kolon).cast("double"))
        uzun = parca if uzun is None else uzun.unionByName(parca)
    w = Window.partitionBy("liste").orderBy(F.desc("deger"), "product_id")
    top = (uzun.withColumn("sira", F.rank().over(w)).filter(F.col("sira") <= TOP_N)
           .select("liste", "sira", "product_id", "category_id", "brand", "last_price", "view", "cart",
                   "purchase", "gelir", "donusum", "sepet_orani", "sepetten_alim_orani"))
    return urun, top


def firsat_urunleri(urun, esik_yuzdelik=0.90, adet=20):
    """'Çok bakılıp az alınan' ürünler. Az görüntülenen ürünlerin oranı gürültülüdür
    (3 view 1 purchase = %33), bu yüzden sadece en çok görüntülenen %10'luk dilim karşılaştırılır."""
    esik = urun.approxQuantile("view", [esik_yuzdelik], 0.001)[0]
    populer = urun.filter(F.col("view") >= esik)
    medyan = populer.approxQuantile("donusum", [0.5], 0.001)[0]
    return (populer.filter(F.col("donusum") < medyan)
            # Dönüşüm medyana çıksaydı kazanılacak ek satış (fırsatın büyüklüğü)
            .withColumn("potansiyel_ek_satis", F.round(F.col("view") * F.lit(medyan) - F.col("purchase"), 1))
            .withColumn("view_esigi", F.lit(esik)).withColumn("populer_medyan_donusum", F.lit(medyan))
            .orderBy(F.desc("potansiyel_ek_satis")).limit(adet)
            .select("product_id", "category_id", "brand", "last_price", "view", "cart", "purchase",
                    "donusum", "populer_medyan_donusum", "potansiyel_ek_satis", "view_esigi"))


# --------------------------------------------------------------------------------------
# 2) Kategori ve marka
# --------------------------------------------------------------------------------------
def kategori_analizi(events):
    # category_code satırların %98'inde boş -> gruplama category_id ile yapılır.
    # Kod etiketi ana aggregation'a first(...) olarak KONMAZ: string tipli ara değer (buffer)
    # Spark'ın hızlı HashAggregate'ini kullanamamasına, sıralama tabanlı aggregation'a düşmesine
    # yol açıyor (5m'de 43 sn -> ölçüldü). Etiket, sadece dolu satırlardan küçük bir tabloda
    # hesaplanıp JOIN ile eklenir.
    etiket = (events.filter(F.col("category_code").isNotNull())
              .groupBy("category_id").agg(F.max("category_code").alias("category_code")))
    metrik = events.groupBy("category_id").agg(F.countDistinct("product_id").alias("urun_sayisi"),
                                                *huni_metrikleri())
    return (oranlar(metrik).join(etiket, on="category_id", how="left")
            .select("category_id", "category_code", *[c for c in oranlar(metrik).columns
                                                      if c != "category_id"])
            .orderBy(F.desc("view")))


def marka_analizi(events):
    # brand %42 boş -> boşlar tek grup "(bos)" olarak tutulur, kaybolmaz
    return oranlar(events.withColumn("brand", F.coalesce("brand", F.lit("(bos)")))
                   .groupBy("brand").agg(F.countDistinct("product_id").alias("urun_sayisi"),
                                         *huni_metrikleri())).orderBy(F.desc("view"))


# --------------------------------------------------------------------------------------
# 3) Zaman: haftanın günü x saat (UTC)
# --------------------------------------------------------------------------------------
def saat_gun_analizi(events):
    # event_dow: 1=Pazar ... 7=Cumartesi  ->  gun_no: 1=Pazartesi ... 7=Pazar
    return oranlar(events.withColumn("gun_no", ((F.col("event_dow") + 5) % 7) + 1)
                   .groupBy("gun_no", F.col("event_hour").alias("saat"))
                   .agg(*huni_metrikleri(False)), False).orderBy("gun_no", "saat")


# --------------------------------------------------------------------------------------
# 4) Müşteri segmenti: RFM
# --------------------------------------------------------------------------------------
def ceyrek_skoru(kolon, kucuk_iyi=False):
    """WINDOW: percent_rank ile 1-4 çeyrek skoru (4 = en iyi).
    ntile yerine percent_rank: ntile aynı değerdeki müşterileri (ör. hepsi 1 kez alan) keyfi olarak
    farklı gruplara böler; percent_rank'te eşit değerler hep aynı skoru alır."""
    sira = F.col(kolon).desc() if kucuk_iyi else F.col(kolon).asc()
    return F.least(F.lit(4), F.floor(F.percent_rank().over(Window.orderBy(sira)) * 4) + 1).cast("int")


def rfm_analizi(events):
    alim = F.col("is_purchase") == 1
    # Sipariş ~ satın alma yapılan oturum (bir oturumda birden çok ürün alınabiliyor)
    oturum = F.coalesce(F.col("user_session"), F.date_format("event_time", "yyyy-MM-dd"))
    kullanici = events.groupBy("user_id").agg(
        F.max(F.when(alim, F.col("event_time"))).alias("son_alim"),
        F.countDistinct(F.when(alim, oturum)).alias("frequency"),
        F.round(F.sum(F.when(alim, F.col("price")).otherwise(0.0)), 2).alias("monetary"),
        F.sum("is_purchase").alias("alinan_urun"),
        F.sum("is_cart").alias("sepete_ekleme"),
        F.sum("is_view").alias("goruntuleme"),
    ).withColumn("recency", F.datediff(F.to_date(F.lit(REFERANS_TARIH)), F.to_date("son_alim")))

    alicilar = (kullanici.filter(F.col("alinan_urun") > 0)
                .withColumn("R", ceyrek_skoru("recency", kucuk_iyi=True))
                .withColumn("F", ceyrek_skoru("frequency"))
                .withColumn("M", ceyrek_skoru("monetary")))
    almayanlar = (kullanici.filter(F.col("alinan_urun") == 0)
                  .withColumn("R", F.lit(None).cast("int")).withColumn("F", F.lit(None).cast("int"))
                  .withColumn("M", F.lit(None).cast("int")))

    segment = (F.when(F.col("alinan_urun") == 0,
                      F.when(F.col("sepete_ekleme") > 0, "Hiç almamış – sepete eklemiş")
                      .otherwise("Hiç almamış – sadece gezmiş"))
               .when((F.col("R") >= 3) & (F.col("F") >= 3) & (F.col("M") >= 3), "Şampiyon")
               .when(F.col("F") >= 3, "Sadık")
               .when((F.col("R") <= 2) & (F.col("M") >= 3), "Risk altında")
               .when(F.col("R") >= 3, "Yeni / umut veren")
               .otherwise("Uykuda"))
    return alicilar.unionByName(almayanlar).withColumn("segment", segment)


def segment_ozeti(rfm):
    tum = Window.partitionBy()
    return (rfm.groupBy("segment").agg(
                F.count("*").alias("kullanici"),
                F.round(F.sum("monetary"), 2).alias("harcama"),
                F.round(F.avg("recency"), 1).alias("ort_recency_gun"),
                F.round(F.avg("frequency"), 2).alias("ort_frequency"),
                F.round(F.avg("monetary"), 2).alias("ort_monetary"),
                F.round(F.avg("goruntuleme"), 1).alias("ort_goruntuleme"))
            .withColumn("kullanici_payi", F.col("kullanici") / F.sum("kullanici").over(tum))
            .withColumn("harcama_payi", F.col("harcama") / F.sum("harcama").over(tum))
            .orderBy(F.desc("harcama"), F.desc("kullanici")))


# --------------------------------------------------------------------------------------
# Ölçüm ve çıktı yardımcıları
# --------------------------------------------------------------------------------------
def olc(adim, boyut, satir, tekrar, fn, sureler):
    """fn bir Spark action içermeli (toPandas/count). Süre: tekrarların medyanı.
    ÖNEMLİ: fn her çağrıda DataFrame'i SIFIRDAN kurmalı. Aynı DataFrame nesnesi tekrar çalıştırılırsa
    Spark önceki shuffle çıktılarını yeniden kullanır ve süre yapay olarak düşer (ilk ölçümde
    ürün adımı 5m'de 12,9 sn iken tekrarlarda 0,12 sn çıktı)."""
    olcumler, sonuc = [], None
    for _ in range(tekrar):
        t0 = time.perf_counter()
        sonuc = fn()
        olcumler.append(time.perf_counter() - t0)
    sureler.append({"boyut": boyut, "adim": adim, "satir": satir, "tekrar": tekrar,
                    "medyan_sn": round(statistics.median(olcumler), 3),
                    "min_sn": round(min(olcumler), 3), "max_sn": round(max(olcumler), 3)})
    print(f"  [{boyut}] {adim:<12} {statistics.median(olcumler):7.2f} sn")
    return sonuc


def csv_yaz(ad, df_yeni, boyutlar):
    """Aynı dosyada farklı boyutlar birlikte tutulur; sadece bu çalıştırmadaki boyutlar yenilenir."""
    yol = REPORT_DIR / f"{ad}.csv"
    if yol.exists():
        eski = pd.read_csv(yol)
        df_yeni = pd.concat([eski[~eski["boyut"].isin(boyutlar)], df_yeni], ignore_index=True)
    sira = {b: i for i, b in enumerate(SAMPLE_SIZES)}
    df_yeni = df_yeni.sort_values("boyut", key=lambda s: s.map(sira), kind="stable")
    df_yeni.to_csv(yol, index=False, encoding="utf-8")


def plan_metni(df):
    tampon = io.StringIO()
    with contextlib.redirect_stdout(tampon):
        df.explain(mode="formatted")
    return tampon.getvalue()


# --------------------------------------------------------------------------------------
# Grafikler
# --------------------------------------------------------------------------------------
def _eksen_sade(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(CIZGI)
    ax.tick_params(colors=MUREKKEP_2)


def grafik_saat_gun(saat_gun, boyut):
    fig = Figure(figsize=(12, 6.2))
    ust, alt = fig.add_subplot(2, 1, 1), fig.add_subplot(2, 1, 2)
    for ax, kolon, baslik in ((ust, "purchase", "Satın alma sayısı"),
                              (alt, "donusum", "Dönüşüm (purchase / view)")):
        tablo = (saat_gun.pivot(index="gun_no", columns="saat", values=kolon)
                 .reindex(index=range(1, 8), columns=range(24)))
        if kolon == "donusum":
            # Az görüntülemeli hücrelerde oran gürültülü (3 view 2 purchase = %67) -> boş bırakılır
            view = (saat_gun.pivot(index="gun_no", columns="saat", values="view")
                    .reindex(index=range(1, 8), columns=range(24)))
            tablo = tablo.where(view >= MIN_VIEW_HUCRE)
            baslik += f"  ·  {MIN_VIEW_HUCRE}'den az görüntülemeli hücreler boş"
        im = ax.imshow(tablo.values, aspect="auto", cmap="Blues")
        ax.set_yticks(range(7), GUNLER)
        ax.set_xticks(range(24), [str(s) for s in range(24)])
        ax.set_title(baslik, loc="left", fontsize=11, color=MUREKKEP)
        fig.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
    alt.set_xlabel("Saat (UTC)")
    fig.suptitle(f"Haftanın günü × saat ({boyut})", fontweight="bold", x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(REPORT_DIR / "saat_gun_isi_haritasi.png", dpi=150)


def grafik_segment(seg, boyut):
    seg = seg.sort_values("kullanici_payi")
    y = range(len(seg))
    fig = Figure(figsize=(9, 4.6))
    ax = fig.add_subplot()
    ax.barh([i + 0.2 for i in y], seg["kullanici_payi"] * 100, height=0.38, color=MAVI, label="Kullanıcı payı")
    ax.barh([i - 0.2 for i in y], seg["harcama_payi"] * 100, height=0.38, color=TURUNCU, label="Harcama payı")
    for i, (k, h) in enumerate(zip(seg["kullanici_payi"] * 100, seg["harcama_payi"] * 100)):
        ax.text(k, i + 0.2, f" {k:.1f}%", va="center", fontsize=8, color=MUREKKEP_2)
        ax.text(h, i - 0.2, f" {h:.1f}%", va="center", fontsize=8, color=MUREKKEP_2)
    ax.set_yticks(list(y), seg["segment"])
    ax.set_xlabel("%")
    ax.legend(frameon=False, loc="lower right")
    _eksen_sade(ax)
    ax.set_title(f"RFM segmentleri: kullanıcı payı ve harcama payı ({boyut})", loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(REPORT_DIR / "segment_dagilimi.png", dpi=150)


def grafik_kategori(kat, boyut, adet=12):
    kat = kat.head(adet).iloc[::-1].copy()
    kat["etiket"] = [f"{c} (…{str(i)[-5:]})" if isinstance(c, str) else f"kategori …{str(i)[-5:]}"
                     for c, i in zip(kat["category_code"], kat["category_id"])]
    fig = Figure(figsize=(11, 5))
    a1, a2 = fig.add_subplot(1, 2, 1), fig.add_subplot(1, 2, 2, sharey=None)
    a1.barh(kat["etiket"], kat["view"] / 1e3, color=MAVI, height=0.6)
    a1.set_title("Görüntüleme (bin)", loc="left")
    a2.barh(kat["etiket"], kat["donusum"] * 100, color=MAVI, height=0.6)
    a2.set_yticks(range(len(kat)), [""] * len(kat))
    a2.set_title("Dönüşüm: purchase / view (%)", loc="left")
    for i, v in enumerate(kat["donusum"] * 100):
        a2.text(v, i, f" {v:.1f}", va="center", fontsize=8, color=MUREKKEP_2)
    for ax in (a1, a2):
        _eksen_sade(ax)
    fig.suptitle(f"En çok görüntülenen {adet} kategori ({boyut})", fontweight="bold", x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(REPORT_DIR / "kategori_donusum.png", dpi=150)


def grafik_firsat(firsat, boyut, adet=10):
    """Çok görüntülenip az satan ürünler: dönüşüm, popüler ürünlerin medyan dönüşümüyle karşılaştırılır."""
    f = firsat.head(adet).iloc[::-1].copy()
    medyan = f["populer_medyan_donusum"].iloc[0] * 100
    f["etiket"] = [f"{p} · {b if isinstance(b, str) else 'markasız'} · {fiyat:.2f}"
                   for p, b, fiyat in zip(f["product_id"], f["brand"], f["last_price"])]
    fig = Figure(figsize=(11, 5.2))
    ax = fig.add_subplot()
    ax.barh(f["etiket"], f["donusum"] * 100, color=TURUNCU, height=0.6)
    ax.axvline(medyan, color=MUREKKEP_2, linewidth=1.2, linestyle="--",
               label=f"Popüler ürünlerin medyan dönüşümü (%{medyan:.1f})".replace(".", ","))
    sag = max(medyan, (f["donusum"] * 100).max()) * 1.06   # açıklamalar çizginin sağında hizalı
    for i, (d, v, ek) in enumerate(zip(f["donusum"] * 100, f["view"], f["potansiyel_ek_satis"])):
        ax.text(d, i, f" %{d:.1f}".replace(".", ","), va="center", fontsize=8, color=MUREKKEP)
        ax.text(sag, i, f"{v:,.0f} görüntüleme · +{ek:,.0f} satış potansiyeli".replace(",", "."),
                va="center", fontsize=8, color=MUREKKEP_2)
    ax.set_xlim(0, sag * 2.05)
    ax.set_xticks([t for t in ax.get_xticks() if t <= sag])
    ax.legend(frameon=False, loc="lower right", fontsize=8)
    ax.set_xlabel("Dönüşüm: purchase / view (%)")
    _eksen_sade(ax)
    toplam = firsat["potansiyel_ek_satis"].sum()
    ax.set_title(f"Çok görüntülenip az satan ürünler ({boyut}) · ilk {len(firsat)} üründe "
                 f"~{toplam:,.0f} ek satış potansiyeli".replace(",", "."), loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(REPORT_DIR / "firsat_urunleri.png", dpi=150)


def grafik_sureler(sureler):
    if sureler["boyut"].nunique() < 2:
        return
    fig = Figure(figsize=(8, 4.6))
    ax = fig.add_subplot()
    x_map = SAMPLE_SIZES
    for adim, grup in sureler.groupby("adim", sort=False):
        grup = grup.assign(x=grup["boyut"].map(x_map)).sort_values("x")
        ax.plot(grup["x"], grup["medyan_sn"], marker="o", linewidth=2, label=adim)
        ax.annotate(adim, (grup["x"].iloc[-1], grup["medyan_sn"].iloc[-1]), textcoords="offset points",
                    xytext=(6, 0), fontsize=8, color=MUREKKEP_2, va="center")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(list(x_map.values()), [b.upper() for b in x_map])
    ax.set_xlabel("Örneklem boyutu (satır)")
    ax.set_ylabel("Medyan süre (sn, log)")
    ax.legend(frameon=False, fontsize=8, ncol=2)
    _eksen_sade(ax)
    ax.set_title("Analiz adımlarının süresi ve veri boyutu", loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(REPORT_DIR / "sureler.png", dpi=150)


# --------------------------------------------------------------------------------------
# Ana akış
# --------------------------------------------------------------------------------------
def boyut_analizi(spark, boyut, tekrar, sureler):
    kaynak = str(PATHS["samples"] / boyut / "events.parquet")

    def oku():
        df = spark.read.parquet(kaynak).select(*KOLONLAR).cache()
        df.count()  # cache'i doldurmak için action
        return df

    # Okuma ölçümü her tekrarda soğuk başlasın diye önceki cache boşaltılır
    onceki = []

    def oku_temiz():
        while onceki:
            onceki.pop().unpersist(blocking=True)
        df = oku()
        onceki.append(df)
        return df

    events = olc("okuma+cache", boyut, None, tekrar, oku_temiz, sureler)
    satir = events.count()
    sureler[-1]["satir"] = satir
    print(f"         ({satir:,} satır)")

    # Süre ölçümleri: her tekrarda sorgu baştan kurulur (ürün boyut tablosu + join + window dahil)
    top_pdf = olc("urun", boyut, satir, tekrar,
                  lambda: urun_analizi(events, urun_boyut_tablosu(spark, events))[1].toPandas(), sureler)
    kat_pdf = olc("kategori", boyut, satir, tekrar, lambda: kategori_analizi(events).toPandas(), sureler)
    marka_pdf = olc("marka", boyut, satir, tekrar, lambda: marka_analizi(events).toPandas(), sureler)
    saat_pdf = olc("saat_gun", boyut, satir, tekrar, lambda: saat_gun_analizi(events).toPandas(), sureler)
    seg_pdf = olc("rfm", boyut, satir, tekrar,
                  lambda: segment_ozeti(rfm_analizi(events)).toPandas(), sureler)

    # Ölçüm dışı çıktılar: fırsat ürünleri, join planı, Öğrenci 4 için RFM tablosu
    urun = urun_analizi(events, urun_boyut_tablosu(spark, events))[0].cache()
    firsat_pdf = firsat_urunleri(urun).toPandas()
    plan = plan_metni(urun)  # action sonrası: AQE'nin seçtiği son fiziksel plan görünür
    (RFM_DIR / boyut).mkdir(parents=True, exist_ok=True)
    rfm_analizi(events).select("user_id", "recency", "frequency", "monetary", "segment", "R", "F", "M") \
        .write.mode("overwrite").parquet(str(RFM_DIR / boyut / "rfm.parquet"))

    for df in (urun, events):
        df.unpersist()
    sonuc = {"top_urunler": top_pdf, "firsat_urunleri": firsat_pdf, "kategori_donusum": kat_pdf,
             "marka_donusum": marka_pdf, "saat_gun": saat_pdf, "rfm_segmentler": seg_pdf}
    return {ad: pdf.assign(boyut=boyut)[["boyut"] + list(pdf.columns)] for ad, pdf in sonuc.items()}, plan


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=["100k"], choices=list(SAMPLE_SIZES))
    ap.add_argument("--repeat", type=int, default=1, help="süre ölçümü için tekrar sayısı (rapor için 3)")
    args = ap.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("BVA-Proje3-Ogrenci3")
    sureler, sonuclar, plan = [], {}, ""
    try:
        for boyut in args.sizes:
            print(f"== {boyut} ==")
            parca, plan = boyut_analizi(spark, boyut, args.repeat, sureler)
            for ad, pdf in parca.items():
                sonuclar.setdefault(ad, []).append(pdf)
    finally:
        spark.stop()

    for ad, parcalar in sonuclar.items():
        csv_yaz(ad, pd.concat(parcalar, ignore_index=True), args.sizes)
    csv_yaz("sureler", pd.DataFrame(sureler), args.sizes)
    en_buyuk = args.sizes[-1]
    (REPORT_DIR / "join_plani.txt").write_text(f"# Ürün metrikleri ⋈ ürün boyut tablosu ({en_buyuk})\n\n{plan}",
                                               encoding="utf-8")

    # Grafikler, bu çalıştırmadaki en büyük boyutun sonuçlarıyla çizilir
    son = {ad: pd.concat(p)[lambda d: d["boyut"] == en_buyuk] for ad, p in sonuclar.items()}
    grafik_saat_gun(son["saat_gun"], en_buyuk)
    grafik_segment(son["rfm_segmentler"], en_buyuk)
    grafik_kategori(son["kategori_donusum"], en_buyuk)
    grafik_firsat(son["firsat_urunleri"], en_buyuk)
    grafik_sureler(pd.read_csv(REPORT_DIR / "sureler.csv"))
    print(f"Bitti -> {REPORT_DIR}  |  RFM -> {RFM_DIR}")


if __name__ == "__main__":
    main()
