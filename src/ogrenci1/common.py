"""
Ortak ayarlar: yollar, Spark oturumu, açık (explicit) şema ve olay modeli.

Öğrenci 1 - Veri hazırlama ve olay modelleme
Diğer öğrenciler de bu modülü import edip aynı şemayı / yolları kullanabilir:
    from src.ogrenci1.common import get_spark, PATHS, RAW_SCHEMA
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Yollar
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

PATHS = {
    # Kaggle'dan inen CSV'ler (test için BVA_RAW_DIR ile değiştirilebilir)
    "raw": Path(os.environ.get("BVA_RAW_DIR", ROOT / "data" / "raw")),
    "events": ROOT / "data" / "processed" / "events_full",   # temizlenmiş tam veri (Parquet)
    "dim_product": ROOT / "data" / "processed" / "dim_product",
    "samples": ROOT / "data" / "processed" / "samples",      # 100k / 1m / 5m
    "reports": ROOT / "reports" / "ogrenci1",
    "docs": ROOT / "docs",
}

# Deneylerde kullanılacak veri büyüklükleri (ders şartı: en az 3 farklı boyut)
SAMPLE_SIZES = {
    "100k": 100_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
}

# ---------------------------------------------------------------------------
# Ham veri şeması (REES46 / Kaggle "eCommerce Events History in Cosmetics Shop")
# inferSchema kullanmıyoruz: büyük dosyada veriyi iki kez okutuyor ve
# product_id / user_id gibi kolonları yanlış tipe çekebiliyor.
# event_time "2019-10-01 00:00:00 UTC" formatında geldiği için önce string okunuyor.
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

# ---------------------------------------------------------------------------
# Olay modeli
# funnel_step: huni (funnel) içindeki sıra. remove_from_cart sepet aşamasında
# ama "geri adım" olduğu için ayrıca is_negative ile işaretleniyor.
# ---------------------------------------------------------------------------
EVENT_MODEL = {
    #  olay tipi           (funnel_step, aşama adı,   açıklama)
    "view":             (1, "goruntuleme", "Kullanıcı ürün sayfasını açtı"),
    "cart":             (2, "sepet",       "Ürün sepete eklendi"),
    "remove_from_cart": (2, "sepet",       "Ürün sepetten çıkarıldı (geri adım)"),
    "purchase":         (3, "satin_alma",  "Ürün satın alındı"),
}
VALID_EVENTS = list(EVENT_MODEL.keys())


def get_spark(app_name: str = "BVA-Proje3-Ogrenci1",
              driver_mem: str = os.environ.get("BVA_DRIVER_MEM", "12g")) -> SparkSession:
    """Yerel (local[*]) Spark oturumu. Saat dilimi UTC'ye sabitleniyor ki
    saat bazlı analizler bilgisayarın saat dilimine göre kaymasın."""
    builder = SparkSession.builder
    # Windows: Java hadoop.dll'i PATH'ten arıyor. HADOOP_HOME\bin PATH'te değilse
    # Parquet yazarken "UnsatisfiedLinkError: NativeIO$Windows.access0" hatası çıkıyor.
    # JVM bu süreçten başlatıldığı için PATH'e burada eklemek yeterli.
    if os.name == "nt":
        hadoop_bin = os.path.join(os.environ.get("HADOOP_HOME", r"C:\hadoop"), "bin")
        if hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
        os.environ.setdefault("HADOOP_HOME", os.path.dirname(hadoop_bin))
        # Ters eğik çizgiler spark-submit'e aktarılırken kayboluyor (C:hadoopbin),
        # bu yüzden yol düz eğik çizgiyle veriliyor
        builder = builder.config("spark.driver.extraJavaOptions",
                                 "-Djava.library.path=" + hadoop_bin.replace("\\", "/"))

    spark = (
        builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Yedek: dll yine de yüklenmediyse Spark'ın JVM'ine elle yükle
    if os.name == "nt":
        dll = os.path.join(os.environ.get("HADOOP_HOME", r"C:\hadoop"), "bin", "hadoop.dll")
        if os.path.exists(dll):
            try:
                spark._jvm.java.lang.System.load(dll.replace("\\", "/"))
            except Exception as e:  # yüklenemezse en azından sebebini göster
                print("[uyari] hadoop.dll yuklenemedi:", e)
    return spark