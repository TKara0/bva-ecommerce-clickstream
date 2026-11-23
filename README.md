# Proje 3 – E-Ticaret Müşteri Davranışı Analitiği

**Büyük Veri Analitiği – 2026-2027 Güz** · Selçuk Üniversitesi Teknoloji Fakültesi Bilgisayar Mühendisliği

**Araştırma sorusu:** Büyük clickstream verisinden satışa dönüşümü artıracak hangi bilgiler çıkarılabilir?

> **Gruba yeni katılıyorsan önce [`docs/BASLANGIC.md`](docs/BASLANGIC.md)'yi oku.**
> Kendi görevin ve agent promptun: [`docs/gorevler/`](docs/gorevler/) ·
> Sınav soruları: [`docs/sinav_sorulari.md`](docs/sinav_sorulari.md)

## Veri seti

| | |
|---|---|
| Ad | eCommerce Events History in Cosmetics Shop |
| Kaynak | Kaggle – https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop |
| Sağlayıcı | REES46 / Open CDP projesi |
| Dönem | Ekim 2019 – Şubat 2020 (5 aylık CSV) |
| Kolonlar | `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session` |
| Olay tipleri | `view`, `cart`, `remove_from_cart`, `purchase` |

Daha büyük ölçek gerekirse aynı şemadaki *eCommerce behavior data from multi category store* seti
(`bash scripts/download_data.sh multi`) kod değişikliği olmadan kullanılabilir.

## Kurulum

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# Java 17+ gerekli (PySpark için):  java -version
```

Kaggle API anahtarı: Kaggle → Settings → API → *Create New Token* → `kaggle.json` dosyasını
`~/.kaggle/` (Windows: `C:\Users\<ad>\.kaggle\`) altına koy.

```bash
bash scripts/download_data.sh          # data/raw/*.csv
bash scripts/run_ogrenci1.sh           # hazırlama + örneklemler + raporlar
```

Kaggle CLI kullanmadan: sayfadan zip'i elle indirip CSV'leri `data/raw/` içine atman yeterli.

## Klasör yapısı

```
data/raw/                      Kaggle CSV'leri (git'e girmez)
data/processed/events_full/    temiz olay tablosu, Parquet, event_month partition'lı
data/processed/dim_product/    ürün boyut tablosu
data/processed/samples/{100k,1m,5m}/events.parquet | events.csv
docs/olay_modeli.md            olay tipleri, huni, varlık ilişkileri, hazırlama kararları
docs/veri_sozlugu.md           otomatik üretilen veri sözlüğü
reports/ogrenci1/              kalite özeti, örneklem özeti, format karşılaştırması, grafikler
src/ogrenci1/                  Öğrenci 1 kodu
tools/generate_test_data.py    sadece test: aynı şemada sentetik veri
```

## Diğer öğrenciler veriyi nasıl kullanır?

```python
from src.ogrenci1.common import get_spark, PATHS

spark = get_spark()
df = spark.read.parquet(str(PATHS["samples"] / "1m" / "events.parquet"))   # veya PATHS["events"] (tam veri)
df.groupBy("event_type").count().show()
```

Pandas ile: `pd.read_parquet("data/processed/samples/1m/events.parquet")` veya `pd.read_csv(".../events.csv")`.

---

## Öğrenci 1 – Veri hazırlama ve olay modelleme

**Görev:** Clickstream/e-commerce veri setini hazırlar ve olay türlerini modeller.

| Adım | Script | Çıktı |
|---|---|---|
| 1. Temin | `scripts/download_data.sh` | `data/raw/*.csv` |
| 2. Hazırlama + olay modeli | `src/ogrenci1/prepare_data.py` | `events_full/`, `dim_product/`, `kalite_ozeti.csv` |
| 3. 100K / 1M / 5M örneklem | `src/ogrenci1/make_samples.py` | `samples/*`, `orneklem_ozeti.csv` |
| 4. Veri sözlüğü + format/boyut analizi | `src/ogrenci1/profile_data.py` | `veri_sozlugu.md`, `format_karsilastirma.csv`, grafikler |

Öne çıkan kararlar (ayrıntısı `docs/olay_modeli.md`'de):

- **Açık şema**, `inferSchema` yok; zaman UTC'ye sabit.
- Her olaya `funnel_step` (1/2/3) ve `is_view/is_cart/is_remove/is_purchase` bayrakları eklendi.
- `category_code` → `category_l1/l2/l3`.
- Örneklemler **kullanıcı bazlı** alındı. Bir kullanıcı seçildiyse tüm geçmişi gelir, böylece oturumlar bölünmez.
  Örneklemler iç içe (100k ⊂ 1m ⊂ 5m) ve `seed=42` ile tekrarlanabilir.
- Her örneklem hem **CSV** hem **Parquet** olarak yazıldı (format karşılaştırması şartı).

### Sonuçlar

Ortam: Windows 11, 32 GB RAM, PySpark 4.3 (local[*], driver 12 GB), Java 17.

**Veri kalitesi** (`reports/ogrenci1/kalite_ozeti.csv`)

| Metrik | Değer |
|---|---|
| Ham satır (5 ay) | 20.692.840 |
| Birebir tekrar eden olay (silindi) | 1.109.092 |
| Negatif fiyat (silindi) | 131 |
| Boş oturum (tutuldu) | 4.598 |
| Geçersiz zaman / olay tipi / boş user-ürün | 0 |
| **Temiz satır** | **19.583.617** (%5,36 elendi) |
| Hazırlama süresi | 134,7 sn |

Elenen satırların neredeyse tamamı tekrar eden olaylar. Aynı kullanıcı, aynı saniyede, aynı ürüne aynı olayı
birden fazla kez üretmiş görünüyor; büyük ihtimalle istemci tarafında çift kayıt. Silinmeseler sepet ve görüntüleme
sayıları şişecek, dönüşüm oranları olduğundan düşük çıkacaktı.

**Olay dağılımı** (`olay_dagilimi_aylik.csv`, `olay_dagilimi.png`)

| Olay | Sayı | Pay |
|---|---|---|
| view | 9.656.788 | %49,3 |
| cart | 5.652.991 | %28,9 |
| remove_from_cart | 2.987.855 | %15,3 |
| purchase | 1.285.983 | %6,6 |

- Olay bazında her 100 görüntülemeye ~58 sepete ekleme ve ~13 satın alma düşüyor. Sepete eklemelerin ~%53'ü
  kadar sepetten çıkarma var. Kozmetikte kullanıcılar sepeti "liste" gibi kullanıyor olabilir; oturum bazlı
  gerçek dönüşüm Öğrenci 2'nin analizinde hesaplanacak.
- En yoğun ay Kasım 2019 (4,39M olay, 322K satın alma), en sakin ay Aralık 2019 (3,35M). Kasım'daki artış
  kampanya dönemiyle (Black Friday) ilişkili olabilir; bu bir hipotez, veri tek başına nedenini göstermiyor.

**Veri sözlüğünden öne çıkanlar** (`docs/veri_sozlugu.md`)

- `category_code` satırların **%98,25'inde boş**. Kategori analizlerinde `category_code` yerine `category_id`
  (500 farklı değer) kullanılmalı.
- `brand` %42,2 boş, 293 marka var.
- `price` en düşük 0,0. Sıfır fiyatlı ürünler hatalı sayılmadı (numune/hediye olabilir); fiyat analizlerinde dikkat.
- 1,57M kullanıcı, 4,83M oturum, 54K ürün. (Farklı değer sayıları `approx_count_distinct` ile yaklaşık
  hesaplandı; ör. `event_hour` 25 görünüyor ama gerçekte 24.)

**Örneklemler** (`orneklem_ozeti.csv`)

| Örneklem | Satır | Kullanıcı | Oturum | Ürün | Satın alma |
|---|---|---|---|---|---|
| 100k | 99.959 | 9.366 | 24.490 | 18.899 | 6.104 |
| 1m | 999.932 | 85.132 | 235.015 | 41.796 | 65.284 |
| 5m | 4.999.968 | 411.547 | 1.165.961 | 49.879 | 324.081 |

Satır sayıları hedefin hemen altında: kullanıcıların tüm geçmişi alındığı için hedefi aşmayan son kullanıcıda
duruluyor. Ürün sayısı satırla doğrusal artmıyor (100k→5m: 50 kat satır, 2,6 kat ürün); popüler ürünler küçük
örneklemde bile görünüyor.

**CSV vs Parquet** (`format_karsilastirma.csv`, `format_boyut.png`, `format_sure.png`; süreler 3 tekrarın medyanı)

| Örneklem | Format | Disk (MB) | count (sn) | avg(price) (sn) | groupBy(event_type) (sn) |
|---|---|---|---|---|---|
| 100k | CSV | 16,6 | 0,28 | 0,36 | 0,36 |
| 100k | Parquet | 3,0 | 0,22 | 0,26 | 0,28 |
| 1m | CSV | 165,4 | 0,27 | 0,41 | 0,40 |
| 1m | Parquet | 40,3 | 0,23 | 0,25 | 0,28 |
| 5m | CSV | 827,2 | 0,46 | 1,03 | 1,30 |
| 5m | Parquet | 206,5 | 0,32 | 0,27 | 0,57 |

Tam veride ham CSV 2.434 MB, temiz Parquet 822 MB (**2,96 kat** küçük), üstelik Parquet'te 9 yerine 22 kolon var.

**Neden böyle çıktı?**

1. **Parquet ~4 kat küçük:** Kolon bazlı saklıyor. Aynı kolondaki benzer değerler (ör. sadece 4 farklı değeri olan
   `event_type`, tekrar eden `user_session`) yan yana durunca sözlük kodlaması + Snappy sıkıştırması çok iyi
   çalışıyor. CSV her değeri metin olarak tekrar tekrar yazıyor.
2. **Tek kolon sorgusunda Parquet'in süresi sabit kalıyor (0,25–0,27 sn), CSV'ninki büyüyor (0,36 → 1,03 sn):**
   `avg(price)` için Parquet sadece `price` kolonunu okuyor (column pruning). CSV'de kolon kavramı olmadığı için
   her satırın tamamı okunup parse edilmek zorunda.
3. **groupBy farkı daha az (1,30'a karşı 0,57 sn):** Burada iki tarafta da gruplama/shuffle maliyeti var; okuma
   avantajı farkın sadece bir kısmını oluşturuyor.
4. **100k ve 1m'de iki format neredeyse aynı:** Bu boyutlarda süreyi veri değil, Spark'ın her iş için harcadığı
   sabit başlatma/planlama maliyeti (~0,2 sn) belirliyor. Fark ancak 5m'de belirginleşiyor. Veri büyüdükçe
   Parquet'in avantajı artacaktır; tam veride (19,6M) fark daha da açılır.
5. **count'ta fark küçük:** Parquet satır sayısını dosya üst verisinden (footer) okuyabiliyor; CSV'de satırları
   saymak için dosyanın taranması gerekiyor ama parse gerekmediği için bu da görece hızlı.

Ayrıntılı Pandas/Spark ve bellek karşılaştırması Öğrenci 5'in bölümünde.

## Öğrenci 2 – Oturumlar ve dönüşüm

PySpark ile kullanıcı oturumları oluşturuldu. Oturum ve kullanıcı bazında görüntüleme → sepet → satın alma
dönüşümü, sepetten çıkarma ve sepet terki hesaplandı. Girdi olarak Öğrenci 1'in REES46 kozmetik mağazası
verisinden hazırladığı kullanıcı bazlı 100k, 1m ve 5m Parquet örneklemleri kullanıldı.
[Kaggle veri kümesi](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)

```mermaid
flowchart LR
    E[Olay örneklemleri] --> W[Eksik oturum kimliklerini zaman penceresiyle tamamla]
    W --> S[Oturum başına Spark gruplaması]
    S --> P[Oturum Parquet tablosu]
    P --> H[Oturum ve kullanıcı hunileri]
    P --> A[Sepet terki ve oturum profili]
    H --> R[CSV raporları]
    A --> R
    A --> G[Sepet terki grafiği]
```

### Çalıştırma

Repo kökünde, projenin Python ortamı etkin durumdayken çalıştırın.

```bash
python -m src.ogrenci2.sessions
python -m src.ogrenci2.conversion
```

İlk komut varsayılan olarak `data/processed/samples/` dizinini okur. Veri başka yerdeyse
`python -m src.ogrenci2.sessions --samples-dir "../samples/samples"` gibi `100k`, `1m` ve `5m`
klasörlerini içeren dizini belirtin. İki modülde de `--sizes 100k` ile yalnızca ilk örneklem seçilebilir.
Dönüşüm komutu CSV ve grafikleri seçilen örneklemler için yeniden yazar.

Oturum tabloları `data/processed/sessions/{100k,1m,5m}/sessions.parquet` altında oluşur. Öğrenci 4 ve 5
bu tabloları doğrudan okuyabilir. Saat ve haftanın günü UTC'dir (`1 = Pazar`, `7 = Cumartesi`).

### Hesaplama kuralları

- Kaynaktaki dolu `user_session` kimlikleri korunur. Oturumlar `user_id` ve `user_session` birlikte kullanılarak
  gruplanır. Eksik kimliği olan kullanıcının olayları zaman sırasına konur. `lag` ve kümülatif toplam ile ardışık
  olaylar arasında **30 dakikayı aşan** boşlukta yeni blok açılır. Blokta tek bir bilinen kimlik varsa eksik
  olaylar o oturuma katılır. Blokta kimlik yoksa veya birden fazla kimlik varsa eksik olaylara blok için üretilen
  bir kimlik verilir. [Spark `lag` belgeleri](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html)
- Huni aşama varlığına dayanır. `huni_sayisi` sırasıyla `view`, `view ∩ cart` ve `view ∩ cart ∩ purchase`
  gruplarını sayar. `bagimsiz_sayi` ise her aşamaya ulaşan bütün oturumları veya kullanıcıları içerir.
  Kullanıcı hunisi aşamaları farklı oturumlardan birleştirebilir. Huni olay sırası ve ürün eşleşmesi gerektirmez.
- Sepet terki, en az bir `cart` olayı bulunan oturumlar içinde hiç `purchase` olayı bulunmayanların payıdır.
  Sepetten çıkarma hem `remove_from_cart / cart` olay oranıyla hem de çıkarma görülen sepetli oturumların
  payıyla ölçülür. Satın almaya kadar geçen süre, satın alan oturumlarda ilk olay ile ilk satın alma arasındadır.
  Medyanlar Spark `percentile_approx` ile hesaplanır.

### Çıktılar

- [Oturum özeti ve süreler](reports/ogrenci2/oturum_ozeti.csv)
- [Oturum hunisi](reports/ogrenci2/huni_oturum.csv) ve [kullanıcı hunisi](reports/ogrenci2/huni_kullanici.csv)
- [Olay bazlı oranlar](reports/ogrenci2/olay_oranlari.csv) ve [oturum başına olay sayısı dağılımı](reports/ogrenci2/oturum_olay_dagilimi.csv)
- [Sepet terki profili](reports/ogrenci2/sepet_terki.csv) ve [grafiği](reports/ogrenci2/sepet_terki.png)

### Sonuçlar

| Örneklem | Olay | Oturum | Oturum hunisi view → purchase | Kullanıcı hunisi view → purchase | Sepet terki | Oturum tablosu süresi |
|---|---:|---:|---:|---:|---:|---:|
| 100k | 99.959 | 24.497 | %2,38 | %6,45 | %87,40 | 8,31 sn |
| 1m | 999.932 | 235.059 | %2,46 | %6,48 | %87,37 | 5,63 sn |
| 5m | 4.999.968 | 1.166.225 | %2,46 | %6,58 | %87,15 | 28,62 sn |

- **Olay ve oturum oranları farklı davranışları ölçüyor.** 5m'de 100 görüntüleme olayına 57,94 sepete ekleme
  olayı düşüyor. Görüntüleme yapılan 100 oturumun ise 18,16'sında sepete ekleme görüldü. Bir oturum çok sayıda
  sepete ekleme üretebildiği için olay oranı, sepete ekleme yapan oturumların payından yüksek.
- **Kullanıcı hunisinin dönüşümü daha yüksek.** Örneğin bir kullanıcı ürünü bir oturumda görüntüleyip sonraki
  oturumlarında sepete ekleyebilir ve satın alabilir. Kullanıcı bazlı örnekleme tüm geçmişi koruduğu için huni
  oranları üç farklı boyutta birbirine yakın kaldı.
- **Sepeti terk eden ve satın alan oturumların profilleri farklı.** 5m'de sepeti terk eden 217.495 oturumda
  oturum başına ortalama 11,6 olay var. Sepete ekleyip satın alan 32.080 oturumda bu değer 29,5. Çıkarma olayı
  sepetli oturumların %39,45'inde görülüyor. İlk satın almaya kadar geçen süre ortalama 6 sa. 55 dk., medyan
  13 dk. 15 sn. Kaynak kimliğiyle gruplanan bazı oturumlarda ilk ve son olay arasında günler var (en uzun
  oturum yaklaşık 151 gün). Bu dağılımda tipik satın alma süresini medyan daha iyi yansıtıyor.
- **Eksik kimlikli olaylar korundu.** 5m'deki 1.061 olayın 459'u mevcut oturumlara atandı. Kalan 602 olay
  için 236 oturum kimliği üretildi.

Süreler Windows 11, Ryzen 7 6800H, 16 GB RAM, Java 17 ve PySpark 4.3 ortamında, `local[*]` ve 4 GB Spark sürücü
belleğiyle tek geçişte ölçüldü. Süre Parquet okumadan oturum Parquet'inin yazımı tamamlanana kadar geçen zamanı
kapsıyor. Küçük bir Spark işiyle ısınma yapıldı. 100k'nın 1m'den yavaş çıkmasında ilk Parquet okuma, sorgu
planlama ve JVM ısınma maliyetleri etkili olabilir. 5m'de daha büyük gruplama ve bölümleme işi ile daha fazla
Parquet yazımı toplam süreyi artırıyor.

## Öğrenci 3 – Spark ile ürün/kategori/saat/segment analizi
*(doldurulacak)*

## Öğrenci 4 – Satın alma olasılığı / segmentasyon modeli
*(doldurulacak)*

## Öğrenci 5 – Funnel, görselleştirme ve performans karşılaştırması
*(doldurulacak)*
