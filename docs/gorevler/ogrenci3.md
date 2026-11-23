# Öğrenci 3 – Ürün, Kategori, Saat ve Müşteri Segmenti Analizi

> Sunumdaki görev: *Spark ile ürün, kategori, saat ve müşteri segmenti bazında analiz yapar.*

## Girdi
`data/processed/samples/{100k,1m,5m}/events.parquet` ve tam veri için ürün tablosu `PATHS["dim_product"]`
(dim_product Drive'da yok; gerekirse Öğrenci 1'den iste ya da örneklemden `groupBy("product_id")` ile üret).

## Yapılacaklar (`src/ogrenci3/`)

1. **Ürün:** en çok görüntülenen / sepete eklenen / satın alınan ürünler; ürün bazlı dönüşüm
   (purchase / view). "Çok bakılıp az alınan" ürünler (yüksek view, düşük dönüşüm) → satışı artırma fırsatı.
2. **Kategori:** `category_id` bazında (category_code %98 boş!) olay sayıları ve dönüşüm; marka (`brand`)
   bazında aynısı (boş markaları ayrı grup yap).
3. **Zaman:** saat (0–23, UTC) × haftanın günü dönüşüm/olay ısı haritası. En çok satın alma hangi saatlerde?
4. **Müşteri segmenti (RFM):** kullanıcı başına
   - Recency: son satın almadan bu yana gün (veri son tarihi 2020-02-29 baz)
   - Frequency: satın alma sayısı
   - Monetary: toplam harcama
   Çeyreklik (quantile) skorlarla segmentler: Şampiyon, Sadık, Risk altında, Hiç almamış… Segment başına
   kullanıcı sayısı ve harcama payı.
   Çıktı: `data/processed/rfm/{1m,5m}/rfm.parquet` (`user_id, recency, frequency, monetary, segment`)
   → Öğrenci 4 segmentasyon yaparsa kullanabilir.
5. **Performans:** aynı aggregation'ları 100k / 1m / 5m'de çalıştır, süreleri ölç.

## Çıktılar (`reports/ogrenci3/`)
- `top_urunler.csv`, `kategori_donusum.csv`, `marka_donusum.csv`, `saat_gun.csv`, `rfm_segmentler.csv`,
  `sureler.csv`
- Grafikler: saat × gün ısı haritası, segment dağılımı, en az bir ürün/kategori grafiği

## Kullanılan ileri analitik
aggregation, join (dim_product), window (`ntile` ile RFM skorları, `rank` ile top-N)

## "Neden?" için ipuçları
- Top-N ürün listesi 100k'da ve 5m'de neden farklı/aynı çıkıyor?
- Hangi aggregation 5m'de en pahalı ve neden (kardinalite: product_id ≫ category_id)?
- Bulgular araştırma sorusuna nasıl bağlanıyor: hangi saatte kampanya, hangi ürüne indirim, hangi segmente hatırlatma?

## 2 sınav sorusu → `docs/sinav_sorulari.md`

---

## Agent promptu (kopyala-yapıştır)

```text
Proje: bva-ecommerce (Büyük Veri Analitiği, E-Ticaret Müşteri Davranışı Analitiği). Ben Öğrenci 3'üm.
Önce docs/BASLANGIC.md, docs/olay_modeli.md, docs/veri_sozlugu.md ve docs/gorevler/ogrenci3.md dosyalarını oku.

Görev: docs/gorevler/ogrenci3.md'deki "Yapılacaklar" ve "Çıktılar" bölümlerini PySpark ile uygula.

KAPSAM KİLİDİ – KESİNLİKLE UY:
- Sadece src/ogrenci3/ ve reports/ogrenci3/ altında dosya oluştur/değiştir. Kapsam dışına çıkma.
- src/ogrenci1/ ve diğer öğrencilerin klasörlerine DOKUNMA; README.md'yi sadece ben söyleyince ve sadece
  "Öğrenci 3" bölümünü değiştir.
- Spark için mutlaka `from src.ogrenci1.common import get_spark, PATHS` kullan; veri yolunu elle yazma.
- Kategori analizinde category_code DEĞİL category_id kullan (category_code %98 boş).
- requirements.txt'ye paket eklemen gerekirse önce bana sor.
- Git commit/push yapma; bitince hangi dosyaları değiştirdiğini listele.

Çalışma sırası:
1) Önce sadece 100k ile geliştir ve çalıştır, sonuçları bana göster.
2) Onaylarsam 1m ve 5m için çalıştır, süreleri ölç.
3) Script'ler repo kökünden `python -m src.ogrenci3.<modul>` ile çalışsın, Türkçe kısa yorum satırları olsun.
4) Sonunda: üretilen dosyalar, öne çıkan 5 bulgu ve "neden böyle çıktı" için taslak yorum ver.
```
