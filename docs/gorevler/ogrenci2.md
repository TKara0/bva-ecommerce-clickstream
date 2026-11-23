# Öğrenci 2 – Oturumlar ve Dönüşüm

> Sunumdaki görev: *Kullanıcı oturumlarını oluşturur; görüntüleme → sepet → satın alma dönüşümünü hesaplar.*

## Girdi
`data/processed/samples/{100k,1m,5m}/events.parquet`

## Yapılacaklar

1. **Oturum tablosu** (`src/ogrenci2/sessions.py`)
   - Oturum anahtarı olarak `user_session` kullan. Boş olan ~%0,02 için: aynı kullanıcının olaylarını zamana göre
     sırala, önceki olaydan **30 dakikadan uzun** ara varsa yeni oturum başlat (`lag` + kümülatif toplam, window).
   - Oturum başına: `user_session, user_id, start_time, end_time, duration_sec, n_events, n_views, n_carts,
     n_removes, n_purchases, n_products, revenue (purchase fiyat toplamı), reached_cart (0/1),
     reached_purchase (0/1), start_hour, start_dow`
   - Çıktı: `data/processed/sessions/{100k,1m,5m}/sessions.parquet`
     → **Öğrenci 4 ve 5 bunu kullanacak, kolon adlarını değiştirme.** Önce 100k'yı push et ki onlar başlasın.

2. **Dönüşüm hesapları** (`src/ogrenci2/conversion.py`)
   - Oturum bazlı huni: view olan oturum → cart olan → purchase olan; adım adım oranlar.
   - Kullanıcı bazlı huni (aynısı kullanıcı seviyesinde).
   - Olay bazlı oranlarla (README: 100 view'e 58 cart) oturum bazlıyı karşılaştır; farkı yorumla.
   - Sepetten çıkarma oranı, satın almaya kadar geçen ortalama/medyan süre, oturum başına olay sayısı dağılımı.
   - Cart yapıp satın almayan oturumlar (sepeti terk) – oranı ve bu oturumların özellikleri.

3. **Performans:** oturum tablosu oluşturma süresini 100k / 1m / 5m için ölç.

## Çıktılar (`reports/ogrenci2/`)
- `huni_oturum.csv`, `huni_kullanici.csv`
- `oturum_ozeti.csv` (boyut, oturum sayısı, ortalama süre, dönüşüm oranı, hesaplama süresi)
- En az bir grafik (örn. oturum süresi dağılımı veya sepeti terk eden/etmeyen karşılaştırması)

## Kullanılan ileri analitik
window (`lag`, kümülatif toplam), aggregation

## "Neden?" sorusu için ipuçları
- Olay bazlı ve oturum bazlı dönüşüm neden farklı çıkıyor?
- Dönüşüm oranı 100k / 1m / 5m'de neden (neredeyse) aynı? (İpucu: kullanıcı bazlı örneklem)
- Window işlemi 5m'de neden diğer işlemlerden daha pahalı? (İpucu: kullanıcıya göre partition + sıralama = shuffle)

## 2 sınav sorusu
Kendi kısmından 2 soru, 5 seçenekli → `docs/sinav_sorulari.md`

---

## Agent promptu (kopyala-yapıştır)

```text
Proje: bva-ecommerce (Büyük Veri Analitiği, E-Ticaret Müşteri Davranışı Analitiği). Ben Öğrenci 2'yim.
Önce docs/BASLANGIC.md, docs/olay_modeli.md, docs/veri_sozlugu.md ve docs/gorevler/ogrenci2.md dosyalarını oku.

Görev: docs/gorevler/ogrenci2.md'deki "Yapılacaklar" ve "Çıktılar" bölümlerini PySpark ile uygula.

KAPSAM KİLİDİ – KESİNLİKLE UY:
- Sadece src/ogrenci2/ ve reports/ogrenci2/ altında dosya oluştur/değiştir. Kapsam dışına çıkma.
- src/ogrenci1/ ve diğer öğrencilerin klasörlerine DOKUNMA; README.md'yi sadece ben söyleyince ve sadece
  "Öğrenci 2" bölümünü değiştir.
- Spark oturumu için mutlaka `from src.ogrenci1.common import get_spark, PATHS` kullan; kendi SparkSession'ını
  kurma, veri yolunu elle yazma.
- Oturum tablosunun kolon adlarını görev dosyasındaki gibi bırak (Öğrenci 4 ve 5 kullanıyor).
- requirements.txt'ye paket eklemen gerekirse önce bana sor.
- Git commit/push yapma; bitince hangi dosyaları değiştirdiğini listele.

Çalışma sırası:
1) Önce sadece 100k ile geliştir ve çalıştır, sonuçları bana göster.
2) Onaylarsam 1m ve 5m için çalıştır, süreleri ölç.
3) Script'ler repo kökünden `python -m src.ogrenci2.<modul>` ile çalışsın, Türkçe kısa yorum satırları olsun.
4) Sonunda: üretilen dosyalar, temel sayılar ve "neden böyle çıktı" için 3-4 maddelik taslak yorum ver.
```
