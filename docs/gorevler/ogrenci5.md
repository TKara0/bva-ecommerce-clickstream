# Öğrenci 5 – Funnel, Görselleştirme ve Performans Karşılaştırması

> Sunumdaki görev: *Funnel analizi, görselleştirme ve farklı veri büyüklüklerinde performans karşılaştırması yapar.*

## Girdi
- Örneklemler: `data/processed/samples/{100k,1m,5m}/events.parquet` ve `events.csv`
- Öğrenci 2: `reports/ogrenci2/huni_*.csv`, oturum tablosu
- Öğrenci 3 ve 4: `reports/ogrenci3/`, `reports/ogrenci4/`

**Beklerken:** Performans karşılaştırması kimseyi beklemiyor, ilk ona başla.

## Yapılacaklar (`src/ogrenci5/`)

1. **Performans karşılaştırması** (asıl teknik katkı)
   - Aynı 3-4 sorguyu (ör. olay tipine göre sayım, saatlik olay sayısı, kullanıcı başına satın alma, oturum
     bazlı dönüşüm) şu kombinasyonlarda çalıştır:
     **Pandas + CSV, Pandas + Parquet, Spark + CSV, Spark + Parquet** × **100k / 1m / 5m**
   - Her biri için: süre (3 tekrarın medyanı), **bellek** (Pandas'ta `psutil` ile süreç RSS'i ya da
     `df.memory_usage(deep=True)`), mümkünse throughput (satır/sn).
   - Öğrenci 1'in ölçtüğü disk boyutlarını (`reports/ogrenci1/format_karsilastirma.csv`) tabloya ekle.
   - Sorulacak asıl soru: **Hangi boyutta Pandas yerine Spark mantıklı hale geliyor?** Küçük veride Spark'ın
     başlatma maliyeti yüzünden yavaş olması beklenir; kesişme noktasını göster.
2. **Funnel görselleştirme:** Öğrenci 2'nin huni sonuçlarından view → cart → purchase funnel grafiği
   (oturum ve kullanıcı bazlı yan yana).
3. **Dashboard:** tüm grubun ana sonuçlarını tek sayfada toplayan bir sayfa (Streamlit, HTML ya da tek bir
   notebook). Funnel, saat × gün ısı haritası (Ö3), model metrikleri (Ö4), performans grafikleri.

## Çıktılar (`reports/ogrenci5/`)
- `performans.csv` (motor, format, boyut, sorgu, süre, bellek)
- Grafikler: boyuta göre süre (log ölçek), bellek karşılaştırması, funnel
- Dashboard dosyası / ekran görüntüleri

## "Neden?" için ipuçları
- 100k'da Pandas neden Spark'tan hızlı? (JVM başlatma, iş planlama, task dağıtma maliyeti)
- 5m'de Pandas'ın belleği neden bu kadar yüksek? (tüm veri RAM'de, string kolonlar object tipinde)
- Parquet her iki motorda da neden hızlandırıyor? (kolon bazlı okuma, sıkıştırma, tip bilgisi dosyada)

## 2 sınav sorusu → `docs/sinav_sorulari.md`

---

## Agent promptu (kopyala-yapıştır)

```text
Proje: bva-ecommerce (Büyük Veri Analitiği, E-Ticaret Müşteri Davranışı Analitiği). Ben Öğrenci 5'im.
Önce docs/BASLANGIC.md, docs/olay_modeli.md, docs/veri_sozlugu.md, docs/gorevler/ogrenci5.md ve
reports/ogrenci1/format_karsilastirma.csv dosyalarını oku.

Görev: docs/gorevler/ogrenci5.md'deki performans karşılaştırmasını (Pandas/Spark × CSV/Parquet × 100k/1m/5m)
uygula; diğer öğrencilerin çıktıları hazır olunca funnel grafiği ve dashboard'u ekle.

KAPSAM KİLİDİ – KESİNLİKLE UY:
- Sadece src/ogrenci5/ ve reports/ogrenci5/ altında dosya oluştur/değiştir. Kapsam dışına çıkma.
- Diğer öğrencilerin klasörlerine ve çıktılarına DOKUNMA, sadece oku. README.md'yi sadece ben söyleyince ve
  sadece "Öğrenci 5" bölümünü değiştir.
- Spark için mutlaka `from src.ogrenci1.common import get_spark, PATHS` kullan; veri yolunu elle yazma.
- Karşılaştırmada iki motorda da aynı sorgu mantığını kullan; sonuçların (sayıların) eşleştiğini doğrula.
- psutil/streamlit gibi yeni paket gerekirse önce bana sor, onaylarsam requirements.txt'ye ekle.
- Git commit/push yapma; bitince hangi dosyaları değiştirdiğini listele.

Çalışma sırası:
1) Önce 100k ile tüm kombinasyonları çalıştır, sonuçları bana göster.
2) Onaylarsam 1m ve 5m'de çalıştır. 5m'de Pandas belleği aşarsa çökertme; atla ve bunu sonuç olarak not et.
3) Script'ler repo kökünden `python -m src.ogrenci5.<modul>` ile çalışsın, Türkçe kısa yorum satırları olsun.
4) Sonunda: performans tablosu, "Spark hangi boyutta mantıklı hale geliyor" cevabı ve taslak yorum ver.
```
