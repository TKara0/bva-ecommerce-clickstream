# Öğrenci 4 – Satın Alma Olasılığı Modeli (veya Segmentasyon)

> Sunumdaki görev: *Satın alma olasılığı veya müşteri segmentasyonu modeli geliştirir.*

Öneri: **oturum bazlı satın alma olasılığı** (sınıflandırma). Araştırma sorusuna en doğrudan bağlanan seçenek bu:
"Hangi oturum satın almaya dönüşecek, hangisine müdahale edilmeli?"

## Girdi
Öğrenci 2'nin oturum tablosu: `data/processed/sessions/{100k,1m,5m}/sessions.parquet`

**Beklerken:** 100k üzerinde `events` → `groupBy("user_session")` ile basit bir oturum tablosunu kendin üretip
pipeline'ı kur. Öğrenci 2'ninki gelince sadece okuma satırını değiştirirsin.

## Yapılacaklar (`src/ogrenci4/`)

1. **Hedef:** `reached_purchase` (0/1).
2. **Veri sızıntısına dikkat:** `n_purchases`, `revenue` hedefi doğrudan veriyor → **özellik olarak kullanma.**
   Daha gerçekçi senaryo: oturumun **ilk satın alma olayından önceki** davranışıyla tahmin (ör. sadece view/cart/remove
   sayıları, süre, ürün sayısı, saat, gün). Hangi tanımı seçtiğini README'de açıkla.
3. **Model:** Spark MLlib `Pipeline`: `VectorAssembler` → `LogisticRegression` (baseline) ve
   `GBTClassifier` veya `RandomForestClassifier`. Train/test %80/%20, `seed=42`.
4. **Dengesizlik:** satın alan oturum azınlıkta; `weightCol` ile sınıf ağırlığı ver ya da eşiği ayarla.
   Accuracy tek başına yanıltıcı → **AUC-ROC, AUC-PR, precision, recall, F1** raporla.
5. **Özellik önemi:** GBT/RF `featureImportances` → hangi davranış satın almayı en çok belirliyor?
6. **Ölçeklenme:** aynı modeli 100k / 1m / 5m'de eğit; eğitim süresi ve AUC'nin boyutla değişimi.

(Alternatif: segmentasyon seçersen Öğrenci 3'ün RFM tablosu üzerinde `KMeans`, k için silhouette skoru,
küme profilleri. Hangisini seçtiğini grupta söyle.)

## Çıktılar (`reports/ogrenci4/`)
- `model_metrikleri.csv` (boyut, model, AUC-ROC, AUC-PR, precision, recall, F1, eğitim süresi)
- `ozellik_onemi.csv` + grafik
- ROC veya precision-recall eğrisi, boyuta göre eğitim süresi grafiği

## "Neden?" için ipuçları
- Veri büyüyünce AUC neden çok az değişiyor ama eğitim süresi artıyor?
- Accuracy neden yüksek çıkıp yanıltıcı olabiliyor? (sınıf dengesizliği)
- Sızıntılı özellikle AUC ~1.0 çıkması neden "başarı" değil?

## 2 sınav sorusu → `docs/sinav_sorulari.md`

---

## Agent promptu (kopyala-yapıştır)

```text
Proje: bva-ecommerce (Büyük Veri Analitiği, E-Ticaret Müşteri Davranışı Analitiği). Ben Öğrenci 4'üm.
Önce docs/BASLANGIC.md, docs/olay_modeli.md, docs/veri_sozlugu.md ve docs/gorevler/ogrenci4.md dosyalarını oku.

Görev: docs/gorevler/ogrenci4.md'deki oturum bazlı satın alma olasılığı modelini Spark MLlib ile uygula.

KAPSAM KİLİDİ – KESİNLİKLE UY:
- Sadece src/ogrenci4/ ve reports/ogrenci4/ altında dosya oluştur/değiştir. Kapsam dışına çıkma.
- src/ogrenci1/, src/ogrenci2/ ve diğer klasörlere DOKUNMA; README.md'yi sadece ben söyleyince ve sadece
  "Öğrenci 4" bölümünü değiştir.
- Spark için mutlaka `from src.ogrenci1.common import get_spark, PATHS` kullan; veri yolunu elle yazma.
- Hedefi doğrudan veren kolonları (n_purchases, revenue ve satın alma sonrası bilgi) özellik olarak KULLANMA.
- scikit-learn değil Spark MLlib kullan. requirements.txt'ye paket eklemen gerekirse önce bana sor.
- Git commit/push yapma; bitince hangi dosyaları değiştirdiğini listele.

Çalışma sırası:
1) data/processed/sessions/100k/sessions.parquet varsa onu kullan; yoksa 100k events'ten geçici oturum tablosu
   üret (kodu ayrı bir fonksiyonda tut ki sonra kolayca değiştirilsin). Sonuçları bana göster.
2) Onaylarsam 1m ve 5m'de eğit, süreleri ölç.
3) Script'ler repo kökünden `python -m src.ogrenci4.<modul>` ile çalışsın, Türkçe kısa yorum satırları olsun.
4) Sonunda: metrik tablosu, en önemli 5 özellik ve "neden böyle çıktı" için taslak yorum ver.
```
