# Başlangıç Rehberi – Herkes Önce Bunu Okusun

Proje 3: **E-Ticaret Müşteri Davranışı Analitiği**
Araştırma sorusu: *Büyük clickstream verisinden satışa dönüşümü artıracak hangi bilgiler çıkarılabilir?*

## 1. Kurulum (bir kere)

Java 17, Python 3.10–3.12, PySpark ve (Windows'ta) winutils gerekiyor. Gruba atılan kurulum promptunu kendi
agent'ınıza verin ya da adımları elle yapın. Kontrol:

```powershell
python -c "from src.ogrenci1.common import get_spark; s=get_spark('test'); print(s.range(1000).count()); s.stop()"
```

32 GB RAM'iniz varsa `setx BVA_DRIVER_MEM "12g"`, 16 GB ise `"6g"`, 8 GB ise ayarlamayın (varsayılan 4g).

## 2. Veri

Kaggle'dan tekrar indirmeyin. Drive'daki `samples.zip`'i açıp şu yapıya getirin:

```
data/processed/samples/100k/events.parquet   (+ events.csv)
data/processed/samples/1m/events.parquet
data/processed/samples/5m/events.parquet
```

Örneklemler iç içe (100k ⊂ 1m ⊂ 5m) ve kullanıcı bazlı: bir kullanıcı varsa tüm geçmişi var, oturumlar bölünmemiş.
Kolonlar ve olay modeli için: `docs/veri_sozlugu.md`, `docs/olay_modeli.md`.

```python
from src.ogrenci1.common import get_spark, PATHS
spark = get_spark("ogrenci2")
df = spark.read.parquet(str(PATHS["samples"] / "1m" / "events.parquet"))
```

**Geliştirirken 100k ile çalışın**, kod oturunca 1m ve 5m'de çalıştırın.

### Bilinmesi gerekenler
- `category_code` satırların **%98'inde boş** → kategori analizinde `category_id` kullanın.
- `brand` %42 boş. `price` içinde **0** değerler var (hatalı sayılmadı).
- `user_session` %0,02 boş. Zaman UTC.
- Her satırda `is_view / is_cart / is_remove / is_purchase` (0/1) ve `funnel_step` (1/2/3) hazır.

## 3. Kim ne yapıyor, kim kimi bekliyor

| Öğrenci | Görev | Girdi | Çıktısını kim kullanıyor |
|---|---|---|---|
| 1 | Veri hazırlama + olay modeli ✅ | Kaggle | Herkes |
| 2 | Oturumlar + view→cart→purchase dönüşümü | örneklemler | **4 ve 5** |
| 3 | Ürün / kategori / saat / segment analizi | örneklemler | 4 (segment), 5 |
| 4 | Satın alma olasılığı modeli veya segmentasyon | 2'nin oturum tablosu | 5 |
| 5 | Funnel, görselleştirme, performans karşılaştırması | 2, 3, 4'ün çıktıları + örneklemler | Sunum |

Görev ayrıntıları: `docs/gorevler/ogrenciN.md`. Her dosyanın sonunda agent'a verilecek hazır bir prompt var.

**Öğrenci 2 kritik yolda.** Oturum tablosunu ne kadar erken push ederse 4 ve 5 o kadar erken başlar. 4 ve 5 beklerken
100k üzerinde kendi tarafını hazırlayabilir (ayrıntı görev dosyalarında).

## 4. Klasör kuralları

```
src/ogrenciN/            sadece kendi kodunuz
reports/ogrenciN/        sadece kendi CSV/PNG çıktılarınız (git'e girer)
data/processed/...       büyük veri çıktıları (git'e girmez)
```

- **Başkasının klasörüne dokunmayın**, özellikle `src/ogrenci1/`. Bir şey lazımsa grupta söyleyin.
- Script'leri repo kökünden modül olarak çalıştırın: `python -m src.ogrenci2.sessions`
- Veri yolu yazmayın, `PATHS` kullanın. Spark için hep `get_spark()` (Windows ayarları onun içinde).
- Ders şartları için her öğrencinin bölümünde en az: **3 boyutta çalıştırma, bir süre ölçümü, bir grafik ve
  "neden böyle çıktı" yorumu** olsun.

## 5. Git akışı (basit tutuyoruz)

```powershell
git pull                       # başlamadan önce
# ... kendi klasörünüzde çalışın ...
git add src/ogrenci2 reports/ogrenci2
git commit -m "Öğrenci 2: oturum tablosu"
git pull --rebase
git push
```

`git add .` yapmayın, sadece kendi klasörlerinizi ekleyin. README'deki kendi bölümünüzü en sonda doldurun.

## 6. Takvim (sunum: 27 Kasım Cuma)

| Ne zaman | Ne |
|---|---|
| Salı 24 akşam | Herkesin kurulumu bitmiş, 100k ile ilk kod çalışıyor |
| Çarşamba 25 akşam | **Öğrenci 2 oturum tablosunu push etmiş** |
| Perşembe 26 öğlen | Herkes 3 boyutta sonuç + README bölümü + 2 sınav sorusu |
| Perşembe 26 akşam | Öğrenci 5 grafik/dashboard, ortak slaytlar, mimari şema |

## 7. Ortak teslimler (herkes katkı veriyor)

- Mimari şema (tek diyagram)
- Kısa teknik rapor (README bu işi görüyor, her öğrenci kendi bölümünü yazar)
- **10 sınav sorusu:** her öğrenci kendi kısmından 2 tane, 5 seçenekli → `docs/sinav_sorulari.md`
- Sunum: beyaz arka plan siyah yazı, manuel hazırlanmış; altbilgide anlatanın numarası ve adı soyadı,
  APA kaynakça, en az bir avantaj-dezavantaj.
