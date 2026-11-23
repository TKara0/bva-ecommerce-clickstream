# Sınav Soruları (10 soru, 5 seçenekli)

Her öğrenci kendi kısmından 2 soru. Doğru cevap kalın.

## Öğrenci 1 – Veri hazırlama ve olay modeli

**S1.** 5 milyon kayıtlık örneklemde yalnızca `price` sütununun ortalaması alınırken Parquet'in süresi CSV'ye göre
yaklaşık 4 kat kısa ölçülmüştür. Bunun temel nedeni nedir?
A) Parquet verileri RAM'de saklar
B) Parquet satır bazlı olduğu için satırları daha hızlı okur
**C) Parquet sütun bazlı olduğu için sorgu sadece gereken sütunu okur (column pruning)**
D) CSV dosyaları Spark tarafından okunamaz, önce dönüştürülür
E) Parquet dosyaları her zaman tek bir dosyadan oluşur

**S2.** Projede 100K/1M/5M örneklemler rastgele satır seçimi (`df.sample()`) yerine kullanıcı bazlı alınmıştır.
Bu tercihin temel amacı nedir?
A) Örneklemleri tam olarak hedef satır sayısına getirmek
B) Spark'ın daha az bellek kullanmasını sağlamak
C) Örneklemlere yalnızca satın alma yapan kullanıcıları almak
**D) Kullanıcının oturumlarını bölmeyerek dönüşüm oranlarının yapay olarak düşmesini önlemek**
E) CSV dosyalarını daha küçük hale getirmek

## Öğrenci 2 – Oturumlar ve dönüşüm
*(S3, S4)*

## Öğrenci 3 – Ürün / kategori / saat / segment
*(S5, S6)*

## Öğrenci 4 – Model
*(S7, S8)*

## Öğrenci 5 – Funnel ve performans
*(S9, S10)*
