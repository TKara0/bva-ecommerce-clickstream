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

**S7.** Oturumların yalnızca %3'ünün satın almayla bittiği veride bir model %84,7 accuracy elde etmiştir. Her oturuma
"satın almaz" diyen ve hiçbir şey öğrenmeyen bir tahmin ise %96,9 accuracy almaktadır. Bu durum için hangisi doğrudur?
A) Hiçbir şey öğrenmeyen tahmin daha iyidir, çünkü accuracy değeri daha yüksektir
B) Model aşırı öğrenmiştir (overfitting), daha az veriyle eğitilmelidir
**C) Sınıflar dengesiz olduğu için accuracy yanıltıcıdır; azınlık sınıfı ölçen AUC-PR, recall ve precision kullanılmalıdır**
D) Accuracy yalnızca Spark MLlib'de yanlış hesaplanır, Pandas ile tekrar hesaplanmalıdır
E) Accuracy'yi yükseltmek için pozitif sınıfın ağırlığı sıfıra indirilmelidir

**S8.** Satın alma olasılığı modeline özellik olarak oturumdaki satın alma sayısı (`n_purchases`) eklenince AUC-ROC
0,90'dan 0,997'ye çıkmıştır. Bu sonuç nasıl yorumlanmalıdır?
A) Model artık neredeyse kusursuzdur ve doğrudan canlıya alınabilir
**B) Veri sızıntısıdır; özellik hedefi doğrudan veriyor ve tahmin anında (kullanıcı henüz satın almamışken) bu bilgi yoktur**
C) GBT yerine Logistic Regression kullanıldığı için AUC yükselmiştir
D) Veri boyutu 100k'dan 5m'e çıktığı için AUC yükselmiştir
E) Sınıf ağırlığı (`weightCol`) kullanıldığında AUC her zaman 1'e yaklaşır

## Öğrenci 5 – Funnel ve performans
*(S9, S10)*
