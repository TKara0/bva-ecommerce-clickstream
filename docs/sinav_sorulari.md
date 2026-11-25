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
**S3.** Kullanıcı oturumlarını view → cart → purchase huniye dönüştürürken, ardışık olaylar arasında 30 dakikayı
aşan boşlukta yeni oturum bloğu açmak için PySpark'ta hangi window fonksiyonu kullanılmıştır?
A) rank()
B) row_number()
C) ntile()
**D) lag() ve kümülatif toplam**
E) first()

**S4.** Oturum bazlı huninin (`huni_oturum.csv`, 5M), bir önceki aşamaya göre ORANSAL (yüzde) kaybı en büyük olan
adımı hangisidir?
A) view → cart (%81,8 kayıp)
**B) cart → purchase (%86,5 kayıp)**
C) İkisi de eşittir
D) Kayıp gözlemlenmemiştir
E) Veri setinde sepet verisi bulunmamaktadır

## Öğrenci 3 – Ürün / kategori / saat / segment
**S5.** RFM segmentasyonunda müşterileri çeyreklere ayırmak için `ntile()` yerine `percent_rank()` tercih
edilmesinin temel nedeni nedir?
A) ntile() Spark'ta mevcut değildir
B) percent_rank() daha hızlı çalışır
**C) Alıcıların çoğu tek sipariş verdiği için ntile() aynı değerdeki müşterileri keyfi farklı çeyreklere bölerdi; percent_rank() eşit değerlere aynı sırayı verir**
D) ntile() sadece tam sayı sütunlarda çalışır
E) percent_rank() negatif değer üretmez

**S6.** Ürün bazlı top-10 listelerini çıkarmak için küçük ürün boyut tablosu (dim_product) büyük olay tablosuyla
birleştirilirken Spark'ın seçtiği fiziksel join stratejisi nedir?
A) Sort Merge Join
**B) Broadcast Hash Join — küçük tablo her executor'a kopyalanır, büyük tablo shuffle edilmez**
C) Shuffle Hash Join
D) Cartesian Join
E) Nested Loop Join

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

**S9.** 100 bin satırlık örneklemde Pandas + Parquet tüm sorgularda PySpark + Parquet'ten 6×–34× daha hızlı çalışırken,
5 milyon satırlık örneklemde tam tablo okuma ve oturum bazlı gruplama (1,16M grup) sorgularında PySpark öne geçmiştir.
Küçük veride PySpark'ın yavaş kalmasının, büyük veride ise öne geçmesinin temel nedeni nedir?
A) PySpark 100 bin satırda Parquet dosyalarını okuyamadığı için önce CSV'ye çevirir
B) Pandas 5 milyon satırda otomatik olarak tek sütun dışındaki sütunları siler
**C) Küçük veride Spark'ın JVM başlatma, Catalyst planlama ve görev dağıtma sabit maliyeti baskındır; veri büyüdükçe Spark'ın çok çekirdekli paralel işlemesi ve dağıtık bellek yönetimi tek çekirdekli Pandas'ı geçer**
D) PySpark yalnızca GPU üzerinde çalıştığı için küçük veride veri transferi gecikir
E) Pandas 100 bin satırda sonuçları yaklaşık hesapladığı için daha hızlıdır

**S10.** 5 milyon satırlık örneklemde diskte 827 MB yer kaplayan CSV dosyası Pandas ile tam tablo olarak okunduğunda
DataFrame belleği yaklaşık 3,1 GB'a (3,7 kat) çıkmış; aynı veride CSV yerine Parquet kullanıldığında Pandas'ın toplam
sorgu süresi 36,35 sn'den 5,35 sn'ye düşmüştür. Bu iki gözlemi en doğru açıklayan ifade hangisidir?
A) Parquet dosyaları RAM'de hiç bellek kullanmadığı için 0 MB yer kaplar
B) CSV dosyaları sıkıştırılmış olduğu için açılınca sadece sayısal kolonlar büyür
C) Pandas Parquet okurken satırların yalnızca ilk %10'unu belleğe alır
**D) Pandas tüm tabloyu RAM'de tutar ve metin kolonlarını Python `object` nesneleri olarak sakladığı için bellek şişer; Parquet ise sütun bazlı depolama (column pruning), sıkıştırma ve kayıtlı veri tipleri sayesinde hem disk G/Ç'sini hem parse maliyetini azaltır**
E) CSV yalnızca PySpark'ta yavaştır, Pandas'ta CSV ile Parquet arasında hız farkı yoktur
