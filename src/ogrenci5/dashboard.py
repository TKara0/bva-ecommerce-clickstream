"""
Öğrenci 5 - Proje Geneli Tek Dosya HTML Dashboard Üretici.

Öğrenci 1, 2, 3, 4 ve 5'in CSV raporları ile PNG grafiklerini tek bir bağımsız,
çevrimdışı açılabilen HTML sayfasında (reports/ogrenci5/dashboard.html) toplar.
Grafikler Base64 olarak gömüldüğü için dosya tek başına başka bilgisayara
taşındığında da eksiksiz görüntülenir.

Kullanım (repo kökünden):
    python -m src.ogrenci5.dashboard
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.ogrenci1.common import ROOT

REPORTS_ROOT = ROOT / "reports"
OUT_HTML = REPORTS_ROOT / "ogrenci5" / "dashboard.html"


def img_to_data_uri(path: Path) -> str:
    """PNG dosyasını HTML içinde çevrimdışı gömülebilir Base64 data URI'ye çevirir."""
    if not path.exists():
        return ""
    b64 = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def load_dashboard_data() -> dict[str, Any]:
    """Tüm öğrencilerin özet CSV verilerini JSON'a uygun yapıda derler."""
    og1 = REPORTS_ROOT / "ogrenci1"
    og2 = REPORTS_ROOT / "ogrenci2"
    og3 = REPORTS_ROOT / "ogrenci3"
    og4 = REPORTS_ROOT / "ogrenci4"
    og5 = REPORTS_ROOT / "ogrenci5"

    data: dict[str, Any] = {
        "oturum_ozeti": pd.read_csv(og2 / "oturum_ozeti.csv").to_dict(orient="records"),
        "huni_oturum": pd.read_csv(og2 / "huni_oturum.csv").fillna(0).to_dict(orient="records"),
        "huni_kullanici": pd.read_csv(og2 / "huni_kullanici.csv").fillna(0).to_dict(orient="records"),
        "sepet_terki": pd.read_csv(og2 / "sepet_terki.csv").fillna(0).to_dict(orient="records"),
        "rfm": pd.read_csv(og3 / "rfm_segmentler.csv").fillna(0).to_dict(orient="records"),
        "modeller": pd.read_csv(og4 / "model_metrikleri.csv").fillna(0).to_dict(orient="records"),
        "ozellik_onemi": pd.read_csv(og4 / "ozellik_onemi.csv").fillna(0).to_dict(orient="records"),
        "performans": pd.read_csv(og5 / "performans.csv").fillna("").to_dict(orient="records"),
        "format_disk": pd.read_csv(og1 / "format_karsilastirma.csv").to_dict(orient="records"),
    }
    return data


def build_html(data: dict[str, Any]) -> str:
    """Etkileşimli sekmeli ve boyut seçicili tek dosya HTML oluşturur."""
    r = REPORTS_ROOT
    imgs = {
        "og1_olay": img_to_data_uri(r / "ogrenci1" / "olay_dagilimi.png"),
        "og2_sepet": img_to_data_uri(r / "ogrenci2" / "sepet_terki.png"),
        "og3_saat": img_to_data_uri(r / "ogrenci3" / "saat_gun_isi_haritasi.png"),
        "og3_seg": img_to_data_uri(r / "ogrenci3" / "segment_dagilimi.png"),
        "og3_kat": img_to_data_uri(r / "ogrenci3" / "kategori_donusum.png"),
        "og4_roc": img_to_data_uri(r / "ogrenci4" / "roc_pr_egrisi.png"),
        "og4_imp": img_to_data_uri(r / "ogrenci4" / "ozellik_onemi.png"),
        "og4_sure": img_to_data_uri(r / "ogrenci4" / "egitim_suresi.png"),
        "og5_funnel": img_to_data_uri(r / "ogrenci5" / "funnel_karsilastirma.png"),
        "og5_sure": img_to_data_uri(r / "ogrenci5" / "performans_sure.png"),
        "og5_mem": img_to_data_uri(r / "ogrenci5" / "performans_bellek_disk.png"),
        "og5_5m": img_to_data_uri(r / "ogrenci5" / "performans_5m_sorgu_detay.png"),
    }

    data_json = json.dumps(data, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Proje 3 — E-Ticaret Müşteri Davranışı Analitiği Dashboard</title>
<style>
  :root {{
    --bg: #f8fafc;
    --card: #ffffff;
    --border: #e2e8f0;
    --text: #0f172a;
    --muted: #475569;
    --accent: #2563eb;
    --green: #059669;
    --amber: #d97706;
    --red: #dc2626;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
  }}
  header {{
    background: #ffffff;
    border-bottom: 1px solid var(--border);
    padding: 18px 28px;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    gap: 14px;
    position: sticky;
    top: 0;
    z-index: 20;
  }}
  header h1 {{
    margin: 0;
    font-size: 1.28rem;
    font-weight: 700;
  }}
  header p {{
    margin: 2px 0 0 0;
    font-size: 0.86rem;
    color: var(--muted);
  }}
  .controls {{
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .size-btn {{
    border: 1px solid var(--border);
    background: #fff;
    color: var(--text);
    padding: 6px 14px;
    border-radius: 8px;
    font-weight: 600;
    font-size: 0.86rem;
    cursor: pointer;
    transition: all 0.15s;
  }}
  .size-btn.active {{
    background: var(--accent);
    color: #fff;
    border-color: var(--accent);
  }}
  .container {{
    max-width: 1360px;
    margin: 0 auto;
    padding: 20px 24px 48px;
  }}
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(185px, 1fr));
    gap: 14px;
    margin-bottom: 20px;
  }}
  .kpi {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 14px 16px;
  }}
  .kpi .label {{
    font-size: 0.78rem;
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }}
  .kpi .val {{
    font-size: 1.55rem;
    font-weight: 700;
    margin-top: 4px;
  }}
  .kpi .sub {{
    font-size: 0.78rem;
    color: var(--muted);
    margin-top: 2px;
  }}
  .tabs {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
    padding-bottom: 10px;
  }}
  .tab-btn {{
    border: 1px solid transparent;
    background: transparent;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 0.92rem;
    font-weight: 600;
    color: var(--muted);
    cursor: pointer;
  }}
  .tab-btn:hover {{ background: #e2e8f0; color: var(--text); }}
  .tab-btn.active {{
    background: #eff6ff;
    color: var(--accent);
    border-color: #bfdbfe;
  }}
  .panel {{ display: none; }}
  .panel.active {{ display: block; }}
  .grid-2 {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(520px, 1fr));
    gap: 18px;
    margin-bottom: 18px;
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 18px;
  }}
  .card h2 {{
    margin: 0 0 6px 0;
    font-size: 1.05rem;
  }}
  .card .desc {{
    margin: 0 0 12px 0;
    font-size: 0.85rem;
    color: var(--muted);
  }}
  .chart-img {{
    width: 100%;
    height: auto;
    border-radius: 8px;
    border: 1px solid #f1f5f9;
    display: block;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.86rem;
  }}
  th, td {{
    padding: 8px 10px;
    border-bottom: 1px solid var(--border);
    text-align: left;
  }}
  th {{
    background: #f8fafc;
    font-weight: 600;
    color: var(--muted);
  }}
  td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 0.76rem;
    font-weight: 600;
  }}
  .badge-pandas {{ background: #ffedd5; color: #9a3412; }}
  .badge-spark {{ background: #dcfce7; color: #065f46; }}
  .badge-warn {{ background: #fee2e2; color: #991b1b; }}
  .filter-bar {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 12px;
    align-items: center;
  }}
  .filter-bar select {{
    padding: 6px 10px;
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 0.86rem;
    background: #fff;
  }}
  .callout {{
    background: #f0f9ff;
    border-left: 4px solid var(--accent);
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 0.9rem;
    margin-bottom: 14px;
  }}
  footer {{
    text-align: center;
    color: var(--muted);
    font-size: 0.8rem;
    margin-top: 24px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
  }}
</style>
</head>
<body>
<header>
  <div>
    <h1>Proje 3 — E-Ticaret Müşteri Davranışı Analitiği</h1>
    <p>Büyük Veri Analitiği · Selçuk Üniversitesi · Tüm Grup Sonuçları (Öğrenci 1–5)</p>
  </div>
  <div class="controls">
    <span style="font-size:0.84rem;color:var(--muted);font-weight:600;">Örneklem Boyutu:</span>
    <button class="size-btn" data-size="100k">100K</button>
    <button class="size-btn" data-size="1m">1M</button>
    <button class="size-btn active" data-size="5m">5M</button>
  </div>
</header>

<div class="container">
  <!-- KPI Özet Kartları -->
  <div class="kpi-grid" id="kpi-grid"></div>

  <!-- Sekmeler -->
  <div class="tabs">
    <button class="tab-btn active" data-tab="funnel">1. Huni &amp; Veri Özeti (Ö1–Ö2–Ö5)</button>
    <button class="tab-btn" data-tab="segment">2. Saat, Kategori &amp; RFM (Ö3)</button>
    <button class="tab-btn" data-tab="model">3. Satın Alma Modeli (Ö4)</button>
    <button class="tab-btn" data-tab="perf">4. Pandas vs Spark Performans (Ö5)</button>
    <button class="tab-btn" data-tab="actions">5. Araştırma Sorusu &amp; Aksiyonlar</button>
  </div>

  <!-- SEKME 1: FUNNEL & VERI -->
  <section id="tab-funnel" class="panel active">
    <div class="card">
      <h2>Oturum vs Kullanıcı Dönüşüm Hunisi (Öğrenci 2 + Öğrenci 5)</h2>
      <p class="desc">Kullanıcı bazlı örneklem tüm kullanıcı geçmişini koruduğu için dönüşüm oranları 100K, 1M ve 5M boyutlarında istikrarlıdır.</p>
      <img class="chart-img" src="{imgs['og5_funnel']}" alt="Funnel Karşılaştırması" />
    </div>
    <div class="grid-2">
      <div class="card">
        <h2>Seçili Boyut Huni Tablosu (<span class="cur-size">5M</span>)</h2>
        <div id="funnel-table"></div>
      </div>
      <div class="card">
        <h2>Sepet Terki Profili (Öğrenci 2)</h2>
        <img class="chart-img" src="{imgs['og2_sepet']}" alt="Sepet Terki" />
      </div>
    </div>
    <div class="card">
      <h2>Aylık Olay Dağılımı — Tam Veri 19,58M Olay (Öğrenci 1)</h2>
      <img class="chart-img" src="{imgs['og1_olay']}" alt="Aylık Olay Dağılımı" />
    </div>
  </section>

  <!-- SEKME 2: URUN, SAAT & RFM -->
  <section id="tab-segment" class="panel">
    <div class="grid-2">
      <div class="card">
        <h2>Haftanın Günü × Saat Dönüşüm Isı Haritası (Öğrenci 3)</h2>
        <p class="desc">Trafik 19:00 UTC'de zirve yaparken satın alma dönüşümü 09:00–11:00 UTC arasında en yüksektir.</p>
        <img class="chart-img" src="{imgs['og3_saat']}" alt="Saat x Gün Isı Haritası" />
      </div>
      <div class="card">
        <h2>RFM Müşteri Segmentleri (Öğrenci 3)</h2>
        <p class="desc">Şampiyon, Sadık ve Risk Altında segmentleri kullanıcıların %2,6'sı ama harcamanın %68,6'sını oluşturuyor.</p>
        <img class="chart-img" src="{imgs['og3_seg']}" alt="RFM Segment Dağılımı" />
      </div>
    </div>
    <div class="card">
      <h2>RFM Segment Tablosu (<span class="cur-size">5M</span>)</h2>
      <div id="rfm-table"></div>
    </div>
    <div class="card">
      <h2>Kategori Bazlı Dönüşüm (Öğrenci 3)</h2>
      <img class="chart-img" src="{imgs['og3_kat']}" alt="Kategori Dönüşümü" />
    </div>
  </section>

  <!-- SEKME 3: MODEL -->
  <section id="tab-model" class="panel">
    <div class="card">
      <h2>Model Karşılaştırması (<span class="cur-size">5M</span>) — Logistic Regression vs GBT (Öğrenci 4)</h2>
      <p class="desc">Pozitif sınıf oranı %3 olduğundan accuracy yanıltıcıdır; değerlendirmede AUC-PR, Recall ve Precision öne çıkar. Sızıntılı model yalnızca veri sızıntısı (data leakage) etkisini göstermek içindir.</p>
      <div id="model-table"></div>
    </div>
    <div class="grid-2">
      <div class="card">
        <h2>ROC ve Precision-Recall Eğrileri (Öğrenci 4)</h2>
        <img class="chart-img" src="{imgs['og4_roc']}" alt="ROC PR Eğrisi" />
      </div>
      <div class="card">
        <h2>GBT Özellik Önemi (Öğrenci 4)</h2>
        <img class="chart-img" src="{imgs['og4_imp']}" alt="Özellik Önemi" />
      </div>
    </div>
    <div class="card">
      <h2>Boyuta Göre Eğitim Süresi ve AUC (Öğrenci 4)</h2>
      <img class="chart-img" src="{imgs['og4_sure']}" alt="Eğitim Süresi" />
    </div>
  </section>

  <!-- SEKME 4: PERFORMANS -->
  <section id="tab-perf" class="panel">
    <div class="callout" id="crossover-callout"></div>
    <div class="card">
      <h2>Veri Boyutuna Göre Süre Ölçeklenmesi — Log Ölçek (Öğrenci 5)</h2>
      <p class="desc">100K'da Pandas+Parquet sabit JVM/planlama maliyeti olmayan yapısıyla açık ara hızlıdır; veri 5M'e büyüdükçe Spark'ın çok çekirdekli paralelleşmesi ağır sorgularda öne geçer.</p>
      <img class="chart-img" src="{imgs['og5_sure']}" alt="Boyuta Göre Süre" />
    </div>
    <div class="grid-2">
      <div class="card">
        <h2>Bellek (RAM) ve Disk Boyutu Karşılaştırması (Öğrenci 1 + 5)</h2>
        <img class="chart-img" src="{imgs['og5_mem']}" alt="Bellek ve Disk" />
      </div>
      <div class="card">
        <h2>5M Boyutunda Sorgu Bazlı Süreler (Öğrenci 5)</h2>
        <img class="chart-img" src="{imgs['og5_5m']}" alt="5M Sorgu Detay" />
      </div>
    </div>
    <div class="card">
      <h2>Detaylı Performans Ölçüm Tablosu (3 Tekrar Medyanı)</h2>
      <div class="filter-bar">
        <label>Motor:
          <select id="f-motor">
            <option value="ALL">Tümü</option>
            <option value="Pandas">Pandas</option>
            <option value="Spark">Spark</option>
          </select>
        </label>
        <label>Format:
          <select id="f-format">
            <option value="ALL">Tümü</option>
            <option value="CSV">CSV</option>
            <option value="Parquet">Parquet</option>
          </select>
        </label>
        <label>Sorgu:
          <select id="f-sorgu">
            <option value="ALL">Tümü</option>
            <option value="Q0_tam_okuma">Q0 Tam Okuma</option>
            <option value="Q1_olay_tipi">Q1 Olay Tipi</option>
            <option value="Q2_saatlik_olay">Q2 Saatlik Olay</option>
            <option value="Q3_kullanici_satin_alma">Q3 Kullanıcı Satın Alma</option>
            <option value="Q4_oturum_donusum">Q4 Oturum Dönüşümü</option>
          </select>
        </label>
      </div>
      <div id="perf-table"></div>
    </div>
  </section>

  <!-- SEKME 5: AKSIYONLAR -->
  <section id="tab-actions" class="panel">
    <div class="card">
      <h2>Araştırma Sorusuna Ortak Cevap</h2>
      <p class="desc"><strong>Soru:</strong> Büyük clickstream verisinden satışa dönüşümü artıracak hangi bilgiler çıkarılabilir?</p>
      <table>
        <thead>
          <tr><th>Katkı</th><th>Temel Bulgu (5M)</th><th>Önerilen İş Aksiyonu</th></tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Öğrenci 1</strong><br/>Veri &amp; Format</td>
            <td>%5,36 tekrar/hatalı olay temizlendi; Parquet diskte ~4× küçük ve sütun bazlı sorguda çok daha hızlı.</td>
            <td>Üretim clickstream hattında olay tekilleştirme ve Snappy-Parquet depolama standart olmalı.</td>
          </tr>
          <tr>
            <td><strong>Öğrenci 2</strong><br/>Oturum &amp; Huni</td>
            <td>Sepet terki %87,15; oturum dönüşümü %2,46, kullanıcı dönüşümü %6,58 (~2,7×). Satın almaya medyan süre 13 dk.</td>
            <td>Çoklu oturum yolculuğu desteklenmeli; sepeti kalıcı tutma ve 15–30 dk içinde sepet hatırlatma kurgulanmalı.</td>
          </tr>
          <tr>
            <td><strong>Öğrenci 3</strong><br/>Ürün, Saat &amp; RFM</td>
            <td>%17,7 kullanıcı sepete ekleyip hiç almamış; Risk Altında grup harcamanın %20,5'i; dönüşüm 09–11 UTC'de zirvede.</td>
            <td>Kampanya bildirimlerini akşam trafiği yerine sabah dönüşüm zirvesine almak; ilk 20 fırsat ürününde sayfa/fiyat iyileştirmesi (~7.700 ek satış potansiyeli).</td>
          </tr>
          <tr>
            <td><strong>Öğrenci 4</strong><br/>Tahmin Modeli</td>
            <td>GBT modeli oturumların %18'ini hedefleyerek gerçek alıcıların %94'ünü yakalıyor (AUC-ROC 0,944; AUC-PR 0,312).</td>
            <td>Düşük maliyetli anlık müdahaleleri (ücretsiz kargo eşiği, sepet uyarısı) yüksek olasılıklı oturumlara yönlendirmek.</td>
          </tr>
          <tr>
            <td><strong>Öğrenci 5</strong><br/>Performans &amp; Mimari</td>
            <td>100K'da Pandas+Parquet çok daha hızlı; 5M'de Pandas tam tablo RAM'i GB seviyesine fırlarken Spark çok çekirdekli paralelleşme ve CSV/ağır gruplamada öne geçiyor.</td>
            <td>Küçük keşif analizlerinde Pandas+Parquet, milyonlarca satırlık üretim ETL/ML hatlarında PySpark+Parquet hibrit mimarisi tercih edilmeli.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>

  <footer>
    BVA Proje 3 · Otomatik üretildi: <code>python -m src.ogrenci5.dashboard</code> · Veri: REES46 eCommerce Cosmetics Shop
  </footer>
</div>

<script>
const DATA = {data_json};
let currentSize = "5m";

function fmtInt(n) {{
  return Math.round(Number(n || 0)).toLocaleString("tr-TR");
}}
function fmtPct(v, d = 2) {{
  return "%" + (Number(v || 0) * 100).toFixed(d).replace(".", ",");
}}
function fmtNum(v, d = 2) {{
  if (v === "" || v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return Number(v).toFixed(d).replace(".", ",");
}}

function renderKPIs() {{
  const oz = DATA.oturum_ozeti.find(r => r.boyut === currentSize) || {{}};
  const hu = DATA.huni_kullanici.find(r => r.boyut === currentSize && r.asama === "purchase") || {{}};
  const gbt = DATA.modeller.find(r => r.boyut === currentSize && r.model === "GBT") || {{}};
  const q4Rows = DATA.performans.filter(r => r.boyut === currentSize && r.sorgu === "Q4_oturum_donusum" && r.durum === "OK");
  q4Rows.sort((a, b) => Number(a.sure_medyan_sn) - Number(b.sure_medyan_sn));
  const bestQ4 = q4Rows[0];

  const cards = [
    {{ label: "Olay Sayısı", val: fmtInt(oz.olay_sayisi), sub: currentSize.toUpperCase() + " örneklem" }},
    {{ label: "Oturum / Kullanıcı", val: fmtInt(oz.oturum_sayisi), sub: fmtInt(oz.kullanici_sayisi) + " tekil kullanıcı" }},
    {{ label: "Oturum Dönüşümü", val: fmtPct(oz.huni_view_satin_alma_orani), sub: "Kullanıcı: " + fmtPct(hu.view_asamasindan_oran) }},
    {{ label: "Sepet Terki", val: fmtPct(oz.sepet_terki_orani), sub: "Medyan satın alma: " + Math.round((oz.satin_almaya_medyan_sure_sn||0)/60) + " dk" }},
    {{ label: "GBT AUC-ROC / Recall", val: fmtNum(gbt.auc_roc, 3), sub: "Recall " + fmtPct(gbt.recall, 1) + " · AUC-PR " + fmtNum(gbt.auc_pr, 3) }},
    {{ label: "En Hızlı Q4 Motoru", val: bestQ4 ? (bestQ4.motor + " + " + bestQ4.format) : "—", sub: bestQ4 ? (fmtNum(bestQ4.sure_medyan_sn, 3) + " sn (medyan)") : "" }}
  ];
  document.getElementById("kpi-grid").innerHTML = cards.map(c => `
    <div class="kpi">
      <div class="label">${{c.label}}</div>
      <div class="val">${{c.val}}</div>
      <div class="sub">${{c.sub}}</div>
    </div>
  `).join("");
}}

function renderFunnelTable() {{
  const sRows = DATA.huni_oturum.filter(r => r.boyut === currentSize);
  const uRows = DATA.huni_kullanici.filter(r => r.boyut === currentSize);
  const stages = ["view", "cart", "purchase"];
  const names = {{ view: "1. Görüntüleme (view)", cart: "2. Sepete Ekleme (view ∩ cart)", purchase: "3. Satın Alma (view ∩ cart ∩ purchase)" }};
  let html = `<table><thead><tr><th>Aşama</th><th class="num">Oturum</th><th class="num">Oturum %</th><th class="num">Kullanıcı</th><th class="num">Kullanıcı %</th></tr></thead><tbody>`;
  for (const st of stages) {{
    const sr = sRows.find(r => r.asama === st) || {{}};
    const ur = uRows.find(r => r.asama === st) || {{}};
    html += `<tr>
      <td><strong>${{names[st]}}</strong></td>
      <td class="num">${{fmtInt(sr.huni_sayisi)}}</td>
      <td class="num">${{fmtPct(sr.view_asamasindan_oran)}}</td>
      <td class="num">${{fmtInt(ur.huni_sayisi)}}</td>
      <td class="num">${{fmtPct(ur.view_asamasindan_oran)}}</td>
    </tr>`;
  }}
  html += `</tbody></table>`;
  document.getElementById("funnel-table").innerHTML = html;
}}

function renderRfmTable() {{
  const rows = DATA.rfm.filter(r => r.boyut === currentSize);
  let html = `<table><thead><tr><th>Segment</th><th class="num">Kullanıcı</th><th class="num">Kullanıcı Payı</th><th class="num">Toplam Harcama</th><th class="num">Harcama Payı</th><th class="num">Ort. Görüntüleme</th></tr></thead><tbody>`;
  for (const r of rows) {{
    html += `<tr>
      <td><strong>${{r.segment}}</strong></td>
      <td class="num">${{fmtInt(r.kullanici)}}</td>
      <td class="num">${{fmtPct(r.kullanici_payi, 1)}}</td>
      <td class="num">${{fmtNum(r.harcama, 0)}}</td>
      <td class="num">${{fmtPct(r.harcama_payi, 1)}}</td>
      <td class="num">${{fmtNum(r.ort_goruntuleme, 1)}}</td>
    </tr>`;
  }}
  html += `</tbody></table>`;
  document.getElementById("rfm-table").innerHTML = html;
}}

function renderModelTable() {{
  const rows = DATA.modeller.filter(r => r.boyut === currentSize);
  let html = `<table><thead><tr><th>Model</th><th class="num">AUC-ROC</th><th class="num">AUC-PR</th><th class="num">Precision</th><th class="num">Recall</th><th class="num">F1</th><th class="num">Accuracy</th><th class="num">Eğitim (sn)</th></tr></thead><tbody>`;
  for (const r of rows) {{
    const isLeak = String(r.model).includes("sizintili");
    html += `<tr>
      <td><strong>${{r.model}}</strong> ${{isLeak ? '<span class="badge badge-warn">Veri Sızıntısı</span>' : ''}}</td>
      <td class="num">${{fmtNum(r.auc_roc, 4)}}</td>
      <td class="num">${{fmtNum(r.auc_pr, 4)}}</td>
      <td class="num">${{fmtNum(r.precision, 4)}}</td>
      <td class="num">${{fmtNum(r.recall, 4)}}</td>
      <td class="num">${{fmtNum(r.f1, 4)}}</td>
      <td class="num">${{fmtPct(r.accuracy, 1)}}</td>
      <td class="num">${{fmtNum(r.egitim_sn, 2)}}</td>
    </tr>`;
  }}
  html += `</tbody></table>`;
  document.getElementById("model-table").innerHTML = html;
}}

function renderPerfTable() {{
  const fm = document.getElementById("f-motor").value;
  const ff = document.getElementById("f-format").value;
  const fq = document.getElementById("f-sorgu").value;

  const rows = DATA.performans.filter(r =>
    r.boyut === currentSize &&
    (fm === "ALL" || r.motor === fm) &&
    (ff === "ALL" || r.format === ff) &&
    (fq === "ALL" || r.sorgu === fq)
  );

  let html = `<table><thead><tr>
    <th>Sorgu</th><th>Motor</th><th>Format</th>
    <th class="num">Medyan (sn)</th><th class="num">Throughput (satır/sn)</th>
    <th class="num">DF Bellek (MB)</th><th class="num">Süreç RSS (MB)</th>
    <th class="num">Disk (MB)</th><th>Doğrulama</th>
  </tr></thead><tbody>`;
  for (const r of rows) {{
    const badge = r.motor === "Pandas" ? "badge-pandas" : "badge-spark";
    html += `<tr>
      <td><strong>${{r.sorgu}}</strong><br/><span style="font-size:0.76rem;color:var(--muted)">${{r.sorgu_adi}}</span></td>
      <td><span class="badge ${{badge}}">${{r.motor}}</span></td>
      <td>${{r.format}}</td>
      <td class="num"><strong>${{fmtNum(r.sure_medyan_sn, 3)}}</strong></td>
      <td class="num">${{fmtInt(r.throughput_satir_sn)}}</td>
      <td class="num">${{r.df_bellek_mb !== "" ? fmtNum(r.df_bellek_mb, 1) : "— (dağıtık)"}}</td>
      <td class="num">${{fmtNum(r.toplam_rss_mb, 1)}}</td>
      <td class="num">${{fmtNum(r.disk_mb, 1)}}</td>
      <td>${{r.dogrulama}}</td>
    </tr>`;
  }}
  html += `</tbody></table>`;
  document.getElementById("perf-table").innerHTML = html;

  // Kesişme özeti callout
  const pCsv5 = DATA.performans.filter(r => r.boyut === "5m" && r.motor === "Pandas" && r.format === "CSV" && r.durum === "OK");
  const sCsv5 = DATA.performans.filter(r => r.boyut === "5m" && r.motor === "Spark" && r.format === "CSV" && r.durum === "OK");
  const pParq5 = DATA.performans.filter(r => r.boyut === "5m" && r.motor === "Pandas" && r.format === "Parquet" && r.durum === "OK");
  const sParq5 = DATA.performans.filter(r => r.boyut === "5m" && r.motor === "Spark" && r.format === "Parquet" && r.durum === "OK");
  const sumT = arr => arr.reduce((acc, x) => acc + Number(x.sure_medyan_sn || 0), 0);
  const q0Mem5 = DATA.performans.find(r => r.boyut === "5m" && r.motor === "Pandas" && r.format === "CSV" && r.sorgu === "Q0_tam_okuma");

  document.getElementById("crossover-callout").innerHTML =
    `<strong>Spark hangi boyutta mantıklı hale geliyor?</strong> ` +
    `5M satırda 5 sorgunun toplam süresi: <strong>Pandas+CSV ${{fmtNum(sumT(pCsv5), 2)}} sn</strong>, ` +
    `<strong>Spark+CSV ${{fmtNum(sumT(sCsv5), 2)}} sn</strong>, ` +
    `<strong>Pandas+Parquet ${{fmtNum(sumT(pParq5), 2)}} sn</strong>, ` +
    `<strong>Spark+Parquet ${{fmtNum(sumT(sParq5), 2)}} sn</strong>. ` +
    (q0Mem5 ? `Pandas 5M tam tablo için yaklaşık <strong>${{fmtNum(Number(q0Mem5.df_bellek_mb)/1024, 2)}} GB</strong> DataFrame RAM tüketiyor (süreç RSS: ${{fmtNum(Number(q0Mem5.toplam_rss_mb)/1024, 2)}} GB). ` : "") +
    `100K'da Pandas+Parquet açık ara liderken, 1M–5M aralığında CSV okuma, ağır oturum gruplama (Q4) ve bellek sınırı açısından Spark öne geçmeye başlıyor.`;
}}

function refreshAll() {{
  document.querySelectorAll(".cur-size").forEach(el => el.textContent = currentSize.toUpperCase());
  renderKPIs();
  renderFunnelTable();
  renderRfmTable();
  renderModelTable();
  renderPerfTable();
}}

document.querySelectorAll(".size-btn").forEach(btn => {{
  btn.addEventListener("click", () => {{
    document.querySelectorAll(".size-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    currentSize = btn.dataset.size;
    refreshAll();
  }});
}});

document.querySelectorAll(".tab-btn").forEach(btn => {{
  btn.addEventListener("click", () => {{
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
  }});
}});

["f-motor", "f-format", "f-sorgu"].forEach(id => {{
  document.getElementById(id).addEventListener("change", renderPerfTable);
}});

refreshAll();
</script>
</body>
</html>
"""


def main() -> None:
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    data = load_dashboard_data()
    html = build_html(data)
    OUT_HTML.write_text(html, encoding="utf-8")
    size_kb = OUT_HTML.stat().st_size / 1024
    print(f"Dashboard üretildi: {OUT_HTML} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
