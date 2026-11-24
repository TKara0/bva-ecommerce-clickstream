"""
Öğrenci 5 - Funnel ve Performans Grafikleri.

Ürettiği grafikler (reports/ogrenci5/):
  1. funnel_karsilastirma.png       - Öğrenci 2'nin oturum & kullanıcı hunisi yan yana (5m + boyut özeti)
  2. performans_sure.png            - 100k / 1m / 5m boyutlarına göre süreler (log ölçek, kesişme noktası)
  3. performans_bellek_disk.png     - Bellek (DF & RSS) ve disk boyutu karşılaştırması
  4. performans_5m_sorgu_detay.png  - 5m boyutunda sorgu bazlı Pandas vs Spark süre karşılaştırması

Kullanım (repo kökünden):
    python -m src.ogrenci5.plots
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Başsız (headless) PNG üretimi
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
import pandas as pd

from src.ogrenci1.common import ROOT

REPORTS_DIR = ROOT / "reports" / "ogrenci5"
OG1_DIR = ROOT / "reports" / "ogrenci1"
OG2_DIR = ROOT / "reports" / "ogrenci2"

# Sunum dostu temiz stil (beyaz arka plan, net kontrast)
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": "#CCCCCC",
    "axes.grid": True,
    "grid.color": "#E5E7EB",
    "grid.linestyle": "--",
    "grid.linewidth": 0.7,
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 11.5,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
})

# Motor + format renk ve çizgi stilleri
COMBO_STYLE = {
    ("Pandas", "CSV"):     {"color": "#DC2626", "marker": "o", "ls": "--", "label": "Pandas + CSV"},
    ("Pandas", "Parquet"): {"color": "#EA580C", "marker": "s", "ls": "-",  "label": "Pandas + Parquet"},
    ("Spark",  "CSV"):     {"color": "#2563EB", "marker": "^", "ls": "--", "label": "Spark + CSV"},
    ("Spark",  "Parquet"): {"color": "#059669", "marker": "D", "ls": "-",  "label": "Spark + Parquet"},
}

QUERY_LABELS = {
    "Q0_tam_okuma": "Q0: Tam Okuma\n(22 kolon)",
    "Q1_olay_tipi": "Q1: Olay Tipi\nSayım + Ort.",
    "Q2_saatlik_olay": "Q2: Saatlik\nOlay & Satış",
    "Q3_kullanici_satin_alma": "Q3: Kullanıcı\nSatın Alma",
    "Q4_oturum_donusum": "Q4: Oturum\nDönüşümü",
}


def _fmt_int(n: float | int) -> str:
    """Türkçe binlik ayraçlı tam sayı formatı (1.102.034)."""
    return f"{int(round(n)):,}".replace(",", ".")


# ---------------------------------------------------------------------------
# 1. Funnel görselleştirmesi (Oturum vs Kullanıcı yan yana)
# ---------------------------------------------------------------------------
def plot_funnel() -> Path:
    """Öğrenci 2'nin huni_oturum.csv ve huni_kullanici.csv çıktılarından yan yana funnel grafiği üretir."""
    df_sess = pd.read_csv(OG2_DIR / "huni_oturum.csv")
    df_user = pd.read_csv(OG2_DIR / "huni_kullanici.csv")

    fig, axes = plt.subplots(1, 2, figsize=(13.5, 6.2))
    fig.suptitle(
        "E-Ticaret Dönüşüm Hunisi (5M Örneklem) — Oturum vs Kullanıcı Bazlı",
        fontsize=13.5,
        fontweight="bold",
        y=0.97,
    )

    stage_labels = {
        "view": "1. Görüntüleme\n(view)",
        "cart": "2. Sepete Ekleme\n(view ∩ cart)",
        "purchase": "3. Satın Alma\n(view ∩ cart ∩ purchase)",
    }
    stage_colors = ["#2563EB", "#F59E0B", "#059669"]

    configs = [
        (axes[0], df_sess[df_sess["boyut"] == "5m"], "Oturum Bazlı Huni (5M)", "oturum"),
        (axes[1], df_user[df_user["boyut"] == "5m"], "Kullanıcı Bazlı Huni (5M)", "kullanıcı"),
    ]

    for ax, sub, title, unit in configs:
        sub = sub.set_index("asama").loc[["view", "cart", "purchase"]]
        counts = sub["huni_sayisi"].values.astype(float)
        ratios_view = sub["view_asamasindan_oran"].values.astype(float)
        ratios_prev = sub["onceki_asamadan_oran"].values.astype(float)

        max_val = counts[0]
        # Görselde küçük son barın (satın alma %2,5) okunabilmesi için minimum genişlik tabanı
        widths = [max(v / max_val, 0.14) for v in counts]
        y_positions = [2.2, 1.15, 0.1]
        bar_h = 0.62

        ax.set_xlim(-0.56, 0.76)
        ax.set_ylim(-0.15, 3.05)
        ax.axis("off")
        ax.set_title(title, fontsize=12, fontweight="bold", pad=8)

        # Trapez bağlantılar (huni akışı)
        for i in range(2):
            w_top = widths[i] / 2.0
            w_bot = widths[i + 1] / 2.0
            y_top = y_positions[i]
            y_bot = y_positions[i + 1] + bar_h
            poly = patches.Polygon(
                [[-w_top, y_top], [w_top, y_top], [w_bot, y_bot], [-w_bot, y_bot]],
                closed=True,
                facecolor="#E5E7EB",
                edgecolor="none",
                alpha=0.75,
            )
            ax.add_patch(poly)

            # Adım arası geçiş & kayıp etiketi
            step_pct = ratios_prev[i + 1] * 100.0
            drop_pct = 100.0 - step_pct
            y_mid = (y_top + y_bot) / 2.0
            ax.text(
                0.0,
                y_mid,
                f"▼ Geçiş: %{step_pct:.1f}   (Kayıp: %{drop_pct:.1f})",
                ha="center",
                va="center",
                fontsize=8.8,
                fontweight="bold",
                color="#374151",
                bbox=dict(boxstyle="round,pad=0.22", facecolor="white", edgecolor="#D1D5DB", alpha=0.95),
            )

        # Huni çubukları
        for i, stage in enumerate(["view", "cart", "purchase"]):
            w = widths[i]
            y = y_positions[i]
            rect = patches.FancyBboxPatch(
                (-w / 2.0, y),
                w,
                bar_h,
                boxstyle="round,pad=0.02,rounding_size=0.04",
                facecolor=stage_colors[i],
                edgecolor="white",
                linewidth=1.5,
            )
            ax.add_patch(rect)

            pct_view = ratios_view[i] * 100.0
            stage_short = stage_labels[stage].replace("\n", " ")
            label_main = f"{_fmt_int(counts[i])} {unit}  (%{pct_view:.2f})"
            if w >= 0.42:
                ax.text(
                    0.0,
                    y + bar_h / 2.0,
                    f"{stage_short}\n{label_main}",
                    ha="center",
                    va="center",
                    color="white",
                    fontsize=9.5,
                    fontweight="bold",
                )
            else:
                # Dar barlarda yüzdeyi bar içinde, aşama adı ve sayıyı barın sağında göster (çakışmasız)
                ax.text(
                    0.0,
                    y + bar_h / 2.0,
                    f"%{pct_view:.2f}",
                    ha="center",
                    va="center",
                    color="white",
                    fontsize=9.5,
                    fontweight="bold",
                )
                ax.text(
                    w / 2.0 + 0.045,
                    y + bar_h / 2.0,
                    f"{stage_labels[stage]}\n{_fmt_int(counts[i])} {unit}",
                    ha="left",
                    va="center",
                    color="#111827",
                    fontsize=8.6,
                    fontweight="bold",
                )

    # Alt not: 3 boyutta tutarlılık
    s100 = df_sess[(df_sess["boyut"] == "100k") & (df_sess["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100
    s1m = df_sess[(df_sess["boyut"] == "1m") & (df_sess["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100
    s5m = df_sess[(df_sess["boyut"] == "5m") & (df_sess["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100
    u100 = df_user[(df_user["boyut"] == "100k") & (df_user["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100
    u1m = df_user[(df_user["boyut"] == "1m") & (df_user["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100
    u5m = df_user[(df_user["boyut"] == "5m") & (df_user["asama"] == "purchase")]["view_asamasindan_oran"].iloc[0] * 100

    fig.text(
        0.5,
        0.02,
        (
            f"Boyut tutarlılığı (view→purchase) — Oturum: 100k %{s100:.2f} · 1m %{s1m:.2f} · 5m %{s5m:.2f}   |   "
            f"Kullanıcı: 100k %{u100:.2f} · 1m %{u1m:.2f} · 5m %{u5m:.2f}\n"
            "Not: Kullanıcı hunisi farklı oturumlardaki aşamaları birleştirdiği için dönüşüm oturum hunisinin ~2,7 katıdır."
        ),
        ha="center",
        va="bottom",
        fontsize=8.8,
        color="#4B5563",
    )

    plt.tight_layout(rect=[0, 0.08, 1, 0.93])
    out = REPORTS_DIR / "funnel_karsilastirma.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# 2. Boyuta göre süre grafiği (Log ölçek — kesişme noktası)
# ---------------------------------------------------------------------------
def plot_duration_scaling(df: pd.DataFrame) -> Path:
    """100k / 1m / 5m boyutlarında motor×format sürelerini (log ölçek) 4 panelde çizer."""
    sizes = ["100k", "1m", "5m"]
    x_pos = np.arange(len(sizes))
    x_labels = ["100K\n(~100 bin satır)", "1M\n(~1 milyon satır)", "5M\n(~5 milyon satır)"]

    fig, axes = plt.subplots(2, 2, figsize=(13.2, 8.8))
    fig.suptitle(
        "Veri Büyüklüğüne Göre Çalışma Süresi (Log Ölçek) — Pandas vs PySpark",
        fontsize=13.5,
        fontweight="bold",
        y=0.98,
    )

    panels = [
        (axes[0, 0], "Q0_tam_okuma", "A) Tam Tablo Okuma (22 Kolon)"),
        (axes[0, 1], "Q1_olay_tipi", "B) Hafif Sorgu: Olay Tipi Sayım + Ort. Fiyat (Q1)"),
        (axes[1, 0], "Q4_oturum_donusum", "C) Ağır Gruplama: Oturum Bazlı Dönüşüm (Q4, 1,16M Grup)"),
        (axes[1, 1], "__TOTAL__", "D) 5 Sorgunun Toplam Süresi (Q0 + Q1 + Q2 + Q3 + Q4)"),
    ]

    for ax, qid, title in panels:
        end_labels: list[tuple[float, str, str]] = []
        for (motor, fmt), st in COMBO_STYLE.items():
            ys = []
            for s in sizes:
                if qid == "__TOTAL__":
                    sub = df[(df["boyut"] == s) & (df["motor"] == motor) & (df["format"] == fmt) & (df["durum"] == "OK")]
                    ys.append(sub["sure_medyan_sn"].sum() if len(sub) == 5 else np.nan)
                else:
                    sub = df[
                        (df["boyut"] == s)
                        & (df["motor"] == motor)
                        & (df["format"] == fmt)
                        & (df["sorgu"] == qid)
                        & (df["durum"] == "OK")
                    ]
                    ys.append(sub["sure_medyan_sn"].iloc[0] if len(sub) else np.nan)

            ax.plot(
                x_pos,
                ys,
                color=st["color"],
                marker=st["marker"],
                linestyle=st["ls"],
                linewidth=2.2,
                markersize=7,
                label=st["label"],
            )
            if not np.isnan(ys[-1]):
                end_labels.append((ys[-1], f"{ys[-1]:.2f}s", st["color"]))

        ax.set_yscale("log")
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels)
        ax.set_xlim(-0.2, 2.50)
        ax.set_ylabel("Medyan Süre (sn, log ölçek)")
        ax.set_title(title)

        # 5M uç etiketlerini log ekseninde çakışmayacak şekilde dikey ayır
        end_labels.sort(key=lambda t: t[0])
        ymin, ymax = ax.get_ylim()
        min_log_gap = (np.log10(ymax) - np.log10(ymin)) * 0.085
        placed_logs: list[float] = []
        for val, txt, col in end_labels:
            lv = np.log10(val)
            if placed_logs and lv - placed_logs[-1] < min_log_gap:
                lv = placed_logs[-1] + min_log_gap
            placed_logs.append(lv)
            ax.annotate(
                txt,
                xy=(x_pos[-1], val),
                xytext=(x_pos[-1] + 0.07, 10 ** lv),
                va="center",
                fontsize=8.3,
                fontweight="bold",
                color=col,
                arrowprops=dict(arrowstyle="-", color=col, lw=0.6, alpha=0.55) if abs(lv - np.log10(val)) > 0.02 else None,
            )

    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.935),
        ncol=4,
        frameon=True,
        facecolor="white",
        edgecolor="#D1D5DB",
        fontsize=9.5,
    )

    spark_start = df[df["motor"] == "Spark"]["spark_baslatma_sn"].dropna().median()
    fig.text(
        0.5,
        0.015,
        f"Her nokta 3 tekrarın medyanıdır. Spark JVM soğuk başlatma süresi (~{spark_start:.1f} sn) sorgu sürelerinden ayrı tutulmuştur.",
        ha="center",
        fontsize=8.8,
        color="#4B5563",
    )

    plt.tight_layout(rect=[0, 0.04, 1, 0.90])
    out = REPORTS_DIR / "performans_sure.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# 3. Bellek (RAM) ve Disk Boyutu Karşılaştırması
# ---------------------------------------------------------------------------
def plot_memory_and_disk(df: pd.DataFrame) -> Path:
    """Tam tablo okuma (Q0) RAM kullanımı ile CSV/Parquet disk boyutlarını yan yana gösterir."""
    sizes = ["100k", "1m", "5m"]
    x = np.arange(len(sizes))
    width = 0.20

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.2, 5.4))
    fig.suptitle(
        "Bellek (RAM) ve Disk Boyutu Karşılaştırması — 100K / 1M / 5M",
        fontsize=13,
        fontweight="bold",
        y=0.98,
    )

    # Sol panel: Q0_tam_okuma bellek kullanımı
    q0 = df[(df["sorgu"] == "Q0_tam_okuma") & (df["durum"] == "OK")]

    def _vals(motor: str, fmt: str, col: str) -> list[float]:
        out = []
        for s in sizes:
            sub = q0[(q0["boyut"] == s) & (q0["motor"] == motor) & (q0["format"] == fmt)]
            out.append(float(sub[col].iloc[0]) if len(sub) else 0.0)
        return out

    p_csv_df = _vals("Pandas", "CSV", "df_bellek_mb")
    p_parq_df = _vals("Pandas", "Parquet", "df_bellek_mb")
    p_csv_rss = _vals("Pandas", "CSV", "toplam_rss_mb")
    s_parq_rss = _vals("Spark", "Parquet", "toplam_rss_mb")

    b1 = ax1.bar(x - 1.5 * width, p_csv_df, width, label="Pandas CSV (DataFrame RAM)", color="#DC2626")
    b2 = ax1.bar(x - 0.5 * width, p_parq_df, width, label="Pandas Parquet (DataFrame RAM)", color="#EA580C")
    b3 = ax1.bar(x + 0.5 * width, p_csv_rss, width, label="Pandas CSV (Süreç RSS)", color="#FCA5A5", edgecolor="#DC2626")
    b4 = ax1.bar(x + 1.5 * width, s_parq_rss, width, label="Spark Parquet (JVM+Py RSS)", color="#059669")

    for bars in (b1, b2, b3, b4):
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                lbl = f"{h / 1024:.1f} GB" if h >= 1024 else f"{h:.0f}"
                ax1.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    h + max(p_csv_rss + s_parq_rss) * 0.015,
                    lbl,
                    ha="center",
                    va="bottom",
                    fontsize=7.6,
                    fontweight="bold",
                )

    ax1.set_xticks(x)
    ax1.set_xticklabels(["100K", "1M", "5M"])
    ax1.set_ylabel("Bellek (MB)")
    ax1.set_ylim(0, max(p_csv_rss + s_parq_rss) * 1.16)
    ax1.set_title("A) Tam Tablo (22 Kolon) Bellek Tüketimi")
    ax1.legend(loc="upper left", fontsize=8.3, frameon=True)

    # Sağ panel: Disk boyutu (Öğrenci 1 verisi)
    fmt_df = pd.read_csv(OG1_DIR / "format_karsilastirma.csv")
    csv_disk = [float(fmt_df[(fmt_df["orneklem"] == s) & (fmt_df["format"] == "CSV")]["disk_mb"].iloc[0]) for s in sizes]
    parq_disk = [float(fmt_df[(fmt_df["orneklem"] == s) & (fmt_df["format"] == "Parquet")]["disk_mb"].iloc[0]) for s in sizes]

    w2 = 0.34
    d1 = ax2.bar(x - w2 / 2, csv_disk, w2, label="CSV (Disk MB)", color="#64748B")
    d2 = ax2.bar(x + w2 / 2, parq_disk, w2, label="Parquet / Snappy (Disk MB)", color="#0EA5E9")

    for i, (cv, pv) in enumerate(zip(csv_disk, parq_disk)):
        ax2.text(x[i] - w2 / 2, cv + 16, f"{cv:.1f} MB", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
        ratio = cv / pv
        ax2.text(
            x[i] + w2 / 2,
            pv + 16,
            f"{pv:.1f} MB\n({ratio:.1f}× küçük)",
            ha="center",
            va="bottom",
            fontsize=8.3,
            fontweight="bold",
            color="#0369A1",
        )

    ax2.set_xticks(x)
    ax2.set_xticklabels(["100K", "1M", "5M"])
    ax2.set_ylabel("Disk Boyutu (MB)")
    ax2.set_ylim(0, max(csv_disk) * 1.18)
    ax2.set_title("B) Dosya Formatı Disk Boyutu (Öğrenci 1 Ölçümü)")
    ax2.legend(loc="upper left", fontsize=8.8, frameon=True)

    plt.tight_layout(rect=[0, 0.02, 1, 0.93])
    out = REPORTS_DIR / "performans_bellek_disk.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


# ---------------------------------------------------------------------------
# 4. 5M boyutunda sorgu bazlı karşılaştırma (Grouped Bar Chart)
# ---------------------------------------------------------------------------
def plot_5m_query_breakdown(df: pd.DataFrame) -> Path:
    """5m örnekleminde 5 sorgunun her biri için 4 kombinasyonun süresini yan yana çizer."""
    sub5 = df[(df["boyut"] == "5m") & (df["durum"] == "OK")]
    queries = list(QUERY_LABELS.keys())
    x = np.arange(len(queries))
    width = 0.19

    fig, ax = plt.subplots(figsize=(13.0, 5.6))
    combos = [("Pandas", "CSV"), ("Pandas", "Parquet"), ("Spark", "CSV"), ("Spark", "Parquet")]

    max_y = 0.0
    for idx, (motor, fmt) in enumerate(combos):
        st = COMBO_STYLE[(motor, fmt)]
        vals = []
        for q in queries:
            r = sub5[(sub5["motor"] == motor) & (sub5["format"] == fmt) & (sub5["sorgu"] == q)]
            vals.append(float(r["sure_medyan_sn"].iloc[0]) if len(r) else 0.0)
        max_y = max(max_y, max(vals) if vals else 0.0)
        offset = (idx - 1.5) * width
        bars = ax.bar(x + offset, vals, width, label=st["label"], color=st["color"])
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    h + max_y * 0.015,
                    f"{h:.2f}s",
                    ha="center",
                    va="bottom",
                    fontsize=7.8,
                    fontweight="bold",
                    rotation=0,
                )

    ax.set_xticks(x)
    ax.set_xticklabels([QUERY_LABELS[q] for q in queries])
    ax.set_ylabel("Medyan Süre (sn)")
    ax.set_ylim(0, max_y * 1.18)
    ax.set_title("5 Milyon Satırda Sorgu Bazlı Süre Karşılaştırması (Pandas vs PySpark)")
    ax.legend(loc="upper right", ncol=2, frameon=True, fontsize=9.2)

    plt.tight_layout()
    out = REPORTS_DIR / "performans_5m_sorgu_detay.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    return out


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    perf_csv = REPORTS_DIR / "performans.csv"
    if not perf_csv.exists():
        raise FileNotFoundError(f"Önce benchmark çalıştırılmalı: {perf_csv}")

    df_perf = pd.read_csv(perf_csv)

    p1 = plot_funnel()
    print(f"Üretildi: {p1}")

    p2 = plot_duration_scaling(df_perf)
    print(f"Üretildi: {p2}")

    p3 = plot_memory_and_disk(df_perf)
    print(f"Üretildi: {p3}")

    if "5m" in set(df_perf["boyut"].astype(str)):
        p4 = plot_5m_query_breakdown(df_perf)
        print(f"Üretildi: {p4}")


if __name__ == "__main__":
    main()
