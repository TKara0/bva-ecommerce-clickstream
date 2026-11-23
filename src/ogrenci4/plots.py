"""
Öğrenci 4 - Grafikler (model.py sonunda otomatik çağrılır, tek başına da çalışır)

  ozellik_onemi.png     GBT özellik önemi (en büyük boyut)
  roc_pr_egrisi.png     ROC ve precision-recall eğrileri (boyut x model)
  egitim_suresi.png     boyuta göre eğitim süresi ve AUC

python -m src.ogrenci4.plots
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.ogrenci4.features import REPORTS

ORDER = ["100k", "1m", "5m"]
MODELS = ["LogisticRegression", "GBT"]


def make_plots():
    met = pd.read_csv(REPORTS / "model_metrikleri.csv")
    cur = pd.read_csv(REPORTS / "egri_noktalari.csv")
    imp = pd.read_csv(REPORTS / "ozellik_onemi.csv")
    sizes = [s for s in ORDER if s in set(met["boyut"])]

    # 1) Özellik önemi (en büyük boyut)
    big = sizes[-1]
    d = imp[imp["boyut"] == big].sort_values("onem")
    fig, ax = plt.subplots(figsize=(7, 5.5))
    ax.barh(d["ozellik"], d["onem"], color="#3b6ea5")
    ax.set_xlabel("Önem (GBT featureImportances)")
    ax.set_title(f"Satın almayı belirleyen davranışlar – GBT, {big}")
    fig.tight_layout(); fig.savefig(REPORTS / "ozellik_onemi.png", dpi=130); plt.close(fig)

    # 2) ROC + PR eğrileri
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.6))
    styles = {"LogisticRegression": "--", "GBT": "-"}
    for s in sizes:
        for m in MODELS:
            c = cur[(cur["boyut"] == s) & (cur["model"] == m)]
            if c.empty:
                continue
            auc = met[(met["boyut"] == s) & (met["model"] == m)]
            lbl = f"{m} {s}"
            a1.plot(c["fpr"], c["tpr"], styles[m], label=f"{lbl} (AUC {auc['auc_roc'].iloc[0]:.3f})")
            a2.plot(c["recall"], c["precision"], styles[m], label=f"{lbl} (AP {auc['auc_pr'].iloc[0]:.3f})")
    a1.plot([0, 1], [0, 1], ":", color="gray", label="rastgele")
    pos = met[met["boyut"] == big]["pozitif_oran"].iloc[0]
    a2.axhline(pos, ls=":", color="gray", label=f"rastgele (~{pos:.3f})")
    a1.set(xlabel="Yanlış pozitif oranı", ylabel="Doğru pozitif oranı (recall)", title="ROC eğrisi")
    a2.set(xlabel="Recall", ylabel="Precision", title="Precision-Recall eğrisi")
    a1.legend(fontsize=7); a2.legend(fontsize=7)
    fig.tight_layout(); fig.savefig(REPORTS / "roc_pr_egrisi.png", dpi=130); plt.close(fig)

    # 3) Boyuta göre eğitim süresi + AUC
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4))
    for m in MODELS:
        d = met[met["model"] == m].set_index("boyut").reindex(sizes)
        a1.plot(sizes, d["egitim_sn"], "o-", label=m)
        a2.plot(sizes, d["auc_roc"], "o-", label=f"{m} AUC-ROC")
        a2.plot(sizes, d["auc_pr"], "s--", label=f"{m} AUC-PR")
    a1.set(xlabel="Veri boyutu (olay)", ylabel="Eğitim süresi (sn)", title="Eğitim süresi")
    a2.set(xlabel="Veri boyutu (olay)", ylabel="Skor", title="Model başarısı", ylim=(0, 1))
    a1.legend(); a2.legend(fontsize=7)
    fig.tight_layout(); fig.savefig(REPORTS / "egitim_suresi.png", dpi=130); plt.close(fig)


if __name__ == "__main__":
    make_plots()
