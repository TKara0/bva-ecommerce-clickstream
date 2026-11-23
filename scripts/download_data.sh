#!/usr/bin/env bash
# Kaggle'dan veri setini data/raw altına indirir.
# Ön koşul: pip install kaggle  +  ~/.kaggle/kaggle.json (Kaggle > Settings > API > Create New Token)
#
# Kullanım:
#   bash scripts/download_data.sh            # Kozmetik mağazası (~20M satır, ana veri seti)
#   bash scripts/download_data.sh multi      # Çok kategorili mağaza (çok büyük, opsiyonel)
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw

case "${1:-cosmetics}" in
  cosmetics)
    kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop -p data/raw --unzip
    ;;
  multi)
    # Uyarı: aylık dosyalar ~5-9 GB. Sadece ihtiyaç olan ayı tutmak mantıklı.
    kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -p data/raw_multi --unzip
    echo "Kullanmak istediğin ayı data/raw altına taşı (ör. 2019-Oct.csv)."
    ;;
  *)
    echo "Bilinmeyen seçenek: $1 (cosmetics | multi)"; exit 1 ;;
esac

ls -lh data/raw
