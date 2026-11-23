#!/usr/bin/env bash
# Öğrenci 1 pipeline'ını baştan sona çalıştırır.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== 1/3 Hazırlama (CSV -> temiz Parquet) =="
python -m src.ogrenci1.prepare_data

echo "== 2/3 Örneklemler (100k / 1m / 5m) =="
python -m src.ogrenci1.make_samples --seed 42

echo "== 3/3 Veri sözlüğü + format karşılaştırması =="
python -m src.ogrenci1.profile_data

echo "Bitti. Raporlar: reports/ogrenci1/  |  Veri sözlüğü: docs/veri_sozlugu.md"
