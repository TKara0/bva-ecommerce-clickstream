"""
SADECE TEST İÇİN: Kaggle verisiyle aynı şema ve formatta sentetik CSV üretir.
Gerçek veri indirilmeden pipeline'ın uçtan uca çalıştığını doğrulamak için.
Raporda / sunumda bu veri KULLANILMAZ.

python tools/generate_test_data.py --rows 6000000 --out data/raw_test
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def make_month(rng, n_rows, year, month, user_pool, sess_offset):
    # Oturum başına olay sayısı ~ geometrik; her oturumda önce view'lar, sonra bazen cart/purchase
    sess_len = rng.geometric(0.18, size=n_rows // 5)
    sess_len = sess_len[np.cumsum(sess_len) <= n_rows]
    n = int(sess_len.sum())
    n_sess = len(sess_len)

    sess_idx = np.repeat(np.arange(n_sess), sess_len)
    users = rng.choice(user_pool, size=n_sess, replace=True)[sess_idx]
    start = pd.Timestamp(year=year, month=month, day=1)
    days = pd.Timestamp(year=year, month=month, day=1).days_in_month
    sess_start = rng.integers(0, days * 86400 - 3600, size=n_sess)
    offset = np.concatenate([np.cumsum(rng.integers(5, 120, size=l)) for l in sess_len])
    ts = start + pd.to_timedelta(sess_start[sess_idx] + offset, unit="s")

    r = rng.random(n)
    event = np.where(r < 0.80, "view", np.where(r < 0.91, "cart", np.where(r < 0.96, "remove_from_cart", "purchase")))

    n_prod = 50_000
    product = rng.zipf(1.3, size=n) % n_prod + 3_700_000
    cat_id = 1_487_580_000_000_000_000 + (product % 500)
    brands = np.array(["runail", "irisk", "masura", "grattol", "estel", "kapous", "ingarden", "jessnail", None])
    brand = brands[product % len(brands)]
    codes = np.array(["appliances.environment.vacuum", "furniture.bathroom.bath",
                      "accessories.bag", "stationery.cartrige"] + [None] * 16, dtype=object)
    code = codes[product % len(codes)]
    price = np.round((product % 997) / 37 + 0.5, 2)
    # birkaç hatalı kayıt ekleyelim ki kalite kontrolleri bir şey yakalasın
    bad = rng.random(n) < 0.0002
    price = np.where(bad, -price, price)

    sess_ids = np.array([f"{x:08x}-test-sess-{x:012d}" for x in range(sess_offset, sess_offset + n_sess)])
    session = sess_ids[sess_idx].astype(object)
    session[rng.random(n) < 0.0003] = None

    df = pd.DataFrame({
        "event_time": ts.strftime("%Y-%m-%d %H:%M:%S") + " UTC",
        "event_type": event,
        "product_id": product,
        "category_id": cat_id,
        "category_code": code,
        "brand": brand,
        "price": price,
        "user_id": users,
        "user_session": session,
    }).sort_values("event_time")
    # birebir tekrar eden satırlar
    df = pd.concat([df, df.sample(frac=0.001, random_state=1)])
    return df, sess_offset + n_sess


def main(rows, out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(7)
    user_pool = rng.integers(10_000_000, 700_000_000, size=max(rows // 12, 1000))
    months = [(2019, 10, "2019-Oct"), (2019, 11, "2019-Nov")]
    offset = 0
    for y, m, name in months:
        df, offset = make_month(rng, rows // len(months), y, m, user_pool, offset)
        df.to_csv(out / f"{name}.csv", index=False)
        print(name, len(df))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=6_000_000)
    ap.add_argument("--out", default="data/raw_test")
    a = ap.parse_args()
    main(a.rows, a.out)
