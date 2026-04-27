import pandas as pd, numpy as np, json, sys
from pathlib import Path

_DIR = Path(__file__).parent

long = pd.read_pickle(_DIR / '_long.pkl')

# Drop rows without a usable total
long = long.dropna(subset=['Total']).copy()

# Top 10 sellers by offer count (includes KUTILOVO; they are the largest seller)
top_sellers = long.groupby('Seller').size().sort_values(ascending=False).head(10).index.tolist()

# EAN -> title map
title_map = (long.dropna(subset=['Title'])
             .drop_duplicates('EAN')
             .set_index('EAN')['Title'].to_dict())

# Compact offer list
offers = []
for r in long.itertuples():
    offers.append({
        'e': str(r.EAN),
        's': r.Seller,
        'p': round(float(r.Price), 2) if not pd.isna(r.Price) else None,
        'd': round(float(r.Delivery), 2) if not pd.isna(r.Delivery) else None,
        't': round(float(r.Total), 2),
    })

# All sellers (for context)
all_sellers = sorted(long['Seller'].unique().tolist())

# Per-seller stats for selector bar (offer count + unique SKUs)
seller_stats = {}
for s, g in long.groupby('Seller'):
    seller_stats[s] = {'offers': int(len(g)), 'skus': int(g['EAN'].nunique())}

# Slim title map (limit length)
titles = {ean: (t[:120] if isinstance(t, str) else '') for ean, t in title_map.items()}

data = {
    'snapshot_date': '2026-04-26',
    'eans_total': int(long['EAN'].nunique()),
    'sellers_total': int(long['Seller'].nunique()),
    'offers_total': int(len(long)),
    'top_sellers': top_sellers,
    'all_sellers': all_sellers,
    'seller_stats': seller_stats,
    'titles': titles,
    'offers': offers,
}

with open(_DIR / '_data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
print('Wrote _data.json:', round(len(json.dumps(data)) / 1024, 1), 'KB')
print('Top sellers:', top_sellers)
print('Offers:', len(offers))
