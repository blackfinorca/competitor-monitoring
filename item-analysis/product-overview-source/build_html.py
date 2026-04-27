import json, sys
sys.stdout.reconfigure(encoding='utf-8')

with open('C:/Coding/price-list/_data.json', 'r', encoding='utf-8') as f:
    data_str = f.read()

html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Allegro.sk Competitor Pricing</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root {
    --bg: #f7f8fb; --card: #ffffff; --ink: #0f172a; --muted: #64748b;
    --line: #e2e8f0; --accent: #2563eb; --good: #16a34a; --warn: #d97706;
    --bad: #dc2626; --neutral: #475569; --highlight: #f59e0b;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: var(--ink); font-size: 14px; line-height: 1.5; }
  header { background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 100%); color: white;
           padding: 28px 40px 24px 40px; }
  header h1 { margin: 0 0 4px 0; font-size: 22px; font-weight: 700; letter-spacing: -0.02em; }
  header p { margin: 0; opacity: 0.8; font-size: 13px; }

  .seller-bar {
    background: white; border-bottom: 1px solid var(--line);
    padding: 14px 40px; position: sticky; top: 0; z-index: 100;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04);
  }
  .seller-row { margin-bottom: 10px; }
  .seller-row:last-child { margin-bottom: 0; }
  .seller-bar-label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                      letter-spacing: 0.05em; font-weight: 600; margin-bottom: 6px; }
  .seller-buttons { display: flex; gap: 6px; flex-wrap: nowrap; overflow-x: auto;
                    padding-bottom: 4px; scrollbar-width: thin; }
  .seller-buttons::-webkit-scrollbar { height: 6px; }
  .seller-buttons::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
  .seller-btn, .seller-chk { flex: 0 0 auto; white-space: nowrap; }
  .seller-btn { padding: 7px 13px; border: 1px solid var(--line); background: white;
                color: var(--ink); border-radius: 999px; cursor: pointer; font-size: 13px;
                font-weight: 500; font-family: inherit; transition: all 0.12s;
                display: inline-flex; align-items: center; gap: 6px; }
  .seller-btn:hover { border-color: var(--accent); color: var(--accent); }
  .seller-btn.active { background: var(--accent); color: white; border-color: var(--accent); }
  .seller-btn .count { font-size: 11px; opacity: 0.7; }
  .seller-chk { padding: 7px 13px; border: 1px solid var(--line); background: white;
                color: var(--ink); border-radius: 999px; cursor: pointer; font-size: 13px;
                font-family: inherit; transition: all 0.12s;
                display: inline-flex; align-items: center; gap: 6px; user-select: none; }
  .seller-chk:hover { border-color: var(--highlight); }
  .seller-chk.on { background: #fef3c7; border-color: var(--highlight);
                   color: #78350f; font-weight: 600; }
  .seller-chk .box { width: 12px; height: 12px; border: 1.5px solid #94a3b8;
                     border-radius: 3px; display: inline-block; }
  .seller-chk.on .box { background: var(--highlight); border-color: var(--highlight); }

  nav { background: white; border-bottom: 1px solid var(--line);
        padding: 10px 40px; display: flex; gap: 24px; overflow-x: auto; white-space: nowrap; }
  nav a { color: var(--muted); text-decoration: none; font-size: 13px; font-weight: 500; padding: 4px 0; }
  nav a:hover { color: var(--accent); }

  main { max-width: 1400px; margin: 0 auto; padding: 24px 40px 64px 40px; }
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
              gap: 12px; margin-bottom: 24px; }
  .kpi { background: var(--card); border-radius: 8px; padding: 16px 18px;
         border: 1px solid var(--line); }
  .kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase;
               letter-spacing: 0.05em; font-weight: 600; margin-bottom: 6px; }
  .kpi-value { font-size: 24px; font-weight: 700; letter-spacing: -0.02em; }
  .kpi-sub { font-size: 11px; color: var(--muted); margin-top: 4px; }

  section { background: var(--card); border-radius: 10px; padding: 24px 28px;
            margin-bottom: 24px; border: 1px solid var(--line); }
  section h2 { margin: 0 0 4px 0; font-size: 18px; font-weight: 700; letter-spacing: -0.01em; }
  section h2 .ref { color: var(--accent); }
  section .subtitle { color: var(--muted); font-size: 13px; margin-bottom: 18px; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; background: #f8fafc; font-weight: 600;
       color: var(--muted); border-bottom: 1px solid var(--line); font-size: 11px;
       text-transform: uppercase; letter-spacing: 0.04em; }
  td { padding: 8px 10px; border-bottom: 1px solid var(--line); }
  tr:hover td { background: #f8fafc; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .ean { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--muted); }
  .title-cell { max-width: 380px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
  .badge-bad { background: #fef2f2; color: var(--bad); }
  .badge-warn { background: #fffbeb; color: var(--warn); }
  .badge-good { background: #f0fdf4; color: var(--good); }
  .badge-neutral { background: #f1f5f9; color: var(--neutral); }
  .seller-name.highlighted { font-weight: 700; color: #b45309; }
  .controls { display: flex; gap: 12px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; }
  .controls input, .controls select { padding: 7px 10px; border: 1px solid var(--line);
                                       border-radius: 6px; font-size: 13px; background: white;
                                       font-family: inherit; }
  .controls input { min-width: 240px; }
  .table-scroll { max-height: 460px; overflow-y: auto; border: 1px solid var(--line); border-radius: 6px; }
  .chart { width: 100%; }
  details { margin-top: 8px; }
  summary { cursor: pointer; font-size: 12px; color: var(--accent); padding: 4px 0; }
  .footer-note { color: var(--muted); font-size: 12px; padding: 16px; text-align: center; }
</style>
</head>
<body>

<header>
  <h1>Allegro.sk Competitor Pricing</h1>
  <p>Snapshot 2026-04-26 &middot; <span id="hd-stats"></span></p>
</header>

<div class="seller-bar">
  <div class="seller-row">
    <div class="seller-bar-label">Reference seller &mdash; click to set</div>
    <div class="seller-buttons" id="seller-buttons"></div>
  </div>
  <div class="seller-row">
    <div class="seller-bar-label">Highlight competitors &mdash; tick to bold their name across charts and tables</div>
    <div class="seller-buttons" id="highlight-buttons"></div>
  </div>
</div>

<nav>
  <a href="#position">Position</a>
  <a href="#scatter">Scatter</a>
  <a href="#h2h">Head-to-head</a>
  <a href="#density">Density</a>
  <a href="#overlap">Overlap</a>
  <a href="#delivery">Delivery</a>
  <a href="#opportunity">Opportunity</a>
  <a href="#explorer">SKU Explorer</a>
</nav>

<main>

<div class="kpi-grid">
  <div class="kpi"><div class="kpi-label"><span class="ref-name">--</span> SKUs</div><div class="kpi-value" id="kpi-skus">--</div>
    <div class="kpi-sub">listed by reference</div></div>
  <div class="kpi"><div class="kpi-label">Win rate</div><div class="kpi-value" id="kpi-win">--</div>
    <div class="kpi-sub">cheapest among comparable</div></div>
  <div class="kpi"><div class="kpi-label">Comparable</div><div class="kpi-value" id="kpi-comp">--</div>
    <div class="kpi-sub">SKUs with &gt;=1 rival</div></div>
  <div class="kpi"><div class="kpi-label">Deep pricier</div><div class="kpi-value" id="kpi-pricey">--</div>
    <div class="kpi-sub">&gt;=10% over cheapest</div></div>
  <div class="kpi"><div class="kpi-label">Deep cheaper</div><div class="kpi-value" id="kpi-cheap">--</div>
    <div class="kpi-sub">&gt;=10% under cheapest</div></div>
  <div class="kpi"><div class="kpi-label">Monopoly</div><div class="kpi-value" id="kpi-mono">--</div>
    <div class="kpi-sub">no rival on Allegro</div></div>
  <div class="kpi"><div class="kpi-label">Median gap</div><div class="kpi-value" id="kpi-median">--</div>
    <div class="kpi-sub">vs cheapest rival</div></div>
</div>

<section id="position">
  <h2>1. Pricing position &mdash; <span class="ref ref-name"></span></h2>
  <div class="subtitle">For every SKU the reference lists, where its total price (price + delivery) sits versus the cheapest competing offer.</div>
  <div id="chart-buckets" class="chart" style="height: 380px;"></div>
</section>

<section id="scatter">
  <h2>2. <span class="ref ref-name"></span> vs cheapest competitor &mdash; SKU scatter</h2>
  <div class="subtitle">Each dot is one SKU. Above the diagonal = reference is losing on price; below = reference is winning. Hover for SKU details.</div>
  <div id="chart-scatter" class="chart" style="height: 540px;"></div>
</section>

<section id="h2h">
  <h2>3. Head-to-head &mdash; <span class="ref ref-name"></span> vs other top sellers</h2>
  <div class="subtitle">For SKUs where the reference and each rival both have offers: who is cheaper, how often.</div>
  <div id="chart-h2h" class="chart" style="height: 380px;"></div>
  <details><summary>Show table</summary>
    <table><thead><tr>
      <th>Competitor</th><th class="num">Overlap</th>
      <th class="num"><span class="ref-name"></span> cheaper</th>
      <th class="num">Comp cheaper</th>
      <th class="num">Same</th>
      <th class="num"><span class="ref-name"></span> win %</th>
      <th class="num">Median gap %</th>
    </tr></thead><tbody id="h2h-tbody"></tbody></table>
  </details>
</section>

<section id="density">
  <h2>4. Competitor density &mdash; <span class="ref ref-name"></span>'s catalog</h2>
  <div class="subtitle">How many other sellers compete on each SKU the reference lists. Hover a bar to see which sellers most often compete in that density bucket.</div>
  <div id="chart-density" class="chart" style="height: 380px;"></div>
</section>

<section id="overlap">
  <h2>5. Catalog overlap with <span class="ref ref-name"></span></h2>
  <div class="subtitle">Each bar shows a competitor's full catalog split into the SKUs that overlap with the reference (blue) and the rest (gray). Hover for both percentages.</div>
  <div id="chart-overlap" class="chart" style="height: 460px;"></div>
</section>

<section id="delivery">
  <h2>6. Delivery cost benchmark</h2>
  <div class="subtitle">Median delivery cost by major seller. Reference seller highlighted in red.</div>
  <div id="chart-delivery" class="chart" style="height: 420px;"></div>
</section>

<section id="opportunity">
  <h2>7. Pricing opportunities for <span class="ref ref-name"></span></h2>
  <div class="subtitle">Three lists for the buyer team.</div>

  <div class="controls" style="margin-top: 8px;">
    <strong style="font-size: 13px;">Top 25 SKUs where <span class="ref-name"></span> is most overpriced</strong>
  </div>
  <div class="table-scroll"><table><thead><tr>
    <th>EAN</th><th>Title</th><th class="num">Ref &euro;</th><th>Cheapest seller</th>
    <th class="num">Their &euro;</th><th class="num">Gap %</th><th class="num">Rivals</th>
  </tr></thead><tbody id="tbody-worst"></tbody></table></div>

  <div class="controls" style="margin-top: 24px;">
    <strong style="font-size: 13px;">Top 25 SKUs where <span class="ref-name"></span> is far cheapest (potential margin left on table)</strong>
  </div>
  <div class="table-scroll"><table><thead><tr>
    <th>EAN</th><th>Title</th><th class="num">Ref &euro;</th><th>Next-cheapest seller</th>
    <th class="num">Their &euro;</th><th class="num">Gap %</th><th class="num">Rivals</th>
  </tr></thead><tbody id="tbody-best"></tbody></table></div>

  <div class="controls" style="margin-top: 24px;">
    <strong style="font-size: 13px;">Sample of monopoly SKUs (only <span class="ref-name"></span> on Allegro.sk)</strong>
  </div>
  <div class="table-scroll"><table><thead><tr>
    <th>EAN</th><th>Title</th><th class="num">Ref &euro;</th>
  </tr></thead><tbody id="tbody-mono"></tbody></table></div>
</section>

<section id="explorer">
  <h2>8. SKU explorer</h2>
  <div class="subtitle">Search any EAN or title. Click a row to see every seller's offer with the reference highlighted.</div>
  <div class="controls">
    <input type="text" id="search-input" placeholder="Search by EAN or title..." />
    <select id="filter-bucket">
      <option value="">All positions</option>
      <option value="A">A: Deep cheaper</option>
      <option value="B">B: Cheaper</option>
      <option value="C">C: Parity</option>
      <option value="D">D: Pricier</option>
      <option value="E">E: Deep pricier</option>
      <option value="M">Monopoly</option>
      <option value="N">Not listed by reference</option>
    </select>
    <span style="font-size: 12px; color: var(--muted);" id="search-count"></span>
  </div>
  <div class="table-scroll" style="max-height: 420px;"><table><thead><tr>
    <th>EAN</th><th>Title</th><th class="num">Ref &euro;</th><th class="num">Best comp &euro;</th>
    <th>Cheapest seller</th><th class="num">Gap %</th><th class="num">Rivals</th>
  </tr></thead><tbody id="tbody-search"></tbody></table></div>
  <div id="sku-detail" style="margin-top: 16px; display: none;">
    <h3 id="sku-detail-title" style="font-size: 14px; margin: 0 0 8px 0;"></h3>
    <div id="sku-detail-chart" style="height: 320px;"></div>
    <div class="table-scroll" style="max-height: 280px; margin-top: 12px;">
      <table><thead><tr>
        <th>Seller</th><th class="num">Price &euro;</th><th class="num">Delivery &euro;</th><th class="num">Total &euro;</th>
      </tr></thead><tbody id="tbody-sku-offers"></tbody></table>
    </div>
  </div>
</section>

<div class="footer-note">Generated 2026-04-27 &middot; allegro_offers_wide.xlsx</div>
</main>

<script>
const DATA = __DATA_PLACEHOLDER__;

// ---------- Indices ----------
const byEan = new Map();
const bySeller = new Map();
for (const o of DATA.offers) {
  if (!byEan.has(o.e)) byEan.set(o.e, []);
  byEan.get(o.e).push(o);
  if (!bySeller.has(o.s)) bySeller.set(o.s, []);
  bySeller.get(o.s).push(o);
}

// ---------- State ----------
let currentRef = null;
let currentPerSku = [];
const highlighted = new Set(); // sellers ticked for highlighting

// ---------- Helpers ----------
const fmtEur = v => v == null ? '-' : v.toFixed(2) + ' €';
const fmtPct = v => v == null ? '-' : (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
const median = arr => {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const gapBadge = g => {
  if (g == null) return '<span class="badge badge-neutral">no rival</span>';
  if (g >= 10) return '<span class="badge badge-bad">' + fmtPct(g) + '</span>';
  if (g >= 2) return '<span class="badge badge-warn">' + fmtPct(g) + '</span>';
  if (g <= -2) return '<span class="badge badge-good">' + fmtPct(g) + '</span>';
  return '<span class="badge badge-neutral">' + fmtPct(g) + '</span>';
};
const bucketCode = g => {
  if (g == null) return 'M';
  if (g <= -10) return 'A';
  if (g <= -2) return 'B';
  if (g <= 2) return 'C';
  if (g <= 10) return 'D';
  return 'E';
};
const tickFmt = name => highlighted.has(name) ? '<b>' + name + '</b>' : name;
const cellSeller = name => {
  if (!name) return '-';
  const cls = highlighted.has(name) ? ' class="seller-name highlighted"' : ' class="seller-name"';
  return '<span' + cls + '>' + name + '</span>';
};

// ---------- Compute per-SKU view for a reference seller ----------
function computeRef(ref) {
  const refOffers = bySeller.get(ref) || [];
  const perSku = [];
  for (const o of refOffers) {
    const allOnEan = byEan.get(o.e) || [];
    let bestOther = null;
    let othersCount = 0;
    const others = [];
    for (const x of allOnEan) {
      if (x.s === ref) continue;
      othersCount++;
      others.push(x.s);
      if (!bestOther || x.t < bestOther.t) bestOther = x;
    }
    const gap = bestOther ? (o.t - bestOther.t) / bestOther.t * 100 : null;
    perSku.push({
      ean: o.e,
      title: DATA.titles[o.e] || '',
      refTotal: o.t, refPrice: o.p, refDelivery: o.d,
      bestSeller: bestOther ? bestOther.s : null,
      bestTotal: bestOther ? bestOther.t : null,
      compCount: othersCount,
      compSellers: others,
      gapPct: gap,
      bucket: bucketCode(gap),
    });
  }
  return perSku;
}

// ---------- Selector bar ----------
function buildSelectors() {
  const refWrap = document.getElementById('seller-buttons');
  refWrap.innerHTML = DATA.top_sellers.map(s => {
    const stats = DATA.seller_stats[s] || {skus: 0};
    return `<button class="seller-btn" data-seller="${s}">${s}<span class="count">${stats.skus}</span></button>`;
  }).join('');
  refWrap.querySelectorAll('.seller-btn').forEach(btn => {
    btn.addEventListener('click', () => setReference(btn.dataset.seller));
  });

  const chkWrap = document.getElementById('highlight-buttons');
  // Use top 15 sellers for highlight options to give more flexibility
  const allOffersBySeller = [...bySeller.entries()].map(([s, arr]) => [s, arr.length]).sort((a,b) => b[1]-a[1]);
  const highlightOptions = allOffersBySeller.slice(0, 15).map(x => x[0]);
  chkWrap.innerHTML = highlightOptions.map(s => {
    const stats = DATA.seller_stats[s] || {skus: 0};
    return `<button class="seller-chk" data-seller="${s}"><span class="box"></span>${s}<span class="count" style="opacity:0.6;font-size:11px">${stats.skus}</span></button>`;
  }).join('');
  chkWrap.querySelectorAll('.seller-chk').forEach(btn => {
    btn.addEventListener('click', () => toggleHighlight(btn.dataset.seller));
  });
}

function setReference(ref) {
  currentRef = ref;
  document.querySelectorAll('.seller-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.seller === ref));
  document.querySelectorAll('.ref-name').forEach(el => el.textContent = ref);
  currentPerSku = computeRef(ref);
  renderAll();
}

function toggleHighlight(s) {
  if (highlighted.has(s)) highlighted.delete(s);
  else highlighted.add(s);
  document.querySelectorAll('.seller-chk').forEach(b =>
    b.classList.toggle('on', highlighted.has(b.dataset.seller)));
  // Re-render every view that shows seller names so the bold treatment is consistent
  renderScatter();
  renderH2H();
  renderDensity();
  renderOverlap();
  renderDelivery();
  renderTables();
  renderExplorer();
}

// ---------- Renderers ----------
function renderKPI() {
  const ps = currentPerSku;
  const comp = ps.filter(r => r.gapPct != null);
  const winRate = comp.length ? (comp.filter(r => r.gapPct <= 0).length / comp.length * 100) : 0;
  const mono = ps.filter(r => r.compCount === 0).length;
  const deepPricey = ps.filter(r => r.gapPct != null && r.gapPct >= 10).length;
  const deepCheap = ps.filter(r => r.gapPct != null && r.gapPct <= -10).length;
  const med = median(comp.map(r => r.gapPct));
  document.getElementById('kpi-skus').textContent = ps.length.toLocaleString();
  document.getElementById('kpi-win').textContent = winRate.toFixed(1) + '%';
  document.getElementById('kpi-comp').textContent = comp.length.toLocaleString();
  document.getElementById('kpi-pricey').textContent = deepPricey.toLocaleString();
  document.getElementById('kpi-cheap').textContent = deepCheap.toLocaleString();
  document.getElementById('kpi-mono').textContent = mono.toLocaleString();
  document.getElementById('kpi-median').textContent = med == null ? '-' : fmtPct(med);
}

function renderBuckets() {
  const counts = {A: 0, B: 0, C: 0, D: 0, E: 0, M: 0};
  for (const r of currentPerSku) counts[r.bucket]++;
  const labels = ['A: Deep cheaper', 'B: Cheaper', 'C: Parity', 'D: Pricier', 'E: Deep pricier', 'No competitor'];
  const keys = ['A', 'B', 'C', 'D', 'E', 'M'];
  const colors = ['#16a34a', '#86efac', '#94a3b8', '#fbbf24', '#dc2626', '#cbd5e1'];
  const vals = keys.map(k => counts[k]);
  Plotly.react('chart-buckets', [{
    type: 'bar', x: labels, y: vals, marker: {color: colors},
    text: vals.map(v => v.toLocaleString()), textposition: 'outside'
  }], {
    margin: {l: 50, r: 20, t: 10, b: 80}, yaxis: {title: 'SKU count'},
    xaxis: {tickangle: -15}
  }, {responsive: true, displayModeBar: false});
}

function renderScatter() {
  const ps = currentPerSku.filter(r => r.bestTotal != null);
  const colors = ps.map(r => r.gapPct >= 10 ? '#dc2626' : r.gapPct >= 2 ? '#fbbf24'
                            : r.gapPct <= -10 ? '#16a34a' : r.gapPct <= -2 ? '#86efac' : '#94a3b8');
  const text = ps.map(r => 'EAN: ' + r.ean + '<br>' + currentRef + ': ' + r.refTotal.toFixed(2) + '€<br>Best rival: ' + tickFmt(r.bestSeller) + ' @ ' + r.bestTotal.toFixed(2) + '€<br>Gap: ' + r.gapPct.toFixed(1) + '%<br>Rivals: ' + r.compCount);
  const maxV = Math.max(1, ...ps.map(r => Math.max(r.refTotal, r.bestTotal)));
  Plotly.react('chart-scatter', [{
    x: ps.map(r => r.bestTotal), y: ps.map(r => r.refTotal),
    mode: 'markers', type: 'scatter',
    marker: {color: colors, size: 6, opacity: 0.65, line: {width: 0}},
    text: text, hoverinfo: 'text', name: 'SKUs'
  }, {
    x: [0.1, maxV], y: [0.1, maxV], mode: 'lines',
    line: {dash: 'dash', color: '#0f172a', width: 1},
    name: 'Parity', hoverinfo: 'skip'
  }], {
    margin: {l: 60, r: 20, t: 30, b: 50},
    xaxis: {title: 'Cheapest competitor total (€)', type: 'log'},
    yaxis: {title: currentRef + ' total (€)', type: 'log'},
    showlegend: false
  }, {responsive: true});
}

function renderH2H() {
  const refOffers = bySeller.get(currentRef) || [];
  const refMap = new Map(refOffers.map(o => [o.e, o.t]));
  const rows = [];
  for (const s of DATA.top_sellers) {
    if (s === currentRef) continue;
    const others = bySeller.get(s) || [];
    let overlap = 0, refCheaper = 0, compCheaper = 0, same = 0;
    const gaps = [];
    for (const o of others) {
      const refT = refMap.get(o.e);
      if (refT == null) continue;
      overlap++;
      if (o.t < refT) compCheaper++;
      else if (o.t > refT) refCheaper++;
      else same++;
      gaps.push((o.t - refT) / refT * 100);
    }
    if (overlap === 0) continue;
    rows.push({seller: s, overlap, refCheaper, compCheaper, same,
               winRate: refCheaper / overlap * 100,
               medianGap: median(gaps)});
  }
  rows.sort((a, b) => b.winRate - a.winRate);

  const sorted = [...rows].sort((a, b) => a.winRate - b.winRate);
  const yLabels = sorted.map(d => tickFmt(d.seller));
  Plotly.react('chart-h2h', [
    {type: 'bar', orientation: 'h', name: currentRef + ' cheaper',
     y: yLabels, x: sorted.map(d => d.refCheaper),
     marker: {color: '#16a34a'},
     hovertemplate: '%{y}<br>' + currentRef + ' cheaper on %{x} SKUs<extra></extra>'},
    {type: 'bar', orientation: 'h', name: 'Same',
     y: yLabels, x: sorted.map(d => d.same),
     marker: {color: '#cbd5e1'},
     hovertemplate: '%{y}<br>Same price on %{x} SKUs<extra></extra>'},
    {type: 'bar', orientation: 'h', name: 'Competitor cheaper',
     y: yLabels, x: sorted.map(d => d.compCheaper),
     marker: {color: '#dc2626'},
     hovertemplate: '%{y}<br>Competitor cheaper on %{x} SKUs<extra></extra>'},
  ], {
    barmode: 'stack', margin: {l: 150, r: 20, t: 30, b: 40},
    xaxis: {title: 'SKUs in overlap'}, legend: {orientation: 'h', y: 1.15}
  }, {responsive: true, displayModeBar: false});

  document.getElementById('h2h-tbody').innerHTML = rows.map(d => `<tr>
    <td>${cellSeller(d.seller)}</td><td class="num">${d.overlap}</td>
    <td class="num">${d.refCheaper}</td><td class="num">${d.compCheaper}</td>
    <td class="num">${d.same}</td><td class="num">${d.winRate.toFixed(1)}%</td>
    <td class="num">${gapBadge(d.medianGap)}</td>
  </tr>`).join('');
}

function renderDensity() {
  // Group SKUs by competitor count; for each group, tally which sellers compete
  const groups = new Map();
  for (const r of currentPerSku) {
    if (!groups.has(r.compCount)) groups.set(r.compCount, {skuCount: 0, sellerFreq: new Map()});
    const g = groups.get(r.compCount);
    g.skuCount++;
    for (const sel of r.compSellers) {
      g.sellerFreq.set(sel, (g.sellerFreq.get(sel) || 0) + 1);
    }
  }
  const xs = [...groups.keys()].sort((a, b) => a - b);
  const ys = xs.map(x => groups.get(x).skuCount);
  const hovertext = xs.map(x => {
    const g = groups.get(x);
    if (g.sellerFreq.size === 0) {
      return '<b>' + x + ' competitors</b><br>SKUs: ' + g.skuCount + '<br><i>No rivals (monopoly)</i>';
    }
    const top = [...g.sellerFreq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
    const lines = top.map(([s, n]) => '· ' + tickFmt(s) + ' (' + n + ' SKUs, ' + (n / g.skuCount * 100).toFixed(0) + '%)');
    return '<b>' + x + ' competitor' + (x === 1 ? '' : 's') + '</b><br>' +
           'SKUs at this density: ' + g.skuCount + '<br><br>' +
           '<b>Top sellers competing here:</b><br>' + lines.join('<br>');
  });
  Plotly.react('chart-density', [{
    type: 'bar', x: xs, y: ys, marker: {color: '#2563eb'},
    text: ys, textposition: 'outside',
    customdata: hovertext,
    hovertemplate: '%{customdata}<extra></extra>'
  }], {
    margin: {l: 60, r: 20, t: 30, b: 60},
    xaxis: {title: 'Number of competing sellers (excl. ' + currentRef + ')', dtick: 1},
    yaxis: {title: 'SKU count', type: 'log'},
    hoverlabel: {align: 'left'}
  }, {responsive: true, displayModeBar: false});
}

function renderOverlap() {
  const refEans = new Set((bySeller.get(currentRef) || []).map(o => o.e));
  const refSize = refEans.size;
  const rows = [];
  for (const [s, offers] of bySeller) {
    if (s === currentRef) continue;
    const sEans = new Set(offers.map(o => o.e));
    if (sEans.size < 30) continue;
    const ov = [...sEans].filter(e => refEans.has(e)).length;
    rows.push({
      seller: s,
      sellerSkus: sEans.size,
      overlap: ov,
      pctOfSeller: sEans.size ? ov / sEans.size * 100 : 0,
      pctOfRef: refSize ? ov / refSize * 100 : 0,
    });
  }
  rows.sort((a, b) => b.overlap - a.overlap);
  const top = rows.slice(0, 15);
  const sorted = [...top].sort((a, b) => a.overlap - b.overlap);
  const yLabels = sorted.map(d => tickFmt(d.seller));

  Plotly.react('chart-overlap', [
    {type: 'bar', orientation: 'h', name: 'Overlap with ' + currentRef,
     y: yLabels, x: sorted.map(d => d.overlap),
     marker: {color: '#2563eb'},
     text: sorted.map(d => d.overlap.toLocaleString()),
     textposition: 'inside', insidetextanchor: 'middle',
     textfont: {color: 'white', size: 11},
     customdata: sorted.map(d => [d.sellerSkus, d.pctOfSeller.toFixed(1), d.pctOfRef.toFixed(1)]),
     hovertemplate: '%{y}<br>Overlap: %{x} SKUs<br>%{customdata[1]}% of their %{customdata[0]} SKUs<br>%{customdata[2]}% of ' + currentRef + "'s " + refSize + ' SKUs<extra></extra>'},
    {type: 'bar', orientation: 'h', name: 'Their non-overlapping SKUs',
     y: yLabels, x: sorted.map(d => d.sellerSkus - d.overlap),
     marker: {color: '#cbd5e1'},
     text: sorted.map(d => (d.sellerSkus - d.overlap).toLocaleString()),
     textposition: 'inside', insidetextanchor: 'middle',
     textfont: {color: '#475569', size: 11},
     hovertemplate: '%{y}<br>Not in ' + currentRef + ': %{x} SKUs<extra></extra>'}
  ], {
    barmode: 'stack', margin: {l: 150, r: 30, t: 30, b: 40},
    xaxis: {title: "Competitor's catalog (SKU count)"},
    legend: {orientation: 'h', y: 1.12}
  }, {responsive: true, displayModeBar: false});
}

function renderDelivery() {
  const traces = DATA.top_sellers.map(s => {
    const offs = bySeller.get(s) || [];
    const vals = offs.filter(o => o.d != null).map(o => o.d).slice(0, 800);
    return {
      type: 'box', y: vals, name: tickFmt(s),
      marker: {color: s === currentRef ? '#dc2626' : '#2563eb'},
      boxpoints: false
    };
  });
  Plotly.react('chart-delivery', traces, {
    margin: {l: 50, r: 20, t: 30, b: 100},
    yaxis: {title: 'Delivery cost (€)', range: [0, 40]},
    xaxis: {tickangle: -30},
    showlegend: false
  }, {responsive: true, displayModeBar: false});
}

function renderTables() {
  const comp = currentPerSku.filter(r => r.gapPct != null);
  const worst = [...comp].sort((a, b) => b.gapPct - a.gapPct).slice(0, 25);
  const best = [...comp].sort((a, b) => a.gapPct - b.gapPct).slice(0, 25);
  const mono = currentPerSku.filter(r => r.compCount === 0).slice(0, 30);

  const renderRow = r => `<tr>
    <td class="ean">${r.ean}</td>
    <td class="title-cell" title="${(r.title || '').replace(/"/g, '&quot;')}">${r.title || ''}</td>
    <td class="num">${fmtEur(r.refTotal)}</td>
    <td>${cellSeller(r.bestSeller)}</td>
    <td class="num">${fmtEur(r.bestTotal)}</td>
    <td class="num">${gapBadge(r.gapPct)}</td>
    <td class="num">${r.compCount}</td>
  </tr>`;
  document.getElementById('tbody-worst').innerHTML = worst.map(renderRow).join('');
  document.getElementById('tbody-best').innerHTML = best.map(renderRow).join('');
  document.getElementById('tbody-mono').innerHTML = mono.map(r => `<tr>
    <td class="ean">${r.ean}</td>
    <td class="title-cell" title="${(r.title || '').replace(/"/g, '&quot;')}">${r.title || ''}</td>
    <td class="num">${fmtEur(r.refTotal)}</td>
  </tr>`).join('');
}

// ---------- Explorer ----------
let allSkuView = [];
function buildAllSkuView() {
  const refMap = new Map(currentPerSku.map(r => [r.ean, r]));
  allSkuView = [];
  for (const ean of byEan.keys()) {
    const refRow = refMap.get(ean);
    if (refRow) {
      allSkuView.push(refRow);
    } else {
      const offers = byEan.get(ean) || [];
      let cheapest = null;
      for (const o of offers) if (!cheapest || o.t < cheapest.t) cheapest = o;
      allSkuView.push({
        ean, title: DATA.titles[ean] || '',
        refTotal: null, bestSeller: cheapest ? cheapest.s : null,
        bestTotal: cheapest ? cheapest.t : null,
        compCount: offers.length, gapPct: null, bucket: 'N',
      });
    }
  }
}

function renderExplorer() {
  buildAllSkuView();
  const input = document.getElementById('search-input');
  const filter = document.getElementById('filter-bucket');
  const tbody = document.getElementById('tbody-search');
  const count = document.getElementById('search-count');

  function render() {
    const q = (input.value || '').toLowerCase().trim();
    const f = filter.value;
    let rows = allSkuView;
    if (q) rows = rows.filter(r => r.ean.includes(q) || (r.title || '').toLowerCase().includes(q));
    if (f) rows = rows.filter(r => r.bucket === f);
    count.textContent = rows.length.toLocaleString() + ' SKUs';
    rows = rows.slice(0, 300);
    tbody.innerHTML = rows.map(r => `<tr data-ean="${r.ean}" style="cursor:pointer">
      <td class="ean">${r.ean}</td>
      <td class="title-cell" title="${(r.title || '').replace(/"/g, '&quot;')}">${r.title || ''}</td>
      <td class="num">${fmtEur(r.refTotal)}</td>
      <td class="num">${fmtEur(r.bestTotal)}</td>
      <td>${cellSeller(r.bestSeller)}</td>
      <td class="num">${gapBadge(r.gapPct)}</td>
      <td class="num">${r.compCount}</td>
    </tr>`).join('');
    tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => showSKU(tr.dataset.ean));
    });
  }
  input.oninput = render;
  filter.onchange = render;
  render();
}

function showSKU(ean) {
  const offers = (byEan.get(ean) || []).slice().sort((a, b) => a.t - b.t);
  if (!offers.length) return;
  const detail = document.getElementById('sku-detail');
  detail.style.display = 'block';
  document.getElementById('sku-detail-title').textContent =
    ean + ' — ' + (DATA.titles[ean] || '');

  const colors = offers.map(o => o.s === currentRef ? '#dc2626' : (highlighted.has(o.s) ? '#f59e0b' : '#2563eb'));
  Plotly.react('sku-detail-chart', [{
    type: 'bar', x: offers.map(o => tickFmt(o.s)), y: offers.map(o => o.t),
    marker: {color: colors}, text: offers.map(o => o.t.toFixed(2) + '€'),
    textposition: 'outside',
    customdata: offers.map(o => [o.p, o.d]),
    hovertemplate: '%{x}<br>Price: %{customdata[0]}€<br>Delivery: %{customdata[1]}€<br>Total: %{y}€<extra></extra>'
  }], {
    margin: {l: 50, r: 10, t: 30, b: 100},
    xaxis: {tickangle: -45}, yaxis: {title: 'Total (€)'}
  }, {responsive: true, displayModeBar: false});

  document.getElementById('tbody-sku-offers').innerHTML = offers.map(o => {
    const flag = o.s === currentRef ? ' <span class="badge badge-bad">REFERENCE</span>' : '';
    return `<tr><td>${cellSeller(o.s)}${flag}</td>
      <td class="num">${fmtEur(o.p)}</td>
      <td class="num">${fmtEur(o.d)}</td>
      <td class="num"><strong>${fmtEur(o.t)}</strong></td></tr>`;
  }).join('');
  detail.scrollIntoView({behavior: 'smooth', block: 'start'});
}

function renderAll() {
  renderKPI();
  renderBuckets();
  renderScatter();
  renderH2H();
  renderDensity();
  renderOverlap();
  renderDelivery();
  renderTables();
  renderExplorer();
  document.getElementById('sku-detail').style.display = 'none';
}

// ---------- Init ----------
document.getElementById('hd-stats').textContent =
  DATA.eans_total.toLocaleString() + ' EANs · ' +
  DATA.sellers_total.toLocaleString() + ' sellers · ' +
  DATA.offers_total.toLocaleString() + ' live offers';
buildSelectors();
setReference(DATA.top_sellers[0]);
</script>

</body>
</html>"""

html = html.replace('__DATA_PLACEHOLDER__', data_str)

with open('C:/Coding/price-list/index.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('Wrote index.html:', round(len(html) / 1024, 1), 'KB')
