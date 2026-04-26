# Allegro.sk Scraping Knowledge Base

> Reference document for scraping allegro.sk and similar Allegro-group sites (allegro.pl, allegro.cz, etc.)

---

## 1. Site Architecture

### Domain & URL Patterns

| Purpose | Pattern | Example |
|---|---|---|
| EAN search | `/vyhladavanie?string={EAN}` | `/vyhladavanie?string=3253561947490` |
| Product group page | `/produkt/{slug}-{uuid}` | `/produkt/stanley-kufor-...-743e914d...` |
| Product + selected offer | `/produkt/{slug}-{uuid}?offerId={id}` | `...?offerId=17758115219` |
| Individual offer | `/oferta/{slug}-{id}` | `/oferta/stanley-...` |
| Seller shop | `/obchod/{seller-name}` | `/obchod/MALLShop-sk` |
| Seller profile | `/uzivatel/{name}` | `/uzivatel/abc-tools` |

### Key Navigation Flow
```
EAN search → search results page (/vyhladavanie)
    → /produkt/ page (grouped offers for one product)
        → click "Všetky ponuky" → full offer list loads
            → articles with Predajca: label = scrapeable offers
```

### URL Resolution Edge Cases
1. **Direct redirect**: Search may redirect straight to `/produkt/` (single result). Detect via `page.url` after goto.
2. **Oferta redirect**: Search may only surface `/oferta/` links (single seller). Navigate there, find the parent `/produkt/` link on that page.
3. **#inne-oferty-produktu anchor**: Added to product URL to scroll to offers section — Allegro strips it and adds `?offerId=` instead. The anchor ID no longer exists in the DOM (`document.getElementById("inne-oferty-produktu")` returns null).

---

## 2. Bot Protection

### Stack
- **Cloudflare** — primary challenge layer (JS challenge, CAPTCHA, `cf_clearance` cookie)
- **DataDome** — secondary behavioral analysis (`datadome` cookie)

### What triggers detection
- Headless browser fingerprints (`navigator.webdriver = true`, missing plugins, wrong viewport)
- No-mouse, no-scroll navigation (silent CDP automation)
- Burst requests without delays
- Python's default TLS fingerprint (if using `requests`/`httpx` directly)
- Chrome launched by Playwright (injects automation markers) vs. user-launched Chrome

### Proven bypass: CDP to user-launched Chrome
Connect Playwright to a **user-launched Chrome** via Chrome DevTools Protocol instead of launching Playwright's own Chromium. The browser has a real fingerprint; Playwright just drives it remotely.

**Required Chrome flags:**
```bash
open -na "Google Chrome" --args \
  --remote-debugging-port=9222 \
  --user-data-dir=/tmp/allegro-chrome-1 \
  --enable-automation \          # required for CDP browser-management (Browser.setDownloadBehavior)
  --disable-blink-features=AutomationControlled \  # hides navigator.webdriver in JS
  https://allegro.sk
```

**Why `--enable-automation` is required:** Playwright calls `Browser.setDownloadBehavior` on connect. Without this flag Chrome rejects it with `"Browser context management is not supported"` and disconnects. It shows a banner ("Chrome is being controlled...") but does NOT set `navigator.webdriver = true` — Cloudflare JS detection is unaffected.

**Why NOT launch Chrome via Playwright's `launch()`:** Playwright injects `navigator.webdriver = true` and other automation markers. Cloudflare detects these even with stealth patches.

### Cookie persistence
After the user solves the Cloudflare challenge once, save all browser cookies to disk. On the next run, inject them before any navigation — `cf_clearance` is typically valid 24h+.

```python
# Save (after challenge solved):
cookies = await ctx.cookies()
Path("allegro_cookies.json").write_text(json.dumps(cookies))

# Load (on next run):
await ctx.add_cookies(json.loads(Path("allegro_cookies.json").read_text()))
```

Key cookies to preserve: `cf_clearance`, `datadome`, `_cmuid`, `gdpr_permission_given`.

---

## 3. Page Rendering Behaviour

### Everything is JS-rendered
Allegro is a React SPA. `wait_until="domcontentloaded"` fires on the HTML skeleton — **product links and offer articles are not yet in the DOM**.

| Element | When it appears | How to wait |
|---|---|---|
| `/produkt/` links on search page | After JS renders results (~1-3s) | `wait_for_selector('a[href*="/produkt/"]', timeout=10s)` |
| `/oferta/` links (fallback) | After JS renders results | `wait_for_selector('a[href*="/oferta/"]', timeout=5s)` |
| Offer articles with "Predajca:" | After product page JS fully loads | `wait_for_function("...some(a => a.innerText.includes('Predajca:'))", timeout=20s)` |

**Never** use `wait_for_selector("article")` alone — review articles appear first (top of page) and satisfy this selector before offer articles load.

### Page sections on `/produkt/` page
Top to bottom:
1. **Buy box** — selected offer price, "Pridať do košíka" / "Kúpiť teraz" button
2. **Reviews** (`article` elements) — appear early, have NO "Predajca:" label
3. **Najrýchlejšie** — fastest-delivery offers section (`h2/h3` contains "Najr")
4. **Najlacnejšie** — cheapest offers section (`h2/h3` contains "Najla")
5. **Všetky ponuky** — all offers section (target for scraping)

"Všetky ponuky" may be collapsed behind a button. Must click `button:has-text("ponuky")` or `a:has-text("Všetky")` before articles load.

---

## 4. Data Extraction

### Offer article structure
Each offer is an `<article>` element. Key text patterns (Slovak):

| Field | Regex | Notes |
|---|---|---|
| Seller name | `/Predajca:\s*([^\n|]+)/` | Only present in offer articles, not reviews |
| Price | `/(\d+[,.]\d{2})\s*€/` | European format: comma decimal |
| Delivery | `/(\d+[,.]\d{2})\s*€\s*s doru/` | "s doručením" = "with delivery" |
| Seller URL | `a[href*="/obchod/"]` or `a[href*="/uzivatel/"]` | Inside the article |
| Title | `article h2, article h3` | First heading inside article |

Convert commas to dots for float parsing: `"25,91".replace(",", ".")`.

### Box price (buy-box selected offer price)
Priority order:
1. `meta[itemprop="price"]` content attribute — schema.org, most reliable
2. Walk up from "Pridať do košíka"/"Kúpiť teraz" button — find nearest ancestor with `(\d+[,.]\d{2})\s*€`
3. Section containing "Podmienky ponuky" text
4. `[itemprop="offers"] [itemprop="price"]`
5. First EUR price in top 3000 chars of `document.body.innerText`

### Filtering out non-offer articles
Before extracting, mark excluded articles via their section headings:
```javascript
const excluded = new Set();
for (const h of document.querySelectorAll("h2, h3, h4")) {
    const t = h.innerText || "";
    if (!t.includes("Najr") && !t.includes("Najla")) continue;
    let node = h.parentElement;
    for (let i = 0; i < 8; i++) {
        if (!node || node === document.body) break;
        const arts = node.querySelectorAll("article");
        if (arts.length) { arts.forEach(a => excluded.add(a)); break; }
        node = node.parentElement;
    }
}
// Then filter: [...document.querySelectorAll("article")].filter(a => !excluded.has(a))
```

---

## 5. Anti-Detection Timing

| Pause | Duration | Purpose |
|---|---|---|
| Pre-scrape (per EAN) | 4–7s random | Simulate reading before navigating |
| After "Všetky ponuky" click | 1.5s | Let offer list expand |
| Post-scrape (per EAN) | 2–5s random | Simulate post-read pause |
| Long break (every 8–15 EANs) | 20–30s random | Simulate human fatigue/distraction |

Randomise all intervals — fixed intervals are a bot signal.

---

## 6. Multi-Browser Parallelism

Run 2–3 separate Chrome processes on sequential ports. Each is an independent browser fingerprint and Cloudflare session.

- Port 9222 → `/tmp/allegro-chrome-1`
- Port 9223 → `/tmp/allegro-chrome-2`
- Port 9224 → `/tmp/allegro-chrome-3`

Each browser gets the same `allegro_cookies.json` injected. Use a shared `asyncio.Queue` for EAN distribution and `asyncio.Lock` for CSV writes. Throughput scales ~linearly with browsers.

---

## 7. Output Schema

### allegro_offers.csv
```
ean, title, seller, seller_url, price_eur, delivery_eur, box_price_eur, scraped_at
```
- One row per offer (multiple rows per EAN)
- `seller_url`: often empty — Allegro obfuscates seller links
- `box_price_eur`: constant per EAN (the featured offer price)
- `scraped_at`: ISO 8601 UTC

---

## 8. Known Pitfalls & Fixes

| Problem | Root Cause | Fix |
|---|---|---|
| `Browser context management is not supported` | Chrome missing `--enable-automation` | Add flag on launch |
| "not found" despite product existing | `/produkt/` links not yet rendered at `domcontentloaded` | `wait_for_selector('a[href*="/produkt/"]')` |
| "not found" on product page | Review articles satisfy `wait_for_selector("article")` before offer articles load | `wait_for_function` on `"Predajca:"` text |
| Empty seller list | `wait_for_function` matched review star ratings (e.g. "4,90 €") | Use `"Predajca:"` as the wait condition, not price regex |
| Duplicate sellers per EAN | Same seller appears in Najrýchlejšie + Všetky ponuky | Filter excluded sections before extracting |
| `#inne-oferty-produktu` anchor removed by Allegro | Allegro replaced anchor with `?offerId=` param | Don't rely on the anchor; wait for "Predajca:" instead |
| Cloudflare blocks new tab | New tabs opened by Playwright get fresh challenge | Reuse existing tab (`ctx.pages[0]`) — never open new tabs |

---

## 9. Transferable Patterns for Similar Sites

- **React SPAs**: Always wait for specific text/data markers, never just DOM events
- **Cloudflare sites**: CDP to user-launched browser + cookie persistence is the most reliable approach
- **Multi-section pages**: Identify section headings and walk the DOM to exclude unwanted article groups
- **European price formats**: Regex `(\d+[,.]\d{2})\s*€`, then `.replace(",", ".")`
- **Buy-box price**: Walk up from the CTA button ("Add to cart") — it's always near the primary price
- **Resume logic**: Two modes — skip-any (fast resume after interruption) vs. skip-found-only (retry failed lookups)
