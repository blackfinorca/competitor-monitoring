"""Shared constants for Allegro.sk scrapers."""

EXTRACT_JS = """() => {
    return [...document.querySelectorAll("article")].map(a => {
        const text = a.innerText || "";
        const sellerM = text.match(/Predajca:\\s*([^\\n|]+)/);
        const priceM  = text.match(/(\\d+[,.]\\d{2})\\s*\\u20ac/);
        const delivM  = text.match(/(\\d+[,.]\\d{2})\\s*\\u20ac\\s*s doru/);
        const links   = [...a.querySelectorAll("a")].filter(
            l => l.href.includes("/obchod/") || l.href.includes("/uzivatel/")
        );
        const titleEl = a.querySelector("h2, h3");
        return {
            title:        titleEl ? titleEl.innerText.trim() : "",
            seller:       sellerM ? sellerM[1].trim() : "",
            price_eur:    priceM  ? priceM[1].replace(",", ".") : null,
            delivery_eur: delivM  ? delivM[1].replace(",", ".") : null,
            seller_url:   links[0] ? links[0].href : ""
        };
    });
}"""
