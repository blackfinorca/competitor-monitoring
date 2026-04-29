"""Product category classification and backfill helpers."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from agnaradie_pricing.catalogue.normalise import normalise_ean
from agnaradie_pricing.db.models import CompetitorListing, Product


CATEGORY_NAMES = (
    "Power tools",
    "Garden tools",
    "Construction tools",
    "Household & hardware",
    "Machining tools",
    "Tool holders",
    "Abrasives",
    "Screwdrivers & keys",
    "Wrenches & sockets",
    "Pliers & cutters",
    "Striking tools",
    "Cutting hand tools",
    "Measurement tools",
    "Clamps",
    "Electrical tools",
    "Welding tools",
    "Tool storage",
    "Fasteners",
    "Chemicals",
    "Safety gear",
    "Spare parts",
    "Other",
)


_CATEGORY_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Other",
        (
            r"\bohrievac\s+plynovy\b",
            r"\bpistol\s+tavna\b",
            r"\bpistole\s+na\s+kartuse\b",
            r"\bpistol\s+na\s+pu\s+penu\b",
            r"\bpistole\s+na\s+pu\s+penu\b",
            r"\bdavkovacia\s+pistol\b",
            r"\btavna\s+pistol\b",
            r"\bsada\s+naradia\b",
            r"\bset\s+naradia\b",
            r"\bzastrcka\s+ochrany\s+motora\b",
            r"\buniverzalna\s+ostricka\b",
        ),
    ),
    (
        "Fasteners",
        (
            r"\bklince\s+do\s+klincovacky\b",
            r"\bspona\s+s\s+uzkym\s+chrbtom\b",
            r"\bhak\s+na\s+naradie\b",
        ),
    ),
    (
        "Power tools",
        (
            r"\bvrtacka\b",
            r"\bvrtacky\b",
            r"\baku\s+vrtack",
            r"\bakumulatorov[ey]\s+vrtack",
            r"\b(?:aku|akumulatorova|elektricka|uhlova|priama|stolova|excentricka|pasova|deltova)\s+\w*\s*bruska\b",
            r"\bexcentrick",
            r"\boscilačn",
            r"\boscilacn",
            r"\bhoblik\b",
            r"\bhobliky\b",
            r"\bklincovacka\b",
            r"\bklincovacky\b",
            r"\bmiesadlo\b",
            r"\blesticka\b",
            r"\blesticky\b",
            r"\baku\s+pila\b",
            r"\belektricka\s+pila\b",
            r"\bkotoucova\s+pila\b",
            r"\bkotucova\s+pila\b",
            r"\bpriamociara\s+pila\b",
            r"\bpokosova\s+pila\b",
            r"\bretazova\s+pila\b",
        ),
    ),
    (
        "Garden tools",
        (
            r"\bbambus\b",
            r"\bkosacka\b",
            r"\bkosacky\b",
            r"\bkultivator\b",
            r"\bkultivatory\b",
            r"\bhrable\b",
            r"\bpostrekovac\b",
            r"\bpostrekovacu\b",
            r"\bzavlaz",
            r"\birigac",
            r"\bna\s+travu\b",
            r"\bgarden\b",
        ),
    ),
    (
        "Construction tools",
        (
            r"\bvibrator\s+do\s+beton",
            r"\bbatonovy\s+vibrator\b",
            r"\bsadrokarton\b",
            r"\bhladitko\b",
            r"\bomiet",
            r"\bstierk",
            r"\bbeton",
        ),
    ),
    (
        "Household & hardware",
        (
            r"\bbateria\s+longlife\b",
            r"\bdekoracia\b",
            r"\bmagichome\b",
            r"\bzamok\b",
            r"\bzamek\b",
            r"\bvisaci\s+zamok\b",
            r"\bvisiaci\s+zamok\b",
            r"\brebrik\b",
            r"\brebriky\b",
            r"\bumyvadlova\s+.*bateria\b",
            r"\bumyvadlova\s+stojankova\s+bateria\b",
            r"\bdrezova\s+.*bateria\b",
            r"\bstojankova\s+bateria\b",
            r"\bplachta\b",
            r"\bzakryvacia\b",
            r"\bkrycia\s*folia\b",
            r"\bfolia\s+krycia\b",
            r"\btarpaulin\b",
            r"\bkosik\b",
            r"\bkose\b",
        ),
    ),
    (
        "Spare parts",
        (
            r"\bnahradn[yae]\b",
            r"\bnahradni\b",
            r"\bnahrada\b",
            r"\bnasada\b",
            r"\bcelust(?:e|i)\b",
            r"\bnahradni\s+hlava\b",
            r"\bochranny\s+kryt\b",
            r"\bkontaktny\s+valec\b",
            r"\bmatrica\b",
            r"\bvodiaci\s+valcek\b",
            r"\bkoleno\b",
            r"\bkus\s+retazca\b",
            r"\bobjimka\s+spojky\b",
            r"\bprotiprachovy\s+filter\b",
            r"\bfiltracne\s+vrecko\b",
            r"\bfiltracni\s+vrecko\b",
            r"\bpapierove\s+filtracne\s+vrecko\b",
            r"\bvrecko\s+na\s+prach\b",
            r"\bdyza\b",
            r"\btryska\b",
            r"\brukovat\b",
            r"\bhadic\b",
            r"\bsuprava\s+hadic\b",
            r"\bpre\s+(?:klieste|skrutkovac|kladivo|drziak|odihl)",
        ),
    ),
    (
        "Tool holders",
        (
            r"\bsklucovadl",
            r"\bsklicidlo\b",
            r"\bsklicidl",
            r"\bupinacie\s+puzdro\b",
            r"\bupinaci[ae]\s+puzdro\b",
            r"\bkliestin",
            r"\bklestin",
            r"\bdrziak\s+zavitnik",
            r"\bpredlzenie\s+sklucovad",
            r"\bnastrojovy\s+drziak\b",
            r"\bprogramovacia\s+objimka\b",
            r"\bredukcne\s+objimky\b",
            r"\badaptery\s+quickin\b",
            r"\bpredlzenie\b",
            r"\brychlovymenn[ae]\s+sklicidlo\b",
            r"\brychlovymenna\s+vlozka\b",
            r"\butahovaci\s+cap\b",
            r"\bsveraci\s+pouzdro\b",
            r"\bredukcni\s+zdirka\b",
            r"\bredukcni\s+pouzdro\b",
            r"\bparalelnych\s+podloziek\b",
            r"\bvodici\s+cep\b",
            r"\bvodiaci\s+cap\b",
            r"\badapter\b",
        ),
    ),
    (
        "Machining tools",
        (
            r"\bvrtak\b",
            r"\bvrtak",
            r"\bvrtaky\b",
            r"\bvrtaci\b",
            r"\bhss\b",
            r"\bsds\s+plus\b",
            r"\bnavrtavak\b",
            r"\bzahlubnik\b",
            r"\bfreza\b",
            r"\bfrez",
            r"\bzavitnik",
            r"\bvystruznik",
            r"\bzahlbnik",
            r"\bzahlub",
            r"\bjadrovy\s+vrtak\b",
            r"\bkorunk",
            r"\bdierovk",
            r"\bodihl",
            r"\bodhrotovac",
            r"\bvyvrtav",
            r"\brezna\s+dostick",
            r"\brezna\s+platnick",
            r"\bdostick",
            r"\bvbd\b",
            r"\bbritova\s+destick",
            r"\bvymenna\s+britova\s+destick",
            r"\bsoustruznick",
            r"\bsustruznick",
            r"\bsoustruh",
            r"\bsustrnick",
            r"\btvarniaci\s+nastroj\b",
            r"\bryhovac",
            r"\bradlovac",
            r"\bvrtacka\b",
        ),
    ),
    (
        "Abrasives",
        (
            r"\bbrusn",
            r"\bbrusivo\b",
            r"\bbrusny\s+kotuc\b",
            r"\brezny\s+kotuc\b",
            r"\bkotuc\s+(?:rezny|brusny)",
            r"\bbrusny\s+pas\b",
            r"\blamelov",
            r"\bsmirgl",
            r"\bcerabond\b",
            r"\bdiamantov[yey]\s+kotuc\b",
            r"\bdiamantov[yey]\s+rezaci\s+kotuc\b",
            r"\brezaci\s+kotuc\b",
            r"\bkartac",
            r"\bkefa\b",
            r"\blestick",
            r"\bcubitron\b",
            r"\bkotucd\d",
            r"\bkotuc\s*d\d",
            r"\bunasaci\s+tanier\b",
            r"\brunovy\s+pas\b",
            r"\bdelici\s+kotouc\b",
            r"\bdeliaci\s+kotuc\b",
            r"\bruno\s+univerzalne\b",
            r"\bplstene\s+teliesko\b",
            r"\bbristle\s+disc\b",
            r"\bleštiac",
            r"\blestiac",
        ),
    ),
    (
        "Screwdrivers & keys",
        (
            r"\bskrutkovac",
            r"\bbit\b",
            r"\bbity\b",
            r"\bsroubovak",
            r"\btorx\b",
            r"\bimbus\b",
            r"\bsesthran\b",
            r"\bvnutorny\s+sesthran\b",
            r"\bzahnutych\s+skrutkovacov\b",
        ),
    ),
    (
        "Wrenches & sockets",
        (
            r"\bkluc\b",
            r"\bklucov\b",
            r"(?<!klestovy\s)\bklic\b",
            r"(?<!klestove\s)\bklice\b",
            r"(?<!klestovych\s)\bklicu\b",
            r"\bvidlicov",
            r"\bockov",
            r"\bnastrckov",
            r"\bnastrcna\s+hlavica\b",
            r"\bnastrcne\s+kluce\b",
            r"\bnastrcny\s+klic\b",
            r"\bnastrcny\s+kluc\b",
            r"\bnastrcny\s+nadstavec\b",
            r"\bnastavec\b",
            r"\bnadstavec\b",
            r"\bnadstavcov\b",
            r"\bhlavica\b",
            r"\bsocket\b",
            r"\bracn",
            r"\bgola\b",
            r"\borech\b",
            r"\bmomentov",
            r"\butahovak\b",
            r"\butahovac\b",
            r"\butahovanie\b",
            r"\brychlospoj",
            r"\brychlospojka\s+vzduchove\s+pripojky\b",
            r"\bsestihranny\s+hluboky\s+nastavec\b",
        ),
    ),
    (
        "Pliers & cutters",
        (
            r"\bklieste\b",
            r"\bkliesti\b",
            r"\bkleste\b",
            r"\bklestovy\s+klic\b",
            r"\bklestove\s+klice\b",
            r"\bkliestovy\b",
            r"\bkliestove\s+kluce\b",
            r"\bkliestovy\s+kluc\b",
            r"\bstipac",
            r"\bcvikac",
            r"\bkrimp",
            r"\blisovac",
            r"\bodizolovacie\s+klieste\b",
            r"\bnoznice\b",
            r"\bcobra\b",
            r"\balligator\b",
            r"\bcobolt\b",
            r"\bpreciforce\b",
            r"\bsuper\s+knips\b",
            r"\btwinforce\b",
            r"\bx-cut\b",
            r"\bpinzeta\b",
        ),
    ),
    (
        "Striking tools",
        (
            r"\bkladivo\b",
            r"\bkladiva\b",
            r"\bsekac\b",
            r"\bsekace\b",
            r"\bdlato\b",
            r"\bdlat\b",
            r"\bpriebojnik",
            r"\bvytlkac",
            r"\braznik",
            r"\bpalic",
            r"\bjamkovac\b",
            r"\bdulcik\b",
            r"\bderovac\b",
        ),
    ),
    (
        "Cutting hand tools",
        (
            r"\bnoz\b",
            r"\bnoze\b",
            r"\bcepel\b",
            r"\bpilovy\s+list\b",
            r"\bpilove\s+listy\b",
            r"\bpilovy\s+pas\b",
            r"\bpasovej\s+pily\b",
            r"\bponorny\s+list\b",
            r"\blist\s+\d",
            r"\bpilovych\s+platkov\b",
            r"\bpilovych\s+platku\b",
            r"\bpilovy\s+platek\b",
            r"\bpilovy\s+kotuc\b",
            r"\bpilove\s+kotuce\b",
            r"\bpilovych\s+vencov\b",
            r"\be-cut\b",
            r"\bcarbide\s+pro\b",
            r"\bpila\b",
            r"\bpilka\b",
            r"\bpilnik\b",
            r"\braspl",
            r"\brezak\b",
            r"\brezacka\b",
            r"\bskrabk",
            r"\bmultifunkcni\s+nuz\b",
            r"\bsada\s+nozov\b",
            r"\brydlo\b",
        ),
    ),
    (
        "Measurement tools",
        (
            r"\bvodovah",
            r"\bmeter\b",
            r"\bmerac",
            r"\bmeranie\b",
            r"\bdialkomer\b",
            r"\buholnik\b",
            r"\buhelnik\b",
            r"\buhlomer\b",
            r"\bposuvn",
            r"\blupa\b",
            r"\bkalibracny\s+kruzok\b",
            r"\bmedzny\s+kalibracny\s+kruzok\b",
            r"\bkalibracny\s+trn\b",
            r"\bzavitovy\s+medzny\s+kalibracny\s+trn\b",
            r"\bmikrometer\b",
            r"\bznackovac\b",
            r"\bpaint-riter\b",
            r"\bzkousecka\b",
            r"\bskusacka\b",
            r"\bsirkomer\b",
            r"\bhloubkomer\b",
            r"\bmierka\b",
            r"\bkruzitko\b",
            r"\bkruzidlo\b",
            r"\bzrkadlo\b",
            r"\bpravítko\b",
            r"\bpravitko\b",
        ),
    ),
    (
        "Clamps",
        (
            r"\bsvorka\b",
            r"\bzvierk",
            r"\bzverak",
            r"\bupinac\b",
            r"\bupinaci\b",
            r"\bstahovak",
            r"\bstahovac",
            r"\bdelici\s+pripravek\b",
            r"\botocna\s+zakladna\b",
            r"\bupinacia\s+jednotka\b",
            r"\bmagnet\s+plocheho\b",
            r"\bmagneticka\s+noha\b",
            r"\bmagneticky\s+drziak\b",
            r"\bsveraci\s+drzak\b",
            r"\bfixac",
        ),
    ),
    (
        "Electrical tools",
        (
            r"\bkabel\b",
            r"\bkabl",
            r"\bkablov",
            r"\brunpotec\b",
            r"\bxboard\b",
            r"\bdutink",
            r"\bkoncovk",
            r"\belektrikar",
            r"\bcee\b",
            r"\bzastrcka\s+cee\b",
            r"\bsklopny\s+reflektor\b",
            r"\breflektor\b",
            r"\bsvietidlo\b",
            r"\bpracovne\s+svetlo\b",
            r"\bvde\b",
        ),
    ),
    (
        "Welding tools",
        (
            r"\bhorak\b",
            r"\bpropan\b",
            r"\bzvarac",
            r"\bzvaranie\b",
            r"\bzvaracie\b",
            r"\bspajk",
            r"\bgce\b",
            r"\bredukcny\s+ventil\b",
        ),
    ),
    (
        "Tool storage",
        (
            r"\bvozik\b",
            r"\bl-?boxx\b",
            r"\bregal\b",
            r"\bracks\b",
            r"\bpracovny\s+stol\b",
            r"\bnabytok\b",
            r"\bdielensky\s+nabytok\b",
            r"\bbrasna\s+na\s+naradi\b",
            r"\bbrasna\s+na\s+naradie\b",
            r"\bvlozka\s+na\s+naradie\b",
            r"\bschranka\b",
            r"\bpolice\s+pro\s+stenu\b",
            r"\bkufor\b",
            r"\btaska\b",
            r"\bbrasna\b",
            r"\bbox\b",
            r"\borganiz",
            r"\bskrinka\b",
            r"\bpuzdro\b",
            r"\bdrziak\b",
        ),
    ),
    (
        "Fasteners",
        (
            r"\bskrutk",
            r"\bsroub",
            r"\bprichytk",
            r"\bviazaci",
            r"\bsesivacka\b",
            r"\bcalounick",
            r"\bspona\b",
            r"\bklince\b",
            r"\bhak\s+na\s+naradie\b",
            r"\bmatica\b",
            r"\bmatice\b",
            r"\bpodlozk",
            r"\bnit\b",
            r"\bnity\b",
            r"\bzavlack",
            r"\bzavitovy\s+cap\b",
            r"\bvrut\b",
            r"\bkotv",
            r"\bkolik\b",
        ),
    ),
    (
        "Chemicals",
        (
            r"\bmazivo\b",
            r"\bolej\b",
            r"\bsprej\b",
            r"\bcistic\b",
            r"\bcistiaci\s+prostriedok\b",
            r"\bsampon\b",
            r"\bautosampon\b",
            r"\blepiaca\s+paska\b",
            r"\blepici\s+paska\b",
            r"\btesa\b",
            r"\blepidl",
            r"\btmel\b",
            r"\btesniac",
            r"\bsilikon\b",
            r"\bloctite\b",
            r"\bwd-?40\b",
            r"\bpasta\b",
        ),
    ),
    (
        "Safety gear",
        (
            r"\bochranne\s+okuliare\b",
            r"\bokuliare\b",
            r"\brukavice\b",
            r"\brespirator\b",
            r"\bprilba\b",
            r"\bmaska\b",
            r"\bochrana\s+sluchu\b",
            r"\bchranic\b",
        ),
    ),
)


@dataclass(frozen=True)
class CategoryBackfillResult:
    products_seen: int
    products_updated: int
    category_counts: dict[str, int]


@dataclass(frozen=True)
class ListingCategoryBackfillResult:
    listings_seen: int
    listings_updated: int
    category_counts: dict[str, int]


def classify_product_category(*, title: str | None, brand: str | None = None) -> str:
    text = _normalize_text(f"{title or ''} {brand or ''}")
    for category, patterns in _CATEGORY_PATTERNS:
        if any(re.search(pattern, text) for pattern in patterns):
            return category
    return "Other"


def backfill_product_categories(
    session: Session,
    *,
    source_competitor_id: str = "toolzone_sk",
) -> CategoryBackfillResult:
    source_by_ean = _load_source_listings_by_ean(session, source_competitor_id)
    products = list(session.scalars(select(Product).order_by(Product.id)))

    updated = 0
    counts: dict[str, int] = {}
    for product in products:
        source = source_by_ean.get(normalise_ean(product.ean))
        category = classify_product_category(
            title=source.title if source is not None else product.title,
            brand=source.brand if source is not None else product.brand,
        )
        counts[category] = counts.get(category, 0) + 1
        if product.category != category:
            product.category = category
            updated += 1

    return CategoryBackfillResult(
        products_seen=len(products),
        products_updated=updated,
        category_counts=counts,
    )


def backfill_competitor_listing_categories(
    session: Session,
) -> ListingCategoryBackfillResult:
    listings = list(session.scalars(select(CompetitorListing).order_by(CompetitorListing.id)))

    updated = 0
    counts: dict[str, int] = {}
    for listing in listings:
        category = classify_product_category(title=listing.title, brand=listing.brand)
        counts[category] = counts.get(category, 0) + 1
        if listing.category != category:
            listing.category = category
            updated += 1

    return ListingCategoryBackfillResult(
        listings_seen=len(listings),
        listings_updated=updated,
        category_counts=counts,
    )


def _load_source_listings_by_ean(
    session: Session,
    source_competitor_id: str,
) -> dict[str, CompetitorListing]:
    listings = session.scalars(
        select(CompetitorListing)
        .where(CompetitorListing.competitor_id == source_competitor_id)
        .order_by(CompetitorListing.scraped_at.desc(), CompetitorListing.id.desc())
    )
    by_ean: dict[str, CompetitorListing] = {}
    for listing in listings:
        ean = normalise_ean(listing.ean)
        if ean and ean not in by_ean:
            by_ean[ean] = listing
    return by_ean


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.lower())
    without_marks = "".join(
        char for char in normalized
        if not unicodedata.combining(char)
    )
    return re.sub(r"\s+", " ", without_marks)
