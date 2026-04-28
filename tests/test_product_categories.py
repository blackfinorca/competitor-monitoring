from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing.catalogue.categories import (
    classify_product_category,
    backfill_product_categories,
    backfill_competitor_listing_categories,
)
from agnaradie_pricing.db.models import Base, CompetitorListing, Product


def test_classify_product_category_uses_simple_mece_names() -> None:
    examples = {
        "Špirálový vrták HSS-E DIN338 FORMAT": "Machining tools",
        "Upínacie puzdro pre predĺženie skľučovadla": "Tool holders",
        "Brúsny kotúč CERABOND X 125 mm": "Abrasives",
        "Skrutkovač Torx T20 SwissGrip": "Screwdrivers & keys",
        "Nástrčkový kľúč s račňou 1/2": "Wrenches & sockets",
        "Kliešte SIKO Cobra 250mm KNIPEX": "Pliers & cutters",
        "Vodováha RED 180 digital Sola": "Measurement tools",
        "Zverák dielenský 125 mm": "Clamps",
        "Káblový navijak XBoard Runpotec": "Electrical tools",
        "Ochranné okuliare číre": "Safety gear",
        "Pneumatický rázový uťahovák 1/2": "Wrenches & sockets",
        "Rychlospojka vzduchové přípojky 12,91 mm 1/4": "Wrenches & sockets",
        "FEIN Dierovka z tvrdého kovu s upnutím QuickIN PLUS": "Machining tools",
        "Otočná rezná platnička KX": "Machining tools",
        "VBD výmenná břitová destička 7° pozitivní": "Machining tools",
        "Metabo BE 850-2 vŕtačka 600573810": "Power tools",
        "Pílový list E-Cut Long-Life": "Cutting hand tools",
        "Pílový kotúč Eco for wood": "Cutting hand tools",
        "KNIPEX Cobra fosfátováno 250 mm": "Pliers & cutters",
        "KNIPEX Alligator fosfátováno 300 mm": "Pliers & cutters",
        "Plastové příchytky čalounění, 10 ks": "Fasteners",
        "Závitový čap M10": "Fasteners",
        "Kartáč okružný so stopkou vlnitý": "Abrasives",
        "Aku multifunkčná oscilačná brúska": "Power tools",
        "Zamečnický úhelník vč. dorazu": "Measurement tools",
        "Závitový medzný kalibračný krúžok": "Measurement tools",
        "Dělicí přípravek KUKKO": "Clamps",
        "Polák Pracovný stôl KOMBI": "Tool storage",
        "Ochranný kryt pre deliace práce": "Spare parts",
        "Programovacia objímka": "Tool holders",
        "Ohrievač plynový 30kW": "Other",
        "Pištoľ tavná EG Pen 7mm RAPID": "Other",
        "Sada náradia M18 FUEL": "Other",
        "Šestihranný hluboký nástavec CrMo 3/4": "Wrenches & sockets",
        "FEIN E-Cut Carbide Pro": "Cutting hand tools",
        "FEIN Matrica": "Spare parts",
        "FEIN Zástrčka ochrany motora": "Other",
        "Klešťový klíč chromované 250 mm": "Pliers & cutters",
        "FEIN 3M Cubitron II": "Abrasives",
        "FEIN Kotúč D150x6": "Abrasives",
        "FEIN Vodiaci valček": "Spare parts",
        "Markal Paint-Riter značkovač": "Measurement tools",
        "Multifunkční nůž 9 funkcí": "Cutting hand tools",
        "Pistole na kartuše 225 mm": "Other",
        "Pištoľ na PU penu": "Other",
        "Brašna na nářadí": "Tool storage",
        "Unášací tanier so suchým zipsom pre uhlové brúsky": "Abrasives",
        "FEIN Adaptéry QuickIN": "Tool holders",
        "FEIN Koleno": "Spare parts",
        "FEIN Kus reťazca": "Spare parts",
        "FEIN Objímka spojky": "Spare parts",
        "FEIN Predĺženie": "Tool holders",
        "FEIN Protiprachový filter": "Spare parts",
        "FEIN Rukoväť": "Spare parts",
        "FEIN Rúnový pás": "Abrasives",
        "FEIN Upínacia jednotka": "Clamps",
        "KNIPEX TwinForce chromované 180 mm": "Pliers & cutters",
        "KNIPEX X-Cut chromované 160 mm": "Pliers & cutters",
        "M18 dávkovacia pištoľ": "Other",
        "Miešadlo 120 mm": "Power tools",
        "Mikrometer na zabudovanie 0-25 mm": "Measurement tools",
        "Otočná základna pro svěrák": "Clamps",
        "Pinzeta presné provedení": "Pliers & cutters",
        "Ploché sekáče SDS max": "Striking tools",
        "Predlžovací nadstavec výkyvný 1/4": "Wrenches & sockets",
        "Sada dlát 4ks": "Striking tools",
        "Sešívačka čalounická 10,6 mm": "Fasteners",
        "Trubkový nástrčný klíč DIN896B": "Wrenches & sockets",
        "Rýchlovýmenná vložka ES 2": "Tool holders",
        "Sada pílových plátkov": "Cutting hand tools",
        "Vložka na náradie XL-BOXX": "Tool storage",
        "Uťahovací čap JISB6339": "Tool holders",
        "Svěrací pouzdro DIN6328": "Tool holders",
        "Středicí děrovač": "Striking tools",
        "Redukční zdířka 32-25mm": "Tool holders",
        "Redukční pouzdro DIN69893A": "Tool holders",
        "Rezačka polystyrénu": "Cutting hand tools",
        "Viazacia páska prírodná": "Fasteners",
        "Magnet plochého drapáka": "Clamps",
        "Sada paralelných podložiek": "Tool holders",
        "Jamkovač 140 x 14 mm": "Striking tools",
        "Dělicí kotouč Diamant": "Abrasives",
        "Sada nožov Worcraft": "Cutting hand tools",
        "Důlčík 8-hran hrot tvrdokov": "Striking tools",
        "Šroubovák křížový PH 0": "Screwdrivers & keys",
        "Schránka NESTOR": "Tool storage",
        "Čistiaci prostriedok na podlahy": "Chemicals",
        "Police pro stěnu z perforovaných panelů": "Tool storage",
        "Rúno univerzálne použitie, kotúč": "Abrasives",
        "Rydlo polokulaté tvrdokov": "Cutting hand tools",
        "Obkročné dutinové kružidlo": "Measurement tools",
        "Univerzálna ostrička nožov a vrtákov": "Other",
        "Akumulátorové hoblíky GHO 12V-20": "Power tools",
        "M18 FUEL uhlová klincovačka": "Power tools",
        "Leštička 1200 W": "Power tools",
        "Kultivátor Worcraft SF7G602": "Garden tools",
        "Aku kosačka na trávu": "Garden tools",
        "Hrable vejárové kovové": "Garden tools",
        "Postrekovač KingJet": "Garden tools",
        "Batohový vibrátor do betónu": "Construction tools",
        "Škrabák na sadrokartón": "Construction tools",
        "Hladítko pena gumová": "Construction tools",
        "Rebrík hliníkový ALVE": "Household & hardware",
        "Visiaci zámok chróm": "Household & hardware",
        "Ravak Umývadlová stojanková batéria": "Household & hardware",
        "Plachta zakrývacia PE standard": "Household & hardware",
        "Košík Curver STYLE2": "Household & hardware",
        "Oboustranný vidlicový klíč DIN3110 17x19mm FORMAT": "Wrenches & sockets",
        "Sada kombinovaných očkových a vidlicových klíčů DIN3113A": "Wrenches & sockets",
        "Kliešťové kľúče 180 mm": "Pliers & cutters",
        "Vrtáky do kladív SDS plus-7X": "Machining tools",
        "Balenie vrtákov HSS Impact Control": "Machining tools",
        "NC navrtávak DIN1835 HSSCo5": "Machining tools",
        "Kuželový záhlubník DIN335 HSS": "Machining tools",
        "Kleština DIN6499B ER11": "Tool holders",
        "Závitové rychlovýměnné sklíčidlo": "Tool holders",
        "Pílové kotúče tvar 3": "Cutting hand tools",
        "Sada pílových vencov": "Cutting hand tools",
        "Ponorný list 20x40 mm HM": "Cutting hand tools",
        "Diamantový rezací kotúč ECO": "Abrasives",
        "Plstené teliesko ZYA1015": "Abrasives",
        "Bristle Disc M14 115mm": "Abrasives",
        "L-Boxx prázdny Knipex": "Tool storage",
        "Karta na náradie pre L-Boxx": "Tool storage",
        "Regál Racks RAW5T": "Tool storage",
        "Spona s úzkym chrbtom": "Fasteners",
        "Klince do klincovačky": "Fasteners",
        "Hák na náradie pre dierovanú dosku": "Fasteners",
        "Dielenská zkoušečka": "Measurement tools",
        "Šířkoměr a hloubkoměr": "Measurement tools",
        "Závitový medzný kalibračný tŕň": "Measurement tools",
        "Sklopný reflektor Hybrid 500": "Electrical tools",
        "Zástrčka CEE 16A IP44": "Electrical tools",
        "Batéria LONGLIFE VARTA": "Household & hardware",
        "Dekorácia MagicHome Vianoce": "Household & hardware",
        "Bambus štiepaný 1x5m": "Garden tools",
        "Autošampón 5L": "Chemicals",
        "Tesa textilná lepiaca páska": "Chemicals",
        "Papierové filtračné vrecko": "Spare parts",
        "Vysokotlaková dýza pre hadicu": "Spare parts",
        "Extol Premium ERS 450 SCP excentrická brúska": "Power tools",
        "Extol Craft Kartáče so stopkou vlnitý drôt": "Abrasives",
        "Ryhovací tvárniaci nástroj QUICK pre ryhovacie koliesko": "Machining tools",
        "Rádlovací kolečko PM AA se zkosenou hranou QUICK": "Machining tools",
        "List do pásovej píly PROFLEX M42": "Cutting hand tools",
        "Pílový pás 3851-27-0.9-5/8-2720": "Cutting hand tools",
        "Kvalitný PROFI dielenský nábytok": "Tool storage",
        "Vodicí čep vnitřní chlazení": "Tool holders",
        "Vodiaci čap s vnútorným chladením": "Tool holders",
        "DeWalt rázový uťahovač Li-Ion XR 18V": "Wrenches & sockets",
        "Deliaci kotúč rovné na hliník": "Abrasives",
        "Soustružnický nůž tvrdokov DIN4971": "Machining tools",
        "Sústružnícky polotovar HSSE tvar E": "Machining tools",
        "Svěrací držák 95° SCLCR": "Clamps",
        "Viazacie pásky z nylonu": "Fasteners",
        "Pracovné svetlo NOVA 6K": "Electrical tools",
        "Akumulátorové ručné svietidlo": "Electrical tools",
        "Magnet plochého podávača so závitom": "Clamps",
        "Odhrotovač dvojitý": "Machining tools",
        "Súprava hadíc na chladivo": "Spare parts",
    }

    for title, expected in examples.items():
        assert classify_product_category(title=title, brand=None) == expected


def test_backfill_product_categories_prefers_toolzone_title_by_ean() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        session.add_all(
            [
                Product(
                    sku="AG-1",
                    ean="4003773022022",
                    title="Generic imported title",
                    category="Ruční nářadí",
                ),
                Product(
                    sku="AG-2",
                    ean=None,
                    title="Brúsny kotúč 125 mm",
                    category=None,
                ),
                CompetitorListing(
                    competitor_id="toolzone_sk",
                    ean="4003773022022",
                    title="Kliešte SIKO Cobra 250mm KNIPEX",
                    price_eur=10,
                    currency="EUR",
                    url="https://toolzone.test/cobra",
                ),
            ]
        )
        session.commit()

        result = backfill_product_categories(session)
        session.commit()

        products = {
            product.sku: product
            for product in session.scalars(select(Product).order_by(Product.sku))
        }

    assert result.products_seen == 2
    assert result.products_updated == 2
    assert products["AG-1"].category == "Pliers & cutters"
    assert products["AG-2"].category == "Abrasives"


def test_backfill_competitor_listing_categories_updates_all_competitors() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        session.add_all(
            [
                CompetitorListing(
                    competitor_id="ahprofi_sk",
                    ean="4003773022022",
                    title="Skrutkovač Torx T20 SwissGrip",
                    price_eur=10,
                    currency="EUR",
                    url="https://ahprofi.test/screwdriver",
                ),
                CompetitorListing(
                    competitor_id="rebiop_sk",
                    ean="4003773022039",
                    title="Brúsny kotúč CERABOND X 125 mm",
                    price_eur=12,
                    currency="EUR",
                    url="https://rebiop.test/disc",
                ),
            ]
        )
        session.commit()

        result = backfill_competitor_listing_categories(session)
        session.commit()

        rows = {
            listing.competitor_id: listing
            for listing in session.scalars(select(CompetitorListing))
        }

    assert result.listings_seen == 2
    assert result.listings_updated == 2
    assert rows["ahprofi_sk"].category == "Screwdrivers & keys"
    assert rows["rebiop_sk"].category == "Abrasives"
