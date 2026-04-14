"""CSV catalogue ingestion."""

import csv
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import Product


REQUIRED_COLUMNS = {
    "sku",
    "brand",
    "mpn",
    "ean",
    "title",
    "category",
    "price_eur",
    "cost_eur",
    "stock",
}


@dataclass(frozen=True)
class CatalogueRow:
    sku: str
    brand: str | None
    mpn: str | None
    ean: str | None
    title: str
    category: str | None
    price_eur: Decimal | None
    cost_eur: Decimal | None
    stock: int | None


def load_catalogue_csv(path: Path) -> list[CatalogueRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        _validate_columns(reader.fieldnames, path)
        return [_parse_row(row, line_number=index + 2) for index, row in enumerate(reader)]


def ingest_catalogue_csv(session: Session, path: Path) -> int:
    rows = load_catalogue_csv(path)
    existing = {
        product.sku: product
        for product in session.scalars(
            select(Product).where(Product.sku.in_([row.sku for row in rows]))
        )
    }
    for row in rows:
        product = existing.get(row.sku)
        if product is None:
            product = Product(sku=row.sku, title=row.title)
            session.add(product)
        _apply_row(product, row)
    return len(rows)


def _validate_columns(fieldnames: list[str] | None, path: Path) -> None:
    present = set(fieldnames or [])
    missing = REQUIRED_COLUMNS - present
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{path} is missing required columns: {missing_text}")


def _parse_row(row: dict[str, str], line_number: int) -> CatalogueRow:
    sku = _clean(row["sku"])
    title = _clean(row["title"])
    if sku is None:
        raise ValueError(f"CSV line {line_number}: sku is required")
    if title is None:
        raise ValueError(f"CSV line {line_number}: title is required")
    return CatalogueRow(
        sku=sku,
        brand=_clean(row["brand"]),
        mpn=_clean(row["mpn"]),
        ean=_clean(row["ean"]),
        title=title,
        category=_clean(row["category"]),
        price_eur=_parse_decimal(row["price_eur"], line_number, "price_eur"),
        cost_eur=_parse_decimal(row["cost_eur"], line_number, "cost_eur"),
        stock=_parse_int(row["stock"], line_number, "stock"),
    )


def _apply_row(product: Product, row: CatalogueRow) -> None:
    product.brand = row.brand
    product.mpn = row.mpn
    product.ean = row.ean
    product.title = row.title
    product.category = row.category
    product.price_eur = row.price_eur
    product.cost_eur = row.cost_eur
    product.stock = row.stock


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_decimal(
    value: str | None, line_number: int, column: str
) -> Decimal | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    try:
        return Decimal(cleaned)
    except Exception as exc:
        raise ValueError(f"CSV line {line_number}: invalid {column}") from exc


def _parse_int(value: str | None, line_number: int, column: str) -> int | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError as exc:
        raise ValueError(f"CSV line {line_number}: invalid {column}") from exc

