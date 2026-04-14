"""SQLAlchemy models for the pricing agent."""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        Index("idx_products_brand_mpn", "brand", "mpn"),
        Index("idx_products_ean", "ean"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    brand: Mapped[str | None] = mapped_column(Text)
    mpn: Mapped[str | None] = mapped_column(Text)
    ean: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(Text)
    price_eur: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    cost_eur: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    stock: Mapped[int | None] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class CompetitorListing(Base):
    __tablename__ = "competitor_listings"
    __table_args__ = (
        Index("idx_cl_competitor_scraped", "competitor_id", "scraped_at"),
        Index("idx_cl_brand_mpn", "brand", "mpn"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True
    )
    competitor_id: Mapped[str] = mapped_column(Text, nullable=False)
    competitor_sku: Mapped[str | None] = mapped_column(Text)
    brand: Mapped[str | None] = mapped_column(Text)
    mpn: Mapped[str | None] = mapped_column(Text)
    ean: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    price_eur: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="EUR", nullable=False)
    in_stock: Mapped[bool | None] = mapped_column(Boolean)
    url: Mapped[str | None] = mapped_column(Text)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ProductMatch(Base):
    __tablename__ = "product_matches"
    __table_args__ = (
        UniqueConstraint(
            "ag_product_id",
            "competitor_id",
            "competitor_sku",
            name="uq_product_matches_product_competitor_sku",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True
    )
    ag_product_id: Mapped[int | None] = mapped_column(ForeignKey("products.id"))
    competitor_id: Mapped[str] = mapped_column(Text, nullable=False)
    competitor_sku: Mapped[str | None] = mapped_column(Text)
    match_type: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    verified_by_human: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class PricingSnapshot(Base):
    __tablename__ = "pricing_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "ag_product_id", "snapshot_date", name="uq_pricing_snapshot_product_date"
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True
    )
    ag_product_id: Mapped[int | None] = mapped_column(ForeignKey("products.id"))
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    ag_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    competitor_count: Mapped[int | None] = mapped_column(Integer)
    min_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    median_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    max_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    ag_rank: Mapped[int | None] = mapped_column(Integer)
    cheapest_competitor: Mapped[str | None] = mapped_column(Text)


class Recommendation(Base):
    __tablename__ = "recommendations"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True
    )
    ag_product_id: Mapped[int | None] = mapped_column(ForeignKey("products.id"))
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    playbook: Mapped[str] = mapped_column(Text, nullable=False)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    suggested_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    rationale: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="pending", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reviewer: Mapped[str | None] = mapped_column(Text)
