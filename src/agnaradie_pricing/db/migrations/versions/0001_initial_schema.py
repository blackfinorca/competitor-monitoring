"""Initial schema.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-04-12
"""

from alembic import op
import sqlalchemy as sa

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("sku", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=True),
        sa.Column("mpn", sa.Text(), nullable=True),
        sa.Column("ean", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("price_eur", sa.Numeric(10, 2), nullable=True),
        sa.Column("cost_eur", sa.Numeric(10, 2), nullable=True),
        sa.Column("stock", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sku"),
    )
    op.create_index("idx_products_brand_mpn", "products", ["brand", "mpn"])
    op.create_index("idx_products_ean", "products", ["ean"])

    op.create_table(
        "competitor_listings",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("competitor_id", sa.Text(), nullable=False),
        sa.Column("competitor_sku", sa.Text(), nullable=True),
        sa.Column("brand", sa.Text(), nullable=True),
        sa.Column("mpn", sa.Text(), nullable=True),
        sa.Column("ean", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("price_eur", sa.Numeric(10, 2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("in_stock", sa.Boolean(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_cl_competitor_scraped", "competitor_listings", ["competitor_id", "scraped_at"])
    op.create_index("idx_cl_brand_mpn", "competitor_listings", ["brand", "mpn"])

    op.create_table(
        "product_matches",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("ag_product_id", sa.Integer(), nullable=True),
        sa.Column("competitor_id", sa.Text(), nullable=False),
        sa.Column("competitor_sku", sa.Text(), nullable=True),
        sa.Column("match_type", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("verified_by_human", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["ag_product_id"], ["products.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ag_product_id", "competitor_id", "competitor_sku", name="uq_product_matches_product_competitor_sku"),
    )

    op.create_table(
        "pricing_snapshot",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("ag_product_id", sa.Integer(), nullable=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("ag_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("competitor_count", sa.Integer(), nullable=True),
        sa.Column("min_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("median_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("max_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("ag_rank", sa.Integer(), nullable=True),
        sa.Column("cheapest_competitor", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["ag_product_id"], ["products.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ag_product_id", "snapshot_date", name="uq_pricing_snapshot_product_date"),
    )

    op.create_table(
        "recommendations",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("ag_product_id", sa.Integer(), nullable=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("playbook", sa.Text(), nullable=False),
        sa.Column("current_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("suggested_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("rationale", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["ag_product_id"], ["products.id"]),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("recommendations")
    op.drop_table("pricing_snapshot")
    op.drop_table("product_matches")
    op.drop_index("idx_cl_brand_mpn", table_name="competitor_listings")
    op.drop_index("idx_cl_competitor_scraped", table_name="competitor_listings")
    op.drop_table("competitor_listings")
    op.drop_index("idx_products_ean", table_name="products")
    op.drop_index("idx_products_brand_mpn", table_name="products")
    op.drop_table("products")

