"""Add allegro_offers table for EAN-based competitor price tracking.

Revision ID: 0003_add_allegro_offers
Revises: 0002_add_listing_lookup_index
Create Date: 2026-04-18
"""

from alembic import op
import sqlalchemy as sa

revision = "0003_add_allegro_offers"
down_revision = "0002_add_listing_lookup_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "allegro_offers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("ean", sa.String(), nullable=False),
        sa.Column("title", sa.String()),
        sa.Column("seller", sa.String(), nullable=False),
        sa.Column("seller_url", sa.String()),
        sa.Column("price_eur", sa.Float()),
        sa.Column("delivery_eur", sa.Float()),
        sa.Column("scraped_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ean", "seller", name="uq_allegro_offer_ean_seller"),
    )
    op.create_index("ix_allegro_offers_ean", "allegro_offers", ["ean"])


def downgrade() -> None:
    op.drop_index("ix_allegro_offers_ean", "allegro_offers")
    op.drop_table("allegro_offers")
