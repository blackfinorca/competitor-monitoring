"""Add box_price_eur column to allegro_offers.

Revision ID: 0004_allegro_offers_add_box_price
Revises: 0003_add_allegro_offers
Create Date: 2026-04-20
"""

from alembic import op
import sqlalchemy as sa

revision = "0004_allegro_offers_add_box_price"
down_revision = "0003_add_allegro_offers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("allegro_offers", sa.Column("box_price_eur", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("allegro_offers", "box_price_eur")
