"""Add covering index for competitor listing lookups.

The Price Compare query joins product_matches → competitor_listings on
(competitor_id, competitor_sku) and then picks the most recent row via
MAX(scraped_at).  Without an index that covers all three columns the query
planner falls back to a full table scan on competitor_listings for every
matched pair, which is the primary cause of slow dashboard loads.

Revision ID: 0002_add_listing_lookup_index
Revises: 0001_initial_schema
Create Date: 2026-04-14
"""

from alembic import op

revision = "0002_add_listing_lookup_index"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "idx_cl_cid_csku_scraped",
        "competitor_listings",
        ["competitor_id", "competitor_sku", "scraped_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_cl_cid_csku_scraped", table_name="competitor_listings")
