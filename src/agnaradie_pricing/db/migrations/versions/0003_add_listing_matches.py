"""Add listing_matches table and uq_cl_competitor_url constraint.

Revision ID: 0003_add_listing_matches
Revises: 0002_add_listing_lookup_index
Create Date: 2026-04-17
"""

from alembic import op
import sqlalchemy as sa

revision = "0003_add_listing_matches"
down_revision = "0002_add_listing_lookup_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "listing_matches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "toolzone_listing_id",
            sa.Integer(),
            sa.ForeignKey("competitor_listings.id"),
            nullable=False,
        ),
        sa.Column(
            "competitor_listing_id",
            sa.Integer(),
            sa.ForeignKey("competitor_listings.id"),
            nullable=False,
        ),
        sa.Column("match_type", sa.String(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("matched_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "toolzone_listing_id", "competitor_listing_id", name="uq_lm_pair"
        ),
    )
    op.create_unique_constraint(
        "uq_cl_competitor_url", "competitor_listings", ["competitor_id", "url"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_cl_competitor_url", "competitor_listings", type_="unique")
    op.drop_table("listing_matches")
