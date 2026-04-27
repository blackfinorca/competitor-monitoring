"""Add product_clusters and cluster_members tables.

EAN-led cross-store clustering: every competitor_listings row joins at most one
ProductCluster, identified by EAN when known or built by fuzzy vector+LLM match
when not. Replaces ToolZone-anchored matching for the Price Compare view.

Revision ID: 0005_add_product_clusters
Revises: 0004_allegro_offers_add_box_price
Create Date: 2026-04-26
"""

from alembic import op
import sqlalchemy as sa

revision = "0005_add_product_clusters"
down_revision = "0004_allegro_offers_add_box_price"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "product_clusters",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ean", sa.Text(), nullable=True, unique=True),
        sa.Column("cluster_method", sa.Text(), nullable=False),
        sa.Column("representative_brand", sa.Text(), nullable=True),
        sa.Column("representative_title", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("idx_pc_ean", "product_clusters", ["ean"])

    op.create_table(
        "cluster_members",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "cluster_id",
            sa.Integer(),
            sa.ForeignKey("product_clusters.id"),
            nullable=False,
        ),
        sa.Column(
            "listing_id",
            sa.Integer(),
            sa.ForeignKey("competitor_listings.id"),
            nullable=False,
        ),
        sa.Column("match_method", sa.Text(), nullable=False),
        sa.Column("similarity", sa.Numeric(4, 3), nullable=True),
        sa.Column("llm_confidence", sa.Numeric(3, 2), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="approved"),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("cluster_id", "listing_id", name="uq_cm_cluster_listing"),
        sa.UniqueConstraint("listing_id", name="uq_cm_listing"),
    )
    op.create_index("idx_cm_cluster", "cluster_members", ["cluster_id"])
    op.create_index("idx_cm_status", "cluster_members", ["status"])


def downgrade() -> None:
    op.drop_index("idx_cm_status", table_name="cluster_members")
    op.drop_index("idx_cm_cluster", table_name="cluster_members")
    op.drop_table("cluster_members")
    op.drop_index("idx_pc_ean", table_name="product_clusters")
    op.drop_table("product_clusters")
