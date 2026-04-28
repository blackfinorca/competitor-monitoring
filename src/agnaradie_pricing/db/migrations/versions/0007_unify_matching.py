"""Unify matching: drop cluster tables, rework product_matches, add source to products.

Replaces listing_matches, product_clusters and cluster_members with a single
product_matches table where every competitor listing maps directly to a product.

Revision ID: 0007_unify_matching
Revises: 0006_add_competitor_listing_category
Create Date: 2026-04-28
"""

from alembic import op
import sqlalchemy as sa

revision = "0007_unify_matching"
down_revision = "0006_add_competitor_listing_category"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Add source column to products (guard: may already exist)
    # ------------------------------------------------------------------
    bind = op.get_bind()
    existing_cols = {row[1] for row in bind.execute(sa.text("PRAGMA table_info(products)"))}
    if "source" not in existing_cols:
        op.add_column("products", sa.Column("source", sa.Text(), nullable=True))
    op.execute("UPDATE products SET source = 'toolzone' WHERE source IS NULL")

    # sku was NOT NULL in original schema — relax for derived products (SQLite ignores nullable changes)
    # alter_column is a no-op on SQLite for nullability, safe to leave out

    # ------------------------------------------------------------------
    # 2. Drop old match / cluster tables
    # ------------------------------------------------------------------
    for idx in ("idx_lm_toolzone", "idx_lm_competitor"):
        try:
            op.drop_index(idx, table_name="listing_matches")
        except Exception:
            pass
    try:
        op.drop_table("listing_matches")
    except Exception:
        pass

    for idx in ("idx_cm_status", "idx_cm_cluster"):
        try:
            op.drop_index(idx, table_name="cluster_members")
        except Exception:
            pass
    try:
        op.drop_table("cluster_members")
    except Exception:
        pass

    try:
        op.drop_index("idx_pc_ean", table_name="product_clusters")
    except Exception:
        pass
    try:
        op.drop_table("product_clusters")
    except Exception:
        pass

    # ------------------------------------------------------------------
    # 3. Rebuild product_matches
    # ------------------------------------------------------------------
    try:
        op.drop_index("idx_pm_product", table_name="product_matches")
    except Exception:
        pass
    try:
        op.drop_index("idx_pm_status", table_name="product_matches")
    except Exception:
        pass
    op.drop_table("product_matches")

    op.create_table(
        "product_matches",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "listing_id",
            sa.Integer(),
            sa.ForeignKey("competitor_listings.id"),
            nullable=False,
        ),
        sa.Column(
            "product_id",
            sa.Integer(),
            sa.ForeignKey("products.id"),
            nullable=False,
        ),
        sa.Column("match_type", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
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
        sa.UniqueConstraint("listing_id", name="uq_product_match_listing"),
    )
    op.create_index("idx_pm_product", "product_matches", ["product_id"])
    op.create_index("idx_pm_status", "product_matches", ["status"])


def downgrade() -> None:
    op.drop_index("idx_pm_status", table_name="product_matches")
    op.drop_index("idx_pm_product", table_name="product_matches")
    op.drop_table("product_matches")

    op.create_table(
        "product_matches",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("ag_product_id", sa.Integer(), sa.ForeignKey("products.id"), nullable=True),
        sa.Column("competitor_id", sa.Text(), nullable=False),
        sa.Column("competitor_sku", sa.Text(), nullable=True),
        sa.Column("match_type", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("verified_by_human", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("ag_product_id", "competitor_id", "competitor_sku",
                            name="uq_product_matches_product_competitor_sku"),
    )

    op.drop_column("products", "source")
    # SQLite does not support ALTER COLUMN nullability — skip sku NOT NULL restore
