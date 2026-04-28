from __future__ import annotations

import ast
from pathlib import Path

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker


def _load_matching_review_helpers():
    app_path = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
    source = app_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(app_path))

    wanted = {
        "_approve_pending_members",
        "_auto_approve_high_similarity_matches",
        "_selected_matching_review_ids",
        "_store_selected_matching_review_ids",
        "_sync_matching_review_selection",
    }
    selected_nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    module = ast.Module(body=selected_nodes, type_ignores=[])
    namespace: dict[str, object] = {
        "bindparam": bindparam,
        "text": text,
        "SQLAlchemyError": SQLAlchemyError,
        "_MATCH_REVIEW_AUTO_APPROVE_SIMILARITY": 0.96,
        "_MATCH_REVIEW_SELECTION_KEY": "mr_selected_member_ids",
    }
    exec(compile(module, str(app_path), "exec"), namespace)
    return namespace


class _Clearable:
    def __init__(self) -> None:
        self.calls = 0

    def clear(self) -> None:
        self.calls += 1


class _FakeStreamlit:
    session_state: dict[str, object] = {}


def _make_product_matches_db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE product_matches (
                    id INTEGER PRIMARY KEY,
                    listing_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    match_type TEXT NOT NULL,
                    confidence NUMERIC,
                    similarity NUMERIC,
                    llm_confidence NUMERIC,
                    status TEXT NOT NULL,
                    reviewed_at TEXT,
                    reviewer TEXT
                )
                """
            )
        )
    return engine


def test_approve_pending_members_updates_only_selected_pending_rows() -> None:
    helpers = _load_matching_review_helpers()
    engine = _make_product_matches_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO product_matches
                    (id, listing_id, product_id, match_type, confidence, status, reviewed_at, reviewer)
                VALUES
                    (1, 1, 10, 'vector_llm', 0.88, 'pending', NULL, NULL),
                    (2, 2, 10, 'vector_llm', 0.88, 'pending', NULL, NULL),
                    (3, 3, 11, 'vector_llm', 0.92, 'approved', NULL, NULL),
                    (4, 4, 12, 'vector_llm', 0.85, 'pending', NULL, NULL)
                """
            )
        )

    Session = sessionmaker(bind=engine)

    def _session():
        return Session()

    clearable = _Clearable()
    helpers["_session"] = _session
    helpers["_load_price_compare_data"] = clearable

    updated = helpers["_approve_pending_members"]([1, 2, 3, 2])

    assert updated == 2
    assert clearable.calls == 1

    with engine.connect() as conn:
        rows = {
            row.id: dict(row._mapping)
            for row in conn.execute(
                text("SELECT id, status, reviewed_at, reviewer FROM product_matches ORDER BY id")
            )
        }

    assert rows[1]["status"] == "approved"
    assert rows[1]["reviewer"] == "dashboard"
    assert rows[1]["reviewed_at"] is not None
    assert rows[2]["status"] == "approved"
    assert rows[2]["reviewer"] == "dashboard"
    assert rows[3]["status"] == "approved"
    assert rows[3]["reviewer"] is None
    assert rows[4]["status"] == "pending"


def test_approve_pending_members_noops_empty_selection() -> None:
    helpers = _load_matching_review_helpers()
    clearable = _Clearable()
    helpers["_load_price_compare_data"] = clearable

    updated = helpers["_approve_pending_members"]([])

    assert updated == 0
    assert clearable.calls == 0


def test_sync_matching_review_selection_only_updates_session_state() -> None:
    helpers = _load_matching_review_helpers()
    fake_st = _FakeStreamlit()
    fake_st.session_state = {
        "mr_selected_member_ids": [3],
        "mr_select_7": True,
    }
    helpers["st"] = fake_st

    helpers["_sync_matching_review_selection"](7)

    assert fake_st.session_state["mr_selected_member_ids"] == [3, 7]

    fake_st.session_state["mr_select_3"] = False
    helpers["_sync_matching_review_selection"](3)

    assert fake_st.session_state["mr_selected_member_ids"] == [7]


def test_auto_approve_high_similarity_matches_approves_pending_rows_only() -> None:
    helpers = _load_matching_review_helpers()
    engine = _make_product_matches_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO product_matches
                    (id, listing_id, product_id, match_type, similarity, llm_confidence,
                     confidence, status, reviewed_at, reviewer)
                VALUES
                    (1, 1, 10, 'vector_llm', 0.960, 0.10, 0.10, 'pending',  NULL, NULL),
                    (2, 2, 10, 'vector_llm', 0.959, 0.99, 0.99, 'pending',  NULL, NULL),
                    (3, 3, 11, 'vector_llm', 0.990, NULL, 0.90, 'rejected', NULL, 'human'),
                    (4, 4, 12, 'exact_ean',  1.000, NULL, 1.00, 'pending',  NULL, NULL)
                """
            )
        )

    Session = sessionmaker(bind=engine)

    def _session():
        return Session()

    clearable = _Clearable()
    helpers["_session"] = _session
    helpers["_load_price_compare_data"] = clearable

    updated = helpers["_auto_approve_high_similarity_matches"]()

    assert updated == 1
    assert clearable.calls == 1
    with engine.connect() as conn:
        rows = {
            row.id: dict(row._mapping)
            for row in conn.execute(
                text("SELECT id, status, reviewer, reviewed_at FROM product_matches ORDER BY id")
            )
        }

    assert rows[1]["status"] == "approved"
    assert rows[1]["reviewer"] == "auto_similarity_0.96"
    assert rows[1]["reviewed_at"] is not None
    assert rows[2]["status"] == "pending"
    assert rows[3]["status"] == "rejected"
    assert rows[3]["reviewer"] == "human"
    assert rows[4]["status"] == "pending"


def test_auto_approve_high_similarity_matches_handles_database_lock() -> None:
    helpers = _load_matching_review_helpers()

    class LockedSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            raise OperationalError("UPDATE product_matches", {}, Exception("database is locked"))

    helpers["_session"] = lambda: LockedSession()
    clearable = _Clearable()
    helpers["_load_price_compare_data"] = clearable

    updated = helpers["_auto_approve_high_similarity_matches"]()

    assert updated == 0
    assert clearable.calls == 0
