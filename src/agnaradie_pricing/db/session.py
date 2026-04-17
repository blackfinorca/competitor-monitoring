"""Database session factory."""

from collections.abc import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from agnaradie_pricing.settings import Settings


def make_engine(settings: Settings):
    url = settings.database_url
    if url.startswith("sqlite"):
        engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )

        @event.listens_for(engine, "connect")
        def _set_wal(conn, _record):
            conn.execute("PRAGMA journal_mode=WAL")

        return engine
    return create_engine(url, pool_pre_ping=True)


def make_session_factory(settings: Settings) -> sessionmaker[Session]:
    return sessionmaker(bind=make_engine(settings), expire_on_commit=False)


def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    with factory() as session:
        yield session

