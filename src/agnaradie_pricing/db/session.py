"""Database session factory."""

from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from agnaradie_pricing.settings import Settings


def make_engine(settings: Settings):
    return create_engine(settings.database_url, pool_pre_ping=True)


def make_session_factory(settings: Settings) -> sessionmaker[Session]:
    return sessionmaker(bind=make_engine(settings), expire_on_commit=False)


def session_scope(factory: sessionmaker[Session]) -> Iterator[Session]:
    with factory() as session:
        yield session

