"""Daily catalogue ingestion entrypoint."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pathlib import Path

from agnaradie_pricing.catalogue.ingest import ingest_catalogue_csv
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings


def main(path: Path = Path("data/ag_catalogue.csv")) -> int:
    factory = make_session_factory(Settings())
    with factory() as session:
        count = ingest_catalogue_csv(session, path)
        session.commit()
    return count


if __name__ == "__main__":
    ingested = main()
    print(f"Ingested {ingested} catalogue rows")
