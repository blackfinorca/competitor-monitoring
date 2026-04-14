#!/usr/bin/env bash
set -euo pipefail

echo "Create a Postgres 16 database named agnaradie and enable pgvector:"
echo "  createdb agnaradie"
echo "  psql agnaradie -c 'CREATE EXTENSION IF NOT EXISTS vector;'"
echo "Then run:"
echo "  alembic upgrade head"

