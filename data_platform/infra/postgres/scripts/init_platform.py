from __future__ import annotations

import logging
import os

import psycopg2
from psycopg2 import sql

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PG_HOST  = os.environ.get("PG_HOST", "localhost")
PG_PORT  = os.environ.get("PG_PORT", "5432")
PG_USER  = os.environ.get("PG_USER", "postgres")
PG_PASS  = os.environ.get("PG_PASS", "changeme")

DATABASES = ["nessie"]


def _connect(dbname: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=dbname, user=PG_USER, password=PG_PASS,
    )
    conn.autocommit = True
    return conn


def ensure_databases() -> None:
    with _connect("postgres") as conn, conn.cursor() as cur:
        for db in DATABASES:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
            if cur.fetchone() is None:
                logger.info("Creating database: %s", db)
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
            else:
                logger.info("Database already exists: %s", db)


def main() -> None:
    try:
        ensure_databases()
        logger.info("Platform databases ready: %s", DATABASES)
    except Exception:
        logger.exception("Initialization failed.")
        raise


if __name__ == "__main__":
    main()