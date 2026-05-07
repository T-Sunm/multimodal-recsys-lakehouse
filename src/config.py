import os

TRINO_HOST    = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER    = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA  = "recsys_silver"
