import logging
import boto3
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY     = "minioadmin"
SECRET_KEY     = "minioadmin"
BUCKET         = "datalake"
PREFIX         = "recsys/bronze"

LOCAL_DATA_DIR = Path(__file__).parent.parent / "microlens-5k"

FILES = [
    "pairs.csv",
    "titles.csv",
    "likes_and_views.txt",
    "visual_embeddings.parquet",
]

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)


def seed_raw_data() -> None:
    if not LOCAL_DATA_DIR.exists():
        log.error("Source directory not found: %s", LOCAL_DATA_DIR)
        return

    uploaded, skipped = 0, 0

    for filename in FILES:
        local_path = LOCAL_DATA_DIR / filename
        s3_key = f"{PREFIX}/{filename}"

        if not local_path.exists():
            log.warning("File not found, skipping: %s", local_path)
            skipped += 1
            continue

        s3.upload_file(str(local_path), BUCKET, s3_key)
        log.info("Uploaded %s -> s3://%s/%s", filename, BUCKET, s3_key)
        uploaded += 1

    log.info(
        "Seeding complete. uploaded=%d skipped=%d total=%d",
        uploaded, skipped, len(FILES),
    )


if __name__ == "__main__":
    seed_raw_data()