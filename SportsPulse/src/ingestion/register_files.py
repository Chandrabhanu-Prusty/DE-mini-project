
import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "bronze" / "ipl"
MANIFEST_PATH = PROJECT_ROOT / "reports" / "file_manifest.csv"


def calculate_sha256(file_path: Path) -> str:
    """Calculate the SHA-256 checksum without loading the whole file."""
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw data folder not found: {RAW_DIR}")

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    json_files = sorted(RAW_DIR.glob("*.json"))

    if not json_files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    registered_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for file_path in json_files:
        try:
            rows.append({
                "file_name": file_path.name,
                "relative_path": file_path.relative_to(PROJECT_ROOT).as_posix(),
                "size_bytes": file_path.stat().st_size,
                "sha256": calculate_sha256(file_path),
                "status": "REGISTERED",
                "registered_at_utc": registered_at,
                "error": "",
            })
        except OSError as exc:
            rows.append({
                "file_name": file_path.name,
                "relative_path": file_path.relative_to(PROJECT_ROOT).as_posix(),
                "size_bytes": "",
                "sha256": "",
                "status": "ERROR",
                "registered_at_utc": registered_at,
                "error": str(exc),
            })

    columns = [
        "file_name",
        "relative_path",
        "size_bytes",
        "sha256",
        "status",
        "registered_at_utc",
        "error",
    ]

    with MANIFEST_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    successful = sum(row["status"] == "REGISTERED" for row in rows)
    failed = len(rows) - successful

    print(f"JSON files discovered: {len(json_files)}")
    print(f"Successfully registered: {successful}")
    print(f"Registration errors: {failed}")
    print(f"Manifest saved to: {MANIFEST_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
