
import json
import csv
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "bronze" / "ipl"
REPORT_DIR = PROJECT_ROOT / "reports"

REPORT_PATH = REPORT_DIR / "schema_validation_report.csv"

REQUIRED_TOP_LEVEL = {"info", "innings"}
REQUIRED_INFO = {"teams", "dates", "venue"}
REQUIRED_INNING = {"team", "overs"}
REQUIRED_DELIVERY = {"batter", "bowler", "non_striker", "runs"}
REQUIRED_RUNS = {"batter", "extras", "total"}


def validate_match(data):
    errors = []

    if not isinstance(data, dict):
        return ["Root JSON value must be an object"]

    missing = REQUIRED_TOP_LEVEL - data.keys()
    if missing:
        errors.append(f"Missing top-level fields: {', '.join(sorted(missing))}")
        return errors

    info = data["info"]
    innings = data["innings"]

    if not isinstance(info, dict):
        errors.append("'info' must be an object")
    else:
        missing_info = REQUIRED_INFO - info.keys()
        if missing_info:
            errors.append(
                f"Missing info fields: {', '.join(sorted(missing_info))}"
            )

        teams = info.get("teams")
        if not isinstance(teams, list) or len(teams) < 2:
            errors.append("'info.teams' must contain at least two teams")

        dates = info.get("dates")
        if not isinstance(dates, list) or not dates:
            errors.append("'info.dates' must be a non-empty list")

    if not isinstance(innings, list) or not innings:
        errors.append("'innings' must be a non-empty list")
        return errors

    for inning_index, inning in enumerate(innings, start=1):
        if not isinstance(inning, dict):
            errors.append(f"Innings {inning_index}: must be an object")
            continue

        missing_inning = REQUIRED_INNING - inning.keys()
        if missing_inning:
            errors.append(
                f"Innings {inning_index}: missing fields "
                f"{', '.join(sorted(missing_inning))}"
            )
            continue

        overs = inning.get("overs")
        if not isinstance(overs, list) or not overs:
            errors.append(f"Innings {inning_index}: overs must be a non-empty list")
            continue

        for over_index, over in enumerate(overs, start=1):
            if not isinstance(over, dict) or not isinstance(
                over.get("deliveries"), list
            ):
                errors.append(
                    f"Innings {inning_index}, over {over_index}: "
                    "deliveries must be a list"
                )
                continue

            for delivery_index, delivery in enumerate(
                over["deliveries"], start=1
            ):
                location = (
                    f"Innings {inning_index}, over {over_index}, "
                    f"delivery {delivery_index}"
                )

                if not isinstance(delivery, dict):
                    errors.append(f"{location}: delivery must be an object")
                    continue

                missing_delivery = REQUIRED_DELIVERY - delivery.keys()
                if missing_delivery:
                    errors.append(
                        f"{location}: missing fields "
                        f"{', '.join(sorted(missing_delivery))}"
                    )
                    continue

                runs = delivery.get("runs")
                if not isinstance(runs, dict):
                    errors.append(f"{location}: runs must be an object")
                    continue

                missing_runs = REQUIRED_RUNS - runs.keys()
                if missing_runs:
                    errors.append(
                        f"{location}: runs missing fields "
                        f"{', '.join(sorted(missing_runs))}"
                    )
                    continue

                for field in ("batter", "extras", "total"):
                    value = runs.get(field)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        errors.append(
                            f"{location}: runs.{field} must be a non-negative integer"
                        )

    return errors


def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw data folder not found: {RAW_DIR}")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(RAW_DIR.glob("*.json"))

    if not files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    validated_at = datetime.now(timezone.utc).isoformat()
    results = []

    for path in files:
        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            errors = validate_match(data)
            results.append({
                "file_name": path.name,
                "status": "VALID" if not errors else "INVALID",
                "error_count": len(errors),
                "errors": " | ".join(errors),
                "validated_at_utc": validated_at,
            })

        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            results.append({
                "file_name": path.name,
                "status": "ERROR",
                "error_count": 1,
                "errors": str(exc),
                "validated_at_utc": validated_at,
            })

    columns = [
        "file_name", "status", "error_count",
        "errors", "validated_at_utc"
    ]

    with REPORT_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(results)

    valid = sum(row["status"] == "VALID" for row in results)
    invalid = sum(row["status"] == "INVALID" for row in results)
    failed = sum(row["status"] == "ERROR" for row in results)

    print("\n===== SPORTSPULSE SCHEMA VALIDATION =====")
    print(f"Files checked: {len(results)}")
    print(f"Valid files: {valid}")
    print(f"Invalid schema: {invalid}")
    print(f"Unreadable/invalid JSON: {failed}")
    print(f"Report: {REPORT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()