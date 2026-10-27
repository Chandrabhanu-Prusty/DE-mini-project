
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "bronze" / "ipl"
REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_PATH = REPORT_DIR / "data_quality_report.csv"


def inspect_match(path):
    issues = []

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [{
            "file_name": path.name,
            "check": "json_readable",
            "severity": "ERROR",
            "details": str(exc),
        }]

    if not isinstance(data, dict):
        return [{
            "file_name": path.name,
            "check": "root_object",
            "severity": "ERROR",
            "details": "JSON root is not an object",
        }]

    info = data.get("info", {})
    innings = data.get("innings", [])

    if not isinstance(info, dict):
        info = {}
        issues.append({
            "file_name": path.name,
            "check": "info_object",
            "severity": "ERROR",
            "details": "'info' is not an object",
        })

    teams = info.get("teams", [])
    if not isinstance(teams, list) or len(teams) < 2:
        issues.append({
            "file_name": path.name,
            "check": "match_teams",
            "severity": "ERROR",
            "details": "Fewer than two teams listed",
        })
        teams = []

    dates = info.get("dates", [])
    if not isinstance(dates, list) or not dates:
        issues.append({
            "file_name": path.name,
            "check": "match_date",
            "severity": "WARNING",
            "details": "Match date is missing or malformed",
        })

    venue = info.get("venue")
    if not isinstance(venue, str) or not venue.strip():
        issues.append({
            "file_name": path.name,
            "check": "venue",
            "severity": "WARNING",
            "details": "Venue is missing",
        })

    if not isinstance(innings, list) or not innings:
        issues.append({
            "file_name": path.name,
            "check": "innings",
            "severity": "ERROR",
            "details": "No innings list found",
        })
        return issues

    innings_teams = []

    for inning_no, inning in enumerate(innings, start=1):
        if not isinstance(inning, dict):
            issues.append({
                "file_name": path.name,
                "check": "innings_structure",
                "severity": "ERROR",
                "details": f"Innings {inning_no} is not an object",
            })
            continue

        batting_team = inning.get("team")
        if batting_team:
            innings_teams.append(batting_team)

        overs = inning.get("overs", [])
        if not isinstance(overs, list):
            issues.append({
                "file_name": path.name,
                "check": "overs_structure",
                "severity": "ERROR",
                "details": f"Innings {inning_no}: overs is not a list",
            })
            continue

        delivery_count = 0

        for over in overs:
            if not isinstance(over, dict):
                continue

            deliveries = over.get("deliveries", [])
            if not isinstance(deliveries, list):
                continue

            for delivery in deliveries:
                delivery_count += 1

                if not isinstance(delivery, dict):
                    issues.append({
                        "file_name": path.name,
                        "check": "delivery_structure",
                        "severity": "ERROR",
                        "details": f"Innings {inning_no}: malformed delivery",
                    })
                    continue

                runs = delivery.get("runs", {})
                if not isinstance(runs, dict):
                    issues.append({
                        "file_name": path.name,
                        "check": "runs_structure",
                        "severity": "ERROR",
                        "details": f"Innings {inning_no}: runs is not an object",
                    })
                    continue

                for field in ("batter", "extras", "total"):
                    value = runs.get(field)
                    if (
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or value < 0
                    ):
                        issues.append({
                            "file_name": path.name,
                            "check": "run_values",
                            "severity": "ERROR",
                            "details": (
                                f"Innings {inning_no}: invalid runs.{field} "
                                f"value {value!r}"
                            ),
                        })

                if all(
                    isinstance(runs.get(field), int)
                    and not isinstance(runs.get(field), bool)
                    and runs.get(field) >= 0
                    for field in ("batter", "extras", "total")
                ):
                    if runs["total"] != runs["batter"] + runs["extras"]:
                        issues.append({
                            "file_name": path.name,
                            "check": "run_total_consistency",
                            "severity": "ERROR",
                            "details": (
                                f"Innings {inning_no}: total runs do not equal "
                                "batter runs plus extras"
                            ),
                        })

        if delivery_count == 0:
            issues.append({
                "file_name": path.name,
                "check": "delivery_count",
                "severity": "WARNING",
                "details": f"Innings {inning_no} contains no deliveries",
            })

    for team in innings_teams:
        if teams and team not in teams:
            issues.append({
                "file_name": path.name,
                "check": "innings_team_consistency",
                "severity": "ERROR",
                "details": f"Innings batting team '{team}' is not in match teams",
            })

    return issues


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(RAW_DIR.glob("*.json"))

    if not files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    issues = []
    for path in files:
        issues.extend(inspect_match(path))

    # Report repeated file content hashes separately from schema checks.
    import hashlib

    hashes = {}
    for path in files:
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            hashes.setdefault(digest, []).append(path.name)
        except OSError as exc:
            issues.append({
                "file_name": path.name,
                "check": "file_read",
                "severity": "ERROR",
                "details": str(exc),
            })

    for duplicate_names in hashes.values():
        if len(duplicate_names) > 1:
            for name in duplicate_names:
                issues.append({
                    "file_name": name,
                    "check": "duplicate_content",
                    "severity": "WARNING",
                    "details": "Identical content found in: " + ", ".join(duplicate_names),
                })

    checked_at = datetime.now(timezone.utc).isoformat()
    columns = ["file_name", "check", "severity", "details", "checked_at_utc"]

    with REPORT_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()

        for issue in issues:
            issue["checked_at_utc"] = checked_at
            writer.writerow(issue)

    counts = Counter(issue["severity"] for issue in issues)
    affected_files = len({issue["file_name"] for issue in issues})

    print("\n===== SPORTSPULSE DATA QUALITY REPORT =====")
    print(f"Files checked: {len(files)}")
    print(f"Files with reported issues: {affected_files}")
    print(f"Errors: {counts['ERROR']}")
    print(f"Warnings: {counts['WARNING']}")
    print(f"Report: {REPORT_PATH.relative_to(PROJECT_ROOT)}")

    if not issues:
        print("No issues detected by the configured checks.")


if __name__ == "__main__":
    main()
