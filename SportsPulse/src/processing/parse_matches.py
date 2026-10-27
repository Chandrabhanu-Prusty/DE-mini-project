
import json
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "bronze" / "ipl"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def safe_get(mapping, *keys, default=None):
    """Safely access nested dictionaries."""
    current = mapping

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current


def parse_match(path):
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    info = data.get("info", {})
    match_id = path.stem
    teams = info.get("teams", [])

    toss = info.get("toss", {})
    outcome = info.get("outcome", {})
    outcome_by = outcome.get("by", {}) if isinstance(outcome, dict) else {}

    match_record = {
        "match_id": match_id,
        "match_date": (
            info.get("dates", [None])[0]
            if info.get("dates")
            else None
        ),
        "season": info.get("season"),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "team1": teams[0] if len(teams) > 0 else None,
        "team2": teams[1] if len(teams) > 1 else None,
        "toss_winner": toss.get("winner"),
        "toss_decision": toss.get("decision"),
        "winner": outcome.get("winner") if isinstance(outcome, dict) else None,
        "result_method": outcome.get("method") if isinstance(outcome, dict) else None,
        "result_runs": outcome_by.get("runs"),
        "result_wickets": outcome_by.get("wickets"),
        "result_type": outcome.get("result") if isinstance(outcome, dict) else None,
        "event_name": safe_get(info, "event", "name"),
        "gender": info.get("gender"),
        "match_type": info.get("match_type"),
        "overs_limit": info.get("overs"),
        "balls_per_over": info.get("balls_per_over", 6),
        "player_of_match": (
            info.get("player_of_match", [None])[0]
            if info.get("player_of_match")
            else None
        ),
    }

    innings_records = []
    delivery_records = []

    for innings_number, innings in enumerate(data.get("innings", []), start=1):
        batting_team = innings.get("team")
        overs = innings.get("overs", [])

        innings_total = 0
        innings_extras = 0
        delivery_count = 0
        wicket_count = 0

        for over in overs:
            over_number = over.get("over")
            deliveries = over.get("deliveries", [])

            for delivery_number, delivery in enumerate(deliveries, start=1):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras", {})
                wickets = delivery.get("wickets", [])

                batter_runs = runs.get("batter", 0)
                extra_runs = runs.get("extras", 0)
                total_runs = runs.get("total", batter_runs + extra_runs)

                innings_total += total_runs
                innings_extras += extra_runs
                delivery_count += 1
                wicket_count += len(wickets)

                # Cricsheet records each delivery, including wides and no-balls.
                # delivery_number is its position within the recorded over,
                # not necessarily the legal ball number.
                delivery_records.append({
                    "match_id": match_id,
                    "innings_number": innings_number,
                    "batting_team": batting_team,
                    "over_number": over_number,
                    "delivery_number": delivery_number,
                    "batter": delivery.get("batter"),
                    "bowler": delivery.get("bowler"),
                    "non_striker": delivery.get("non_striker"),
                    "batter_runs": batter_runs,
                    "extra_runs": extra_runs,
                    "total_runs": total_runs,
                    "wides": extras.get("wides", 0),
                    "noballs": extras.get("noballs", 0),
                    "byes": extras.get("byes", 0),
                    "legbyes": extras.get("legbyes", 0),
                    "penalty": extras.get("penalty", 0),
                    "is_wicket": len(wickets) > 0,
                    "wicket_count": len(wickets),
                    "wicket_types": "|".join(
                        wicket.get("kind", "") for wicket in wickets
                    ),
                    "player_out": "|".join(
                        wicket.get("player_out", "") for wicket in wickets
                    ),
                    "is_super_over": innings.get("super_over", False),
                })

        innings_records.append({
            "match_id": match_id,
            "innings_number": innings_number,
            "batting_team": batting_team,
            "total_runs": innings_total,
            "total_extras": innings_extras,
            "total_wickets": wicket_count,
            "recorded_deliveries": delivery_count,
            "is_super_over": innings.get("super_over", False),
            "target_runs": safe_get(innings, "target", "runs"),
            "target_overs": safe_get(innings, "target", "overs"),
        })

    return match_record, innings_records, delivery_records


def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw data folder not found: {RAW_DIR}")

    files = sorted(RAW_DIR.glob("*.json"))

    if not files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    matches = []
    innings_records = []
    delivery_records = []
    failed_files = []

    for index, path in enumerate(files, start=1):
        try:
            match, match_innings, match_deliveries = parse_match(path)
            matches.append(match)
            innings_records.extend(match_innings)
            delivery_records.extend(match_deliveries)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError,
                TypeError, ValueError, AttributeError, KeyError) as exc:
            failed_files.append({
                "file_name": path.name,
                "error": str(exc),
            })

        if index % 100 == 0 or index == len(files):
            print(f"Processed {index}/{len(files)} files")

    # Keep identifiers as strings and dates in a consistent format.
    matches_df = pd.DataFrame(matches)
    innings_df = pd.DataFrame(innings_records)
    deliveries_df = pd.DataFrame(delivery_records)

    
    if not matches_df.empty:
        matches_df["match_date"] = pd.to_datetime(
            matches_df["match_date"], errors="coerce"
        )

        # Cricsheet may represent seasons as integers or strings.
        matches_df["season"] = matches_df["season"].map(
            lambda value: str(value) if pd.notna(value) else None
        )
        matches_df["season"] = matches_df["season"].astype("string")


    outputs = {
        "matches.parquet": matches_df,
        "innings.parquet": innings_df,
        "deliveries.parquet": deliveries_df,
    }

    for filename, dataframe in outputs.items():
        destination = SILVER_DIR / filename
        dataframe.to_parquet(destination, index=False)
        print(f"Saved {filename}: {len(dataframe):,} rows")

    if failed_files:
        failure_path = PROJECT_ROOT / "reports" / "parse_failures.csv"
        pd.DataFrame(failed_files).to_csv(failure_path, index=False)
        print(f"Failed files: {len(failed_files)}")
        print(f"Failure report: {failure_path.relative_to(PROJECT_ROOT)}")
    else:
        print("Failed files: 0")

    print("\n===== SPORTSPULSE PARSING SUMMARY =====")
    print(f"Input files: {len(files)}")
    print(f"Parsed matches: {len(matches_df)}")
    print(f"Parsed innings: {len(innings_df)}")
    print(f"Parsed deliveries: {len(deliveries_df)}")


if __name__ == "__main__":
    main()
