
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def clean_text(series):
    """Trim whitespace and convert blank text values to missing values."""
    cleaned = series.astype("string").str.strip()
    return cleaned.mask(cleaned == "")


def main():
    matches_path = SILVER_DIR / "matches.parquet"
    innings_path = SILVER_DIR / "innings.parquet"
    deliveries_path = SILVER_DIR / "deliveries.parquet"

    for path in (matches_path, innings_path, deliveries_path):
        if not path.exists():
            raise FileNotFoundError(f"Required dataset missing: {path}")

    matches = pd.read_parquet(matches_path)
    innings = pd.read_parquet(innings_path)
    deliveries = pd.read_parquet(deliveries_path)

    # Normalize textual fields without changing their meanings.
    for column in ("team1", "team2", "toss_winner", "winner", "venue", "city"):
        if column in matches.columns:
            matches[column] = clean_text(matches[column])

    for column in ("batting_team",):
        if column in innings.columns:
            innings[column] = clean_text(innings[column])
        if column in deliveries.columns:
            deliveries[column] = clean_text(deliveries[column])

    for column in ("batter", "bowler", "non_striker"):
        if column in deliveries.columns:
            deliveries[column] = clean_text(deliveries[column])

    # Keep match IDs consistent across all tables.
    matches["match_id"] = matches["match_id"].astype("string")
    innings["match_id"] = innings["match_id"].astype("string")
    deliveries["match_id"] = deliveries["match_id"].astype("string")

    # Remove duplicate match records only when the full row is identical.
    before = len(matches)
    matches = matches.drop_duplicates()
    removed_matches = before - len(matches)

    # Build a distinct team dimension from match and innings records.
    team_values = pd.concat(
        [
            matches["team1"],
            matches["team2"],
            matches["toss_winner"],
            matches["winner"],
            innings["batting_team"],
        ],
        ignore_index=True,
    ).dropna()

    teams = pd.DataFrame({
        "team_name": sorted(team_values.unique().tolist())
    })
    teams.insert(0, "team_id", range(1, len(teams) + 1))

    # Build a player dimension from delivery participants and awards.
    player_columns = [
        deliveries["batter"],
        deliveries["bowler"],
        deliveries["non_striker"],
        matches["player_of_match"].astype("string"),
    ]

    players_values = pd.concat(
        player_columns, ignore_index=True
    ).dropna()

    players = pd.DataFrame({
        "player_name": sorted(players_values.unique().tolist())
    })
    players.insert(0, "player_id", range(1, len(players) + 1))

    # Save normalized Silver tables.
    matches.to_parquet(matches_path, index=False)
    innings.to_parquet(innings_path, index=False)
    deliveries.to_parquet(deliveries_path, index=False)

    teams.to_parquet(SILVER_DIR / "dim_teams.parquet", index=False)
    players.to_parquet(SILVER_DIR / "dim_players.parquet", index=False)

    print("\n===== ENTITY NORMALIZATION SUMMARY =====")
    print(f"Matches: {len(matches):,}")
    print(f"Exact duplicate match rows removed: {removed_matches}")
    print(f"Innings: {len(innings):,}")
    print(f"Deliveries: {len(deliveries):,}")
    print(f"Distinct teams: {len(teams):,}")
    print(f"Distinct players: {len(players):,}")
    print(f"Output directory: {SILVER_DIR}")


if __name__ == "__main__":
    main()
