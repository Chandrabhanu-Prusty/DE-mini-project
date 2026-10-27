
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

NON_BOWLER_WICKETS = {
    "run out",
    "retired hurt",
    "retired out",
    "obstructing the field",
}


def require_columns(df, columns, name):
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"{name} is missing columns: {sorted(missing)}")


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    matches = pd.read_parquet(SILVER_DIR / "matches.parquet")
    innings = pd.read_parquet(SILVER_DIR / "innings.parquet")
    deliveries = pd.read_parquet(SILVER_DIR / "deliveries.parquet")

    require_columns(
        matches,
        ["match_id", "match_date", "team1", "team2", "winner"],
        "matches",
    )
    require_columns(
        innings,
        ["match_id", "innings_number", "batting_team", "total_runs",
         "total_wickets", "is_super_over"],
        "innings",
    )
    require_columns(
        deliveries,
        ["match_id", "innings_number", "batting_team", "batter", "bowler",
         "batter_runs", "extra_runs", "total_runs", "wides", "noballs",
         "byes", "legbyes", "penalty", "is_wicket", "wicket_count",
         "wicket_types", "player_out", "is_super_over"],
        "deliveries",
    )

    # Work on copies and make numeric fields safe for aggregation.
    d = deliveries.copy()
    for col in [
        "batter_runs", "extra_runs", "total_runs", "wides", "noballs",
        "byes", "legbyes", "penalty", "wicket_count",
    ]:
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0)

    d["is_super_over"] = d["is_super_over"].fillna(False).astype(bool)
    d["is_wicket"] = d["is_wicket"].fillna(False).astype(bool)

    # Exclude Super Overs from standard match/player statistics.
    regular = d.loc[~d["is_super_over"]].copy()

    # ---------------- PLAYER BATTING ----------------
    # A wide is not a ball faced. A no-ball is counted as a ball faced.
    regular["balls_faced"] = (regular["wides"] == 0).astype(int)
    regular["fours"] = (regular["batter_runs"] == 4).astype(int)
    regular["sixes"] = (regular["batter_runs"] == 6).astype(int)

    regular["batter_dismissed"] = False
    for idx, row in regular.iterrows():
        kinds = str(row["wicket_types"]).split("|")
        players_out = str(row["player_out"]).split("|")

        if row["batter"] in players_out:
            for kind in kinds:
                if kind not in {"retired hurt", "obstructing the field"}:
                    regular.at[idx, "batter_dismissed"] = True
                    break

    batting = regular.groupby("batter", dropna=True).agg(
        runs=("batter_runs", "sum"),
        balls_faced=("balls_faced", "sum"),
        fours=("fours", "sum"),
        sixes=("sixes", "sum"),
        dismissals=("batter_dismissed", "sum"),
        innings=("match_id", "size"),
    ).reset_index().rename(columns={"batter": "player_name"})

    # Count actual player-innings, not deliveries.
    batter_innings = regular[["match_id", "innings_number", "batter"]].drop_duplicates()
    batter_innings_count = batter_innings.groupby("batter").size()
    batting["innings"] = batting["player_name"].map(batter_innings_count).fillna(0).astype(int)

    batting["average"] = batting.apply(
        lambda r: round(r["runs"] / r["dismissals"], 2)
        if r["dismissals"] else None,
        axis=1,
    )
    batting["strike_rate"] = batting.apply(
        lambda r: round(100 * r["runs"] / r["balls_faced"], 2)
        if r["balls_faced"] else None,
        axis=1,
    )

    # ---------------- PLAYER BOWLING ----------------
    # Legal balls exclude wides and no-balls.
    regular["legal_balls"] = (
        (regular["wides"] == 0) & (regular["noballs"] == 0)
    ).astype(int)

    # Byes, leg-byes and penalty runs are not charged to the bowler.
    regular["bowler_runs_conceded"] = (
        regular["total_runs"]
        - regular["byes"]
        - regular["legbyes"]
        - regular["penalty"]
    ).clip(lower=0)

    regular["bowler_wickets"] = 0
    for idx, row in regular.iterrows():
        kinds = str(row["wicket_types"]).split("|")
        regular.at[idx, "bowler_wickets"] = sum(
            kind not in NON_BOWLER_WICKETS
            for kind in kinds
            if kind and kind.lower() != "nan"
        )

    bowling = regular.groupby("bowler", dropna=True).agg(
        legal_balls=("legal_balls", "sum"),
        runs_conceded=("bowler_runs_conceded", "sum"),
        wickets=("bowler_wickets", "sum"),
        deliveries_recorded=("match_id", "size"),
    ).reset_index().rename(columns={"bowler": "player_name"})

    bowling["overs"] = bowling["legal_balls"].map(
        lambda balls: f"{int(balls // 6)}.{int(balls % 6)}"
    )
    bowling["economy"] = bowling.apply(
        lambda r: round(r["runs_conceded"] / (r["legal_balls"] / 6), 2)
        if r["legal_balls"] else None,
        axis=1,
    )
    bowling["bowling_average"] = bowling.apply(
        lambda r: round(r["runs_conceded"] / r["wickets"], 2)
        if r["wickets"] else None,
        axis=1,
    )
    bowling["bowling_strike_rate"] = bowling.apply(
        lambda r: round(r["legal_balls"] / r["wickets"], 2)
        if r["wickets"] else None,
        axis=1,
    )

    # ---------------- MATCH SUMMARIES ----------------
    match_innings = innings.loc[~innings["is_super_over"].fillna(False)].copy()
    match_innings["innings_number"] = pd.to_numeric(
        match_innings["innings_number"], errors="coerce"
    )

    scorecards = match_innings.pivot_table(
        index="match_id",
        columns="innings_number",
        values="total_runs",
        aggfunc="sum",
    ).reset_index()

    scorecards.columns = [
        f"innings_{int(c)}_runs" if isinstance(c, (int, float)) else c
        for c in scorecards.columns
    ]

    match_summary = matches.merge(scorecards, on="match_id", how="left")

    # ---------------- TEAM PERFORMANCE ----------------
    valid_matches = matches.dropna(subset=["team1", "team2"]).copy()

    team_rows = pd.concat([
        valid_matches[["match_id", "match_date", "team1"]].rename(
            columns={"team1": "team_name"}
        ),
        valid_matches[["match_id", "match_date", "team2"]].rename(
            columns={"team2": "team_name"}
        ),
    ], ignore_index=True)

    played = team_rows.groupby("team_name").agg(
        matches_played=("match_id", "nunique"),
    ).reset_index()

    wins = (
        valid_matches.dropna(subset=["winner"])
        .groupby("winner")["match_id"]
        .nunique()
        .rename("matches_won")
    )

    team_performance = played.merge(
        wins, left_on="team_name", right_index=True, how="left"
    )
    team_performance["matches_won"] = (
        team_performance["matches_won"].fillna(0).astype(int)
    )
    team_performance["win_percentage"] = (
        100 * team_performance["matches_won"]
        / team_performance["matches_played"]
    ).round(2)

    # ---------------- SAVE GOLD TABLES ----------------
    outputs = {
        "player_batting.parquet": batting,
        "player_bowling.parquet": bowling,
        "team_performance.parquet": team_performance,
        "match_summary.parquet": match_summary,
        "innings_summary.parquet": match_innings,
    }

    for filename, df in outputs.items():
        df.to_parquet(GOLD_DIR / filename, index=False)
        print(f"Saved {filename}: {len(df):,} rows")

    print("\n===== SPORTSPULSE GOLD LAYER =====")
    print(f"Players with batting records: {len(batting):,}")
    print(f"Players with bowling records: {len(bowling):,}")
    print(f"Teams: {len(team_performance):,}")
    print(f"Matches summarized: {len(match_summary):,}")
    print(f"Output folder: {GOLD_DIR}")


if __name__ == "__main__":
    main()
