
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# --------------------------------------------------
# PATHS AND DATABASE CONFIGURATION
# --------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR = ROOT / "data" / "gold"

load_dotenv(ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL not found. Please check the .env file in the project root."
    )

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# --------------------------------------------------
# LOAD PARQUET FILES
# --------------------------------------------------

teams = pd.read_parquet(SILVER_DIR / "dim_teams.parquet")
players = pd.read_parquet(SILVER_DIR / "dim_players.parquet")
matches = pd.read_parquet(GOLD_DIR / "match_summary.parquet")
batting = pd.read_parquet(GOLD_DIR / "player_batting.parquet")
bowling = pd.read_parquet(GOLD_DIR / "player_bowling.parquet")


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def clean_records(df):
    """Convert pandas records into SQL-friendly Python values."""
    cleaned = df.astype(object).where(pd.notna(df), None)
    records = cleaned.to_dict("records")

    for record in records:
        for key, value in record.items():
            if hasattr(value, "item"):
                record[key] = value.item()

    return records


def safe_name(value):
    """Normalize names used for database lookups."""
    if pd.isna(value):
        return None
    return str(value).strip()


# --------------------------------------------------
# CREATE FACT TABLES
# --------------------------------------------------

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_player_batting (
            player_name VARCHAR(150) PRIMARY KEY,
            runs BIGINT,
            innings INTEGER,
            dismissals INTEGER,
            average NUMERIC(10, 2),
            strike_rate NUMERIC(10, 2),
            fours INTEGER,
            sixes INTEGER,
            balls_faced BIGINT
        )
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_player_bowling (
            player_name VARCHAR(150) PRIMARY KEY,
            legal_balls BIGINT,
            runs_conceded BIGINT,
            wickets INTEGER,
            deliveries_recorded BIGINT,
            overs VARCHAR(20),
            economy NUMERIC(10, 2),
            bowling_average NUMERIC(10, 2),
            bowling_strike_rate NUMERIC(10, 2)
        )
    """))


# --------------------------------------------------
# LOAD DIMENSION TABLES
# --------------------------------------------------

with engine.begin() as conn:

    # Insert teams without replacing existing database IDs.
    team_records = [
        {"team_name": safe_name(name)}
        for name in teams["team_name"].dropna().unique()
        if safe_name(name)
    ]

    if team_records:
        conn.execute(text("""
            INSERT INTO dim_teams (team_name)
            VALUES (:team_name)
            ON CONFLICT (team_name) DO NOTHING
        """), team_records)

    # Preserve the player IDs generated during normalization.
    player_records = [
        {
            "player_id": str(row["player_id"]),
            "player_name": safe_name(row["player_name"]),
        }
        for row in players.to_dict("records")
        if safe_name(row["player_id"]) and safe_name(row["player_name"])
    ]

    if player_records:
        conn.execute(text("""
            INSERT INTO dim_players (player_id, player_name)
            VALUES (:player_id, :player_name)
            ON CONFLICT (player_id) DO UPDATE
            SET player_name = EXCLUDED.player_name
        """), player_records)

    # Retrieve PostgreSQL-generated team IDs.
    team_rows = conn.execute(text("""
        SELECT team_id, team_name
        FROM dim_teams
    """)).mappings().all()

    team_id_map = {
        safe_name(row["team_name"]): row["team_id"]
        for row in team_rows
    }

    # --------------------------------------------------
    # LOAD MATCH FACT TABLE
    # --------------------------------------------------

    match_records = []

    for row in matches.to_dict("records"):
        match_date = row.get("match_date")

        if pd.notna(match_date):
            match_date = pd.to_datetime(match_date).date()
        else:
            match_date = None

        team1 = safe_name(row.get("team1"))
        team2 = safe_name(row.get("team2"))
        winner = safe_name(row.get("winner"))

        match_records.append({
            "match_id": str(row["match_id"]),
            "match_date": match_date,
            "team1_id": team_id_map.get(team1),
            "team2_id": team_id_map.get(team2),
            "venue": safe_name(row.get("venue")),
            "winner_id": team_id_map.get(winner),
        })

    if match_records:
        conn.execute(text("""
            INSERT INTO fact_matches (
                match_id,
                match_date,
                team1_id,
                team2_id,
                venue,
                winner_id
            )
            VALUES (
                :match_id,
                :match_date,
                :team1_id,
                :team2_id,
                :venue,
                :winner_id
            )
            ON CONFLICT (match_id) DO UPDATE SET
                match_date = EXCLUDED.match_date,
                team1_id = EXCLUDED.team1_id,
                team2_id = EXCLUDED.team2_id,
                venue = EXCLUDED.venue,
                winner_id = EXCLUDED.winner_id
        """), match_records)

    # --------------------------------------------------
    # REPLACE BATTING FACT DATA
    # --------------------------------------------------

    conn.execute(text("DELETE FROM fact_player_batting"))

    batting_columns = [
        "player_name",
        "runs",
        "innings",
        "dismissals",
        "average",
        "strike_rate",
        "fours",
        "sixes",
        "balls_faced",
    ]

    batting_records = clean_records(batting[batting_columns])

    if batting_records:
        conn.execute(text("""
            INSERT INTO fact_player_batting (
                player_name,
                runs,
                innings,
                dismissals,
                average,
                strike_rate,
                fours,
                sixes,
                balls_faced
            )
            VALUES (
                :player_name,
                :runs,
                :innings,
                :dismissals,
                :average,
                :strike_rate,
                :fours,
                :sixes,
                :balls_faced
            )
        """), batting_records)

    # --------------------------------------------------
    # REPLACE BOWLING FACT DATA
    # --------------------------------------------------

    conn.execute(text("DELETE FROM fact_player_bowling"))

    bowling_columns = [
        "player_name",
        "legal_balls",
        "runs_conceded",
        "wickets",
        "deliveries_recorded",
        "overs",
        "economy",
        "bowling_average",
        "bowling_strike_rate",
    ]

    bowling_records = clean_records(bowling[bowling_columns])

    if bowling_records:
        conn.execute(text("""
            INSERT INTO fact_player_bowling (
                player_name,
                legal_balls,
                runs_conceded,
                wickets,
                deliveries_recorded,
                overs,
                economy,
                bowling_average,
                bowling_strike_rate
            )
            VALUES (
                :player_name,
                :legal_balls,
                :runs_conceded,
                :wickets,
                :deliveries_recorded,
                :overs,
                :economy,
                :bowling_average,
                :bowling_strike_rate
            )
        """), bowling_records)


# --------------------------------------------------
# VERIFY LOADED ROW COUNTS
# --------------------------------------------------

print("\nSportsPulse warehouse load completed.")
print("-" * 45)

with engine.connect() as conn:
    tables = [
        "dim_teams",
        "dim_players",
        "fact_matches",
        "fact_player_batting",
        "fact_player_bowling",
    ]

    for table in tables:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {table}")
        ).scalar_one()

        print(f"{table}: {count:,} rows")

print("-" * 45)

engine.dispose()
