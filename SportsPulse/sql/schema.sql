-- =====================================================================
-- SportsPulse: PostgreSQL Analytical Warehouse DDL Schema
-- =====================================================================

-- 1. Dimension Tables
CREATE TABLE IF NOT EXISTS dim_teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_players (
    player_id VARCHAR(50) PRIMARY KEY,
    player_name VARCHAR(150) NOT NULL
);

-- 2. Fact Tables
CREATE TABLE IF NOT EXISTS fact_matches (
    match_id VARCHAR(50) PRIMARY KEY,
    match_date DATE,
    team1_id INTEGER REFERENCES dim_teams(team_id),
    team2_id INTEGER REFERENCES dim_teams(team_id),
    venue VARCHAR(255),
    winner_id INTEGER REFERENCES dim_teams(team_id)
);

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
);

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
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_matches_team1 ON fact_matches(team1_id);
CREATE INDEX IF NOT EXISTS idx_fact_matches_team2 ON fact_matches(team2_id);
CREATE INDEX IF NOT EXISTS idx_fact_matches_winner ON fact_matches(winner_id);
CREATE INDEX IF NOT EXISTS idx_fact_matches_date ON fact_matches(match_date);
