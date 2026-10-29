
from pathlib import Path
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

try:
    from dashboard.pipeline_control import render_pipeline_control_center
except ImportError:
    from pipeline_control import render_pipeline_control_center

try:
    from dashboard.architecture import render_system_architecture_page
except ImportError:
    from architecture import render_system_architecture_page


# ============================================================
# CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold"

load_dotenv(ROOT / ".env")
DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(
    page_title="SportsPulse | IPL Analytics",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.8rem;
        padding-bottom: 2rem;
    }

    div[data-testid="stMetric"] {
        background-color: #f3f6fb;
        border: 1px solid #dbe4f0;
        padding: 16px;
        border-radius: 12px;
    }

    div[data-testid="stMetric"] label,
    div[data-testid="stMetric"] label p,
    div[data-testid="stMetric"] [data-testid="stMetricValue"],
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        color: #1e293b !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# SIDEBAR NAVIGATION
# ============================================================

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Batting Analytics",
        "Bowling Analytics",
        "Team Analytics",
        "Match Explorer",
        "Pipeline Control",
        "System Architecture",
    ],
)

st.sidebar.divider()
st.sidebar.caption("SportsPulse · IPL Analytics")
st.sidebar.caption("Warehouse: PostgreSQL")


# ============================================================
# STANDALONE CONTROL & ARCHITECTURE DISPATCH
# ============================================================

if page == "Pipeline Control":
    render_pipeline_control_center(ROOT)
    st.divider()
    st.caption("SportsPulse | Cricket Data Engineering & Analytics Pipeline")
    st.stop()

if page == "System Architecture":
    render_system_architecture_page(ROOT)
    st.divider()
    st.caption("SportsPulse | Cricket Data Engineering & Analytics Pipeline")
    st.stop()


# ============================================================
# DATABASE CONNECTION & LOADING (FOR ANALYTICS PAGES)
# ============================================================

@st.cache_resource
def get_db_engine():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is missing. Check .env file.")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(ttl=300)
def load_data():
    engine = get_db_engine()

    with engine.connect() as conn:
        batting = pd.read_sql(
            text("""
                SELECT player_name, runs, innings, dismissals,
                       average, strike_rate, fours, sixes, balls_faced
                FROM fact_player_batting
            """),
            conn,
        )

        bowling = pd.read_sql(
            text("""
                SELECT player_name, legal_balls, runs_conceded,
                       wickets, deliveries_recorded, overs, economy,
                       bowling_average, bowling_strike_rate
                FROM fact_player_bowling
            """),
            conn,
        )

        matches = pd.read_sql(
            text("""
                SELECT
                    f.match_id,
                    f.match_date,
                    f.venue,
                    t1.team_name AS team1,
                    t2.team_name AS team2,
                    tw.team_name AS winner
                FROM fact_matches f
                LEFT JOIN dim_teams t1
                    ON f.team1_id = t1.team_id
                LEFT JOIN dim_teams t2
                    ON f.team2_id = t2.team_id
                LEFT JOIN dim_teams tw
                    ON f.winner_id = tw.team_id
            """),
            conn,
        )

        teams = pd.read_sql(
            text("""
                SELECT
                    t.team_name AS team,
                    COUNT(DISTINCT f.match_id) AS matches_played,
                    COUNT(DISTINCT f.match_id) FILTER (
                        WHERE f.winner_id = t.team_id
                    ) AS matches_won,
                    ROUND(
                        100.0 * COUNT(DISTINCT f.match_id) FILTER (
                            WHERE f.winner_id = t.team_id
                        ) / NULLIF(COUNT(DISTINCT f.match_id), 0),
                        2
                    ) AS win_percentage
                FROM dim_teams t
                LEFT JOIN fact_matches f
                    ON t.team_id = f.team1_id
                    OR t.team_id = f.team2_id
                GROUP BY t.team_id, t.team_name
                ORDER BY matches_played DESC
            """),
            conn,
        )

    innings_path = GOLD / "innings_summary.parquet"
    if not innings_path.exists():
        raise FileNotFoundError(
            f"Missing file: {innings_path}"
        )

    innings = pd.read_parquet(innings_path)

    return {
        "batting": batting,
        "bowling": bowling,
        "teams": teams,
        "matches": matches,
        "innings": innings,
    }


# ============================================================
# LOAD AND PREPARE DATA
# ============================================================

st.title("🏏 SportsPulse")
st.caption("IPL Data Engineering & Analytics Platform")

try:
    data = load_data()
except Exception as exc:
    st.error(f"Could not load dashboard data from PostgreSQL: {exc}")
    st.info(
        "Check that PostgreSQL is running, the .env file is correct, "
        "the warehouse loader completed successfully, and the Gold "
        "Parquet files exist. Tip: You can execute or inspect the pipeline "
        "from the **Pipeline Control** page in the sidebar."
    )
    st.stop()

batting = data["batting"].copy()
bowling = data["bowling"].copy()
teams = data["teams"].copy()
matches = data["matches"].copy()
innings = data["innings"].copy()

# Convert numeric columns safely.
for frame, columns in [
    (
        batting,
        [
            "runs", "innings", "dismissals", "average",
            "strike_rate", "fours", "sixes", "balls_faced",
        ],
    ),
    (
        bowling,
        [
            "wickets", "economy", "runs_conceded",
            "legal_balls", "deliveries_recorded",
            "bowling_average", "bowling_strike_rate",
        ],
    ),
    (
        teams,
        ["matches_played", "matches_won", "win_percentage"],
    ),
]:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(
                frame[column], errors="coerce"
            )

if "match_date" in matches.columns:
    matches["match_date"] = pd.to_datetime(
        matches["match_date"], errors="coerce"
    )

# Handle missing names.
for frame, columns in [
    (batting, ["player_name"]),
    (bowling, ["player_name"]),
    (teams, ["team"]),
]:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].fillna("Unknown")




# ============================================================
# OVERVIEW
# ============================================================

if page == "Overview":
    st.header("Tournament Overview")
    st.caption(
        "A consolidated view of IPL matches, batting, bowling and teams."
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Matches", f"{len(matches):,}")
    c2.metric(
        "Teams",
        f"{teams['team'].nunique():,}" if "team" in teams else "—",
    )
    c3.metric(
        "Batters",
        f"{batting['player_name'].nunique():,}"
        if "player_name" in batting else "—",
    )
    c4.metric(
        "Bowlers",
        f"{bowling['player_name'].nunique():,}"
        if "player_name" in bowling else "—",
    )

    left, right = st.columns(2)

    with left:
        st.subheader("Top Run Scorers")
        if not batting.empty:
            top_batters = batting.nlargest(
                10, "runs"
            ).sort_values("runs", ascending=True)

            fig = px.bar(
                top_batters,
                x="runs",
                y="player_name",
                orientation="h",
                title="Top 10 Batters by Runs",
                labels={
                    "runs": "Total Runs",
                    "player_name": "Player",
                },
            )
            fig.update_layout(yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No batting data available.")

    with right:
        st.subheader("Top Wicket Takers")
        if not bowling.empty:
            top_bowlers = bowling.nlargest(
                10, "wickets"
            ).sort_values("wickets", ascending=True)

            fig = px.bar(
                top_bowlers,
                x="wickets",
                y="player_name",
                orientation="h",
                title="Top 10 Bowlers by Wickets",
                labels={
                    "wickets": "Wickets",
                    "player_name": "Player",
                },
            )
            fig.update_layout(yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No bowling data available.")

    st.subheader("Team Win Summary")
    if not teams.empty:
        chart_teams = teams.sort_values(
            "matches_won", ascending=False
        )
        fig = px.bar(
            chart_teams,
            x="team",
            y="matches_won",
            title="Matches Won by Team",
            labels={
                "team": "Team",
                "matches_won": "Matches Won",
            },
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent Matches")
    st.dataframe(
        matches.sort_values(
            "match_date", ascending=False, na_position="last"
        ).head(10),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# BATTING ANALYTICS
# ============================================================

elif page == "Batting Analytics":
    st.header("Batting Analytics")

    if batting.empty:
        st.warning("No batting records found in PostgreSQL.")
        st.stop()

    min_runs = int(batting["runs"].fillna(0).min())
    max_runs = int(batting["runs"].fillna(0).max())

    run_range = st.slider(
        "Filter by total runs",
        min_value=min_runs,
        max_value=max(max_runs, min_runs),
        value=(min_runs, max(max_runs, min_runs)),
    )

    filtered = batting[
        batting["runs"].between(run_range[0], run_range[1])
    ].copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Players", f"{len(filtered):,}")
    c2.metric(
        "Total Runs",
        f"{int(filtered['runs'].sum()):,}",
    )
    c3.metric(
        "Average Strike Rate",
        f"{filtered['strike_rate'].mean():.2f}"
        if filtered["strike_rate"].notna().any()
        else "—",
    )

    left, right = st.columns(2)

    with left:
        st.subheader("Runs by Player")
        top = filtered.nlargest(15, "runs").sort_values("runs")
        fig = px.bar(
            top,
            x="runs",
            y="player_name",
            orientation="h",
            labels={"runs": "Runs", "player_name": "Player"},
        )
        fig.update_layout(yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Strike Rate vs Runs")
        plot_data = filtered.dropna(
            subset=["runs", "strike_rate"]
        )
        fig = px.scatter(
            plot_data,
            x="runs",
            y="strike_rate",
            hover_name="player_name",
            labels={
                "runs": "Runs",
                "strike_rate": "Strike Rate",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Batting Records")
    st.dataframe(
        filtered.sort_values("runs", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# BOWLING ANALYTICS
# ============================================================

elif page == "Bowling Analytics":
    st.header("Bowling Analytics")

    if bowling.empty:
        st.warning("No bowling records found in PostgreSQL.")
        st.stop()

    min_wickets = int(bowling["wickets"].fillna(0).min())
    max_wickets = int(bowling["wickets"].fillna(0).max())

    wicket_range = st.slider(
        "Filter by wickets",
        min_value=min_wickets,
        max_value=max(max_wickets, min_wickets),
        value=(
            min_wickets,
            max(max_wickets, min_wickets),
        ),
    )

    filtered = bowling[
        bowling["wickets"].between(
            wicket_range[0], wicket_range[1]
        )
    ].copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Bowlers", f"{len(filtered):,}")
    c2.metric(
        "Total Wickets",
        f"{int(filtered['wickets'].sum()):,}",
    )
    c3.metric(
        "Average Economy",
        f"{filtered['economy'].mean():.2f}"
        if filtered["economy"].notna().any()
        else "—",
    )

    left, right = st.columns(2)

    with left:
        st.subheader("Wickets by Bowler")
        top = filtered.nlargest(
            15, "wickets"
        ).sort_values("wickets")

        fig = px.bar(
            top,
            x="wickets",
            y="player_name",
            orientation="h",
            labels={
                "wickets": "Wickets",
                "player_name": "Bowler",
            },
        )
        fig.update_layout(yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Economy vs Wickets")
        plot_data = filtered.dropna(
            subset=["economy", "wickets"]
        )
        fig = px.scatter(
            plot_data,
            x="economy",
            y="wickets",
            hover_name="player_name",
            labels={
                "economy": "Economy Rate",
                "wickets": "Wickets",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Bowling Records")
    st.dataframe(
        filtered.sort_values(
            "wickets", ascending=False
        ),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# TEAM ANALYTICS
# ============================================================

elif page == "Team Analytics":
    st.header("Team Analytics")

    if teams.empty:
        st.warning("No team records found in PostgreSQL.")
        st.stop()

    left, right = st.columns(2)

    with left:
        fig = px.bar(
            teams.sort_values("matches_played"),
            x="matches_played",
            y="team",
            orientation="h",
            title="Matches Played",
            labels={
                "matches_played": "Matches Played",
                "team": "Team",
            },
        )
        fig.update_layout(yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    with right:
        fig = px.bar(
            teams.sort_values("matches_won"),
            x="matches_won",
            y="team",
            orientation="h",
            title="Matches Won",
            labels={
                "matches_won": "Matches Won",
                "team": "Team",
            },
        )
        fig.update_layout(yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Team Win Percentage")
    fig = px.bar(
        teams.sort_values("win_percentage"),
        x="win_percentage",
        y="team",
        orientation="h",
        labels={
            "win_percentage": "Win Percentage (%)",
            "team": "Team",
        },
    )
    fig.update_layout(yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Team Statistics")
    st.dataframe(
        teams.sort_values("matches_won", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# MATCH EXPLORER
# ============================================================

elif page == "Match Explorer":
    st.header("Match Explorer")

    if matches.empty:
        st.warning("No match records found in PostgreSQL.")
        st.stop()

    filtered = matches.copy()

    team_options = sorted(
        set(matches["team1"].dropna().tolist())
        | set(matches["team2"].dropna().tolist())
    )

    selected_team = st.selectbox(
        "Filter by team",
        ["All Teams"] + team_options,
    )

    if selected_team != "All Teams":
        filtered = filtered[
            (filtered["team1"] == selected_team)
            | (filtered["team2"] == selected_team)
        ]

    venue_options = sorted(
        matches["venue"].dropna().unique().tolist()
    )

    selected_venue = st.selectbox(
        "Filter by venue",
        ["All Venues"] + venue_options,
    )

    if selected_venue != "All Venues":
        filtered = filtered[
            filtered["venue"] == selected_venue
        ]

    st.metric("Matching Records", f"{len(filtered):,}")

    st.dataframe(
        filtered.sort_values(
            "match_date", ascending=False, na_position="last"
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Match Results by Winner")
    winners = (
        filtered["winner"]
        .dropna()
        .value_counts()
        .rename_axis("team")
        .reset_index(name="wins")
    )

    if not winners.empty:
        fig = px.bar(
            winners,
            x="team",
            y="wins",
            labels={"team": "Team", "wins": "Matches Won"},
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No winner information available for this selection.")


# ============================================================
# FOOTER
# ============================================================

st.divider()
st.caption(
    "SportsPulse | Cricket Data Engineering & Analytics Pipeline"
)
