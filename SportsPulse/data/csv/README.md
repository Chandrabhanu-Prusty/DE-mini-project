# SportsPulse — CSV Dataset Export (Converted Export)

This directory contains tabular CSV exports of the 1,243 historical IPL matches (2008–2024) converted from the Cricsheet source data.

## Files
1. `matches.csv`: Match-level metadata including match dates, participating teams, venues, toss details, outcomes, and winners.
2. `innings.csv`: Innings-level totals, run rates, and team batting orders.
3. `deliveries.csv`: Granular ball-by-ball delivery telemetry (295,732 rows) tracking runs, extras, dismissals, bowlers, and batters.
4. `README.md`: This dataset documentation and metadata descriptor.

## Purpose
This export provides a portable, tabular representation of the verified dataset for external data science exploration, machine learning experiments, and cross-platform analysis without requiring a local PostgreSQL database.
