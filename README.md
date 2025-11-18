# Games Analytics - DBT Project

## Quick Start

```bash
dbt seed    # Load data
dbt run     # Build models
```

## Models

**Staging:** `stg_events`, `stg_installs_attribution_table`, `stg_marketing_spend_table`

**Fact Tables:**

- `fct_games` - Player game sessions
- `daily_player` - Daily player activity
- `fct_rooms` - Room aggregations
- `fct_marketing_roas_cohort` - Marketing ROAS by cohort

## ROAS Dashboard Query

```sql
SELECT
    media_source,
    MAX(CASE WHEN days_since_install = 7 THEN roas_percentage END) as d7,
    MAX(CASE WHEN days_since_install = 30 THEN roas_percentage END) as d30,
    MAX(CASE WHEN days_since_install = 90 THEN roas_percentage END) as d90
FROM fct_marketing_roas_cohort
WHERE install_date = '2024-07-01'
GROUP BY media_source;
```

Itamar Ben Oren
