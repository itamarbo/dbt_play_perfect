{{ config(
    materialized='table'
) }}

-- Marketing ROAS Cohort Table
with total_income_per_day_and_player as (
    select
        player_id,
        country_code,
        date_utc,
        sum(price_usd) as total_income
    from {{ ref('stg_events') }}
    where price_usd is not null
    group by player_id, country_code, date_utc
),

player_revenue_with_cohort as (
    select
        t2.install_date,
        t2.media_source,
        t1.country_code as country,
        t1.date_utc,
        t1.player_id,
        t1.total_income,
        (t1.date_utc - t2.install_date)::int as days_since_install
    from total_income_per_day_and_player t1
    inner join {{ ref('stg_installs_attribution_table') }} t2
        on t1.player_id = t2.player_id 
        and t1.country_code = t2.install_country
    where t1.date_utc >= t2.install_date
),

daily_cohort_revenue as (
    select
        install_date,
        media_source,
        country,
        days_since_install,
        sum(total_income) as daily_revenue
    from player_revenue_with_cohort
    group by install_date, media_source, country, days_since_install
),

cumulative_revenue as (
    select
        install_date,
        media_source,
        country,
        days_since_install,
        sum(daily_revenue) over (
            partition by install_date, media_source, country
            order by days_since_install
        ) as cumulative_revenue
    from daily_cohort_revenue
),

marketing_spend as (
    select
        install_date,
        media_source,
        country,
        sum(spend) as total_spend
    from {{ ref('stg_marketing_spend_table') }}
    group by install_date, media_source, country
)

select
    c.install_date,
    c.media_source,
    c.country,
    c.days_since_install,
    c.cumulative_revenue,
    s.total_spend,
    case 
        when s.total_spend > 0 
        then round((c.cumulative_revenue / s.total_spend * 100)::numeric, 1)
        else null
    end as roas_percentage
from cumulative_revenue c
left join marketing_spend s
    on c.install_date = s.install_date
    and c.media_source = s.media_source
    and c.country = s.country




-- SELECT 
--     media_source,
--     --MAX(CASE WHEN days_since_install = 0 THEN roas_percentage END) as d7,
--     MAX(CASE WHEN days_since_install = 7 THEN roas_percentage END) as d7,
--     MAX(CASE WHEN days_since_install = 30 THEN roas_percentage END) as d30,
--     MAX(CASE WHEN days_since_install = 90 THEN roas_percentage END) as d90
-- FROM fct_marketing_roas_cohort
-- WHERE install_date = '2024-07-01'
-- GROUP BY media_source;