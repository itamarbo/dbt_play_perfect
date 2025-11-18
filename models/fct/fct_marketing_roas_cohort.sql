{{ config(
    materialized='table'
) }}

-- Marketing ROAS Cohort Table 
with cohort_revenue as (
    select
        i.install_date,
        i.media_source,
        e.country_code as country,
        (e.date_utc - i.install_date)::int as days_since_install,
        sum(e.price_usd) as daily_revenue
    from {{ ref('stg_events') }} e
    inner join {{ ref('stg_installs_attribution_table') }} i
        on e.player_id = i.player_id 
        and e.country_code = i.install_country
    where e.price_usd is not null
        and e.date_utc >= i.install_date
    group by 1, 2, 3, 4
),

cumulative_data as (
    select
        install_date,
        media_source,
        country,
        days_since_install,
        sum(daily_revenue) over (
            partition by install_date, media_source, country
            order by days_since_install
        ) as cumulative_revenue
    from cohort_revenue
),

spend_data as (
    select
        install_date,
        media_source,
        country,
        sum(spend) as total_spend
    from {{ ref('stg_marketing_spend_table') }}
    group by 1, 2, 3
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
    end as roas_percentage,
    CURRENT_TIMESTAMP as fct_created_date
from cumulative_data c
left join spend_data s
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