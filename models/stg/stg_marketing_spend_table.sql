with marketing_spend as (
SELECT * FROM {{ source('raw', 'marketing_spend') }}
)

select
	TO_DATE(date_utc, 'DD/MM/YYYY') as install_date,
	media_source,
	country,
	CAST(spend AS NUMERIC(10,2)) as spend
from marketing_spend