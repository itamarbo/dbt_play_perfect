with dates_data as (
SELECT * FROM {{ source('raw', 'installs_attribution') }}
)

select
	TO_DATE(install_date, 'DD/MM/YYYY') as install_date,
	media_source,
	install_country,
	player_id,
	CURRENT_TIMESTAMP as stg_created_date
from dates_data