with events as (
SELECT * FROM {{ source('raw', 'events') }}
)

select
	timestamp_utc::timestamp as timestamp_utc,
	TO_DATE(date_utc, 'DD/MM/YYYY') as date_utc,
	event_name,
	player_id,
	level,
	balance_before,
	country_code,
	tournament_id,
	room_id,
	entry_fee,
	coalesce(coins_spent, 0) as coins_spent,
	players_capacity,
	play_duration,
	score,
	position,
	coalesce(reward, 0) as reward,
	coalesce(coins_claimed, 0) as coins_claimed,
	purchase_id,
	product_id,
	price_usd,
    CURRENT_TIMESTAMP as stg_created_date
from events