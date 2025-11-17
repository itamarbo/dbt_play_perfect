with events as (
SELECT * FROM {{ source('raw', 'events') }}
)

select distinct
	timestamp_utc::timestamp as timestamp_utc,
	date_utc:: DATE as date_utc,
	event_name,
	player_id,
	level,
	balance_before,
	country_code,
	tournament_id,
	room_id,
	entry_fee,
	coins_spent,
	players_capacity,
	play_duration,
	score,
	position,
	reward,
	coins_claimed,
	purchase_id,
	product_id,
	price_usd,
    CURRENT_TIMESTAMP as mrr_created_date
from events