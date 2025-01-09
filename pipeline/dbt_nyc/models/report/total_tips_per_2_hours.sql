{{ config(order_by='(time_range)', engine='MergeTree()') }}

with tip_timestamp_range as (
    select pickup_datetime,
           floor(toHour(pickup_datetime, 'UTC') / 2) as time_range
    from {{ source('nyc_taxi', 'fact_trips') }}
  	where tip_amount > 0
),

final as (
	select concat(time_range * 2, ' - ', (time_range + 1) * 2) as time_range, count(*) as count
	from tip_timestamp_range
	group by time_range
)

select * from final
