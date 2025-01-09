{{ config(order_by='(day, hour)', engine='MergeTree()') }}

with pickup_info as (
    select formatDateTime(pickup_datetime, '%W') as day,
    	   toHour(pickup_datetime, 'UTC') as hour,
           pickup_datetime
  	from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select day, hour, count(*) as trip_count
	from pickup_info
	group by day, hour
)

select * from final
