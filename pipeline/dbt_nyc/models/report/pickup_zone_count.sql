{{ config(order_by='(zone)', engine='MergeTree()') }}

with pickup_location_info as (
	select ft.trip_id, dl.borough, dl.zone
	from {{ source('nyc_taxi', 'fact_trips')}} as ft
	join {{ source('nyc_taxi', 'dim_locations')}} as dl
		on ft.pickup_location_id = dl.location_id

),

final as (
	select zone, count(*) as count
	from pickup_location_info
	group by zone
)

select * from final
