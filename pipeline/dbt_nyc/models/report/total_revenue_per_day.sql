{{ config(order_by='(day)', engine='MergeTree()') }}

with revenue as (
    select formatDateTime(pickup_datetime, '%W') as day,
           total_amount as revenue
   from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select day, round(sum(revenue), 2) as total_revenue
	from revenue
	group by day
)

select * from final
