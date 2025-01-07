{{ config(order_by='(date)', engine='MergeTree()') }}

with pickup_date as (
    select CAST(pickup_datetime as date) as date
    from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select date, count(*) as total_trips
	from pickup_date
	group by date
)

select * from final
