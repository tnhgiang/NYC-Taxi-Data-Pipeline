{{ config(order_by='(date)', engine='MergeTree()') }}

with total_amount as (
    select CAST(pickup_datetime as date) as date, total_amount
    from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select date, round(sum(total_amount), 2) as total_income
	from total_amount
	group by date
)

select * from final
