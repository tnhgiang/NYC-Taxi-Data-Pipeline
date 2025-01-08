{{ config(order_by='(date)', engine='MergeTree()') }}

with tip_amount as (
    select CAST(pickup_datetime as date) as date, tip_amount
    from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select date, round(sum(tip_amount), 2) as total_tips
	from tip_amount
	group by date
)

select * from final
