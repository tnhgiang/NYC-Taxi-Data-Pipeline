{{ config(order_by='(payment_type)', engine='MergeTree()') }}

with payment as (
    select payment_type
   from {{ source('nyc_taxi', 'fact_trips') }}
),

final as (
	select payment_type, count(*)
	from payment
	group by payment_type
)

select * from final
