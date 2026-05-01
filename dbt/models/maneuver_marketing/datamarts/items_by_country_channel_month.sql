with

    orders as (
        select * from {{ ref('clean_data') }}
        where 1=1
            and has_incomplete_data = False
            and qc_flagged = False
    ),

    items_by_country_channel_month as (
        select
            date_trunc(date(created_at), month) as month,
            source_channel,
            country_code,
            product_title,
            safe_divide(sum(total_price), sum(quantity)) as price_per_item,
            safe_divide(sum(quantity), count(distinct(order_id))) as quantity_per_order,
            count(distinct(order_id)) as order_count,
            sum(quantity) as quantity,
            sum(net_revenue * to_usd_conversion_rate) as net_revenue_usd
        from orders
        group by
            month,
            source_channel,
            country_code,
            product_title
    )

select
    month,
    source_channel,
    country_code,
    product_title,
    price_per_item,
    quantity_per_order,
    order_count,
    quantity,
    net_revenue_us
from items_by_country_channel_month

-- dbt build --select items_by_country_channel_month --full-refresh

