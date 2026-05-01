with

    orders as (
        select * from {{ ref('clean_data') }}
        where 1=1
            and has_incomplete_data = False
            and qc_flagged = False
    ),

    sales_per_channel_country_month as (
        select
            date_trunc(date(created_at), month) as month,
            country_code,
            source_channel,
            count(distinct(order_id)) as order_count,
            sum(net_revenue * to_usd_conversion_rate) as net_revenue_usd
        from orders
        group by 
            month, 
            country_code, 
            source_channel
    )

select
    month,
    country_code,
    source_channel,
    order_count,
    net_revenue_usd
from sales_per_channel_country_month

-- dbt build --select sales_per_channel_country_month --full-refresh

