{{ config(
    materialized = 'table'
) }}

with

    raw_data as (
        select * from {{ source('raw_data', 'orders_raw') }}
    ),

    deduped as (
        select * except(rn)
        from (
            select
                *,
                row_number() over (
                    partition by order_id
                    order by created_at 
                ) as rn
            from raw_data
            where order_id is not null
                and trim(order_id) != ''
        )
        where rn = 1
    ),

    cleaned as (
        select
            order_id,

            format_timestamp(
                '%Y-%m-%d %H:%M:%S',
                coalesce(
                    safe.parse_timestamp('%Y-%m-%d %H:%M:%S', created_at),  -- 2024-02-14 11:19:00
                    safe.parse_timestamp('%Y-%m-%d',          created_at),  -- 2024-02-22
                    safe.parse_timestamp('%Y/%m/%d',          created_at),  -- 2024/02/22
                    safe.parse_timestamp('%d/%m/%Y %H:%M',    created_at),  -- 20/02/2024 04:42
                    safe.parse_timestamp('%d/%m/%Y',          created_at)   -- 20/02/2024
                )
            ) as created_at,

            customer_id,
            nullif(trim(source_channel), '')          as source_channel,
            lower(nullif(trim(financial_status), '')) as financial_status,
            nullif(trim(fulfillment_status), '')      as fulfillment_status,
            safe_cast(total_price     as numeric)     as total_price,
            safe_cast(total_discounts as numeric)     as total_discounts,
            safe_cast(net_revenue     as numeric)     as net_revenue,
            upper(nullif(trim(currency), ''))         as currency,
            product_title,
            safe_cast(quantity as int64)              as quantity,
            upper(nullif(trim(country_code), ''))     as country_code

        from deduped
    ),

    join_currency_rates as (
        select
            cleaned.*,
            case 
                when currency = 'GBP' then {{ var('currency_rates')['GBPUSD'] }}
                when currency = 'SGD' then {{ var('currency_rates')['SGDUSD'] }}
                when currency = 'AUD' then {{ var('currency_rates')['AUDUSD'] }}
                when currency = 'USD' then 1
                                      else null
            end as to_usd_conversion_rate
        from cleaned
    ),

    final as (
        select
            *,
            case
                when 
                    financial_status is null 
                    or source_channel is null then True
                                              else False
            end                                                              as has_incomplete_data,
            case
                when 
                    abs(net_revenue - (total_price - total_discounts)) > 0.01
                    or total_price is null or total_price <= 0
                    or quantity is null or quantity <= 0
                                                                  then True
                                                                  else False
            end                                                              as qc_flagged
        from join_currency_rates
    )

select
    order_id,
    created_at,
    customer_id,
    source_channel,
    financial_status,
    fulfillment_status,
    total_price,
    total_discounts,
    net_revenue,
    currency,
    to_usd_conversion_rate,
    product_title,
    quantity,
    country_code,
    has_incomplete_data,
    qc_flagged
from final

-- dbt build --select clean_data --full-refresh