{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


-- Execution Time: 233179.365 ms
with check_params(table_name) as
         (
             values ('raw_data.eod_historical_prices')
         ),
     eod_historical_prices as
         (
             select code as symbol,
                    date,
                    adjusted_close,
                    volume
             from {{ source('eod', 'eod_historical_prices') }}
             where date >= first_date
         ),

     tickers_checks_verbose_union as
         (
             (
                 select symbol,
                        table_name || '__adjusted_close_is_notpositive'                                      as code,
                        'daily'                                                                              as "period",
                        'Ticker ' || symbol || ' in table ' || table_name || ' has ' ||
                        count(date) ||
                        ' rows with adjusted_close not positive values (<=0). Example at ' || json_agg(date) as message
                 from eod_historical_prices
                          join check_params cp on true
                 where adjusted_close <= 0
                 group by symbol, table_name
                 having count(date) > 0
             )
             union all
             (
                 select symbol,
                        table_name || '__adjusted_close_is_null'                                 as code,
                        'daily'                                                                  as "period",
                        'Ticker ' || symbol || ' in table ' ||
                        table_name || ' has ' || count(date) ||
                        ' rows with adjusted_close = NULL values. Example at ' || json_agg(date) as message
                 from eod_historical_prices
                          join check_params cp on true
                 where adjusted_close is null
                 group by symbol, table_name
                 having count(date) > 0
             )
             union all
             (
                 select symbol,
                        table_name || '__volume_is_null'                                 as code,
                        'daily'                                                          as "period",
                        'Ticker ' || symbol || ' in table ' ||
                        table_name || ' has ' || count(date) ||
                        ' rows with volume = NULL values. Example at ' || json_agg(date) as message
                 from eod_historical_prices
                          join check_params cp on true
                 where volume is null
                 group by symbol, table_name
                 having count(date) > 0
             )
     )
select code || '__' || symbol as id,
       symbol,
       code,
       "period",
       message,
       now()                  as updated_at
from tickers_checks_verbose_union
