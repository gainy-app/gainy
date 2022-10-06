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

     -- Execution Time: 147882.394 ms
     tickers_checks as materialized
         (
             select *,
                    (adjusted_close <= 0)::int                 as iserror_adjusted_close_is_notpositive,
                    (adjusted_close is null)::int              as iserror_adjusted_close_is_null,
                    (volume is null)::int                      as iserror_volume_is_null
             from eod_historical_prices
                      left join check_params cp on true
         ), -- select * from tickers_checks;

     tickers_checks_verbose_union as
         (
             (
                 select distinct on (
                     symbol
                     ) -- agg >0 && distinct on (symbol) -> example case
                       symbol,
                       table_name || '__adjusted_close_is_notpositive'                            as code,
                       'daily'                                                                    as "period",
                       'Ticker ' || symbol || ' in table ' || table_name || ' has ' ||
                       iserror_adjusted_close_is_notpositive ||
                       ' rows with adjusted_close not positive values (<=0). Example at ' || date as message
                 from tickers_checks
                 where iserror_adjusted_close_is_notpositive > 0
                 order by symbol, date desc
             )
             union all
             (
                 select distinct on (
                     symbol
                     ) -- agg >0 && distinct on (symbol) -> example case
                       symbol,
                       table_name || '__adjusted_close_is_null'                       as code,
                       'daily'                                                        as "period",
                       'Ticker ' || symbol || ' in table ' ||
                       table_name || ' has ' || iserror_adjusted_close_is_null ||
                       ' rows with adjusted_close = NULL values. Example at ' || date as message
                 from tickers_checks
                 where iserror_adjusted_close_is_null > 0
                 order by symbol, date desc
             )
             union all
             (
                 select distinct on (
                     symbol
                     ) -- agg >0 && distinct on (symbol) -> example case
                       symbol,
                       table_name || '__volume_is_null'                       as code,
                       'daily'                                                as "period",
                       'Ticker ' || symbol || ' in table ' ||
                       table_name || ' has ' || iserror_volume_is_null ||
                       ' rows with volume = NULL values. Example at ' || date as message
                 from tickers_checks
                 where iserror_volume_is_null > 0
                 order by symbol, date desc
             )
         )
select code || '__' || symbol as id,
       symbol,
       code,
       "period",
       message,
       now()                  as updated_at
from tickers_checks_verbose_union
