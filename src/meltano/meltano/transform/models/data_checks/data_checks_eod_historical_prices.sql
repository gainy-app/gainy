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
with check_params(table_name, dt_interval,
                  depth_stddev, depth_stddev_threshold,
                  depth_check, depth_check_threshold, -- depth_stddev >= depth_check !!
                  allowable_sigma_dev_adjclose_percchange, allowable_sigma_dev_volume_dif,
                  symbol_asmarket_crypto, symbol_asmarket_other) as materialized
         (
             values ('raw_data.eod_historical_prices', interval '1 day',
                     interval '5 year', to_char(now()::timestamp - interval '5 year', 'YYYY-MM-DD'),
                     interval '5 year', to_char(now()::timestamp - interval '5 year', 'YYYY-MM-DD'),
                     5., 10.,
                     'BTC.CC', 'SPY')
         ),

     intrpl_dt_now_wrt_interval as materialized -- generating time frames
         ( -- now()='2022-06-22 15:15:14' with dt_interval='15 minutes'::interval will give: '2022-06-22 15:15:00' (it drops last not full frame)
             select (to_timestamp(
                                 (extract(epoch from now()::timestamp)::int / extract(epoch from cp.dt_interval)::int
                                     ) * extract(epoch from cp.dt_interval)::int
                         ) AT TIME ZONE
                     'UTC' --to cancel out TZ in to_timestamp(0)='1970-01-01 00:00:00+TZ' (will be shifted by 3 hrs if TZ was +3, ie '1970-01-01 03:00:00+03')
                        )::timestamp as value
             from check_params cp
         ),

     intrpl_symbol_asmarket_dt as materialized
         (
             select unnest(array [cp.symbol_asmarket_other, cp.symbol_asmarket_crypto]) as intrpl_symbol_asmarket,
                    TO_CHAR(d.dt, 'YYYY-MM-DD')                                         as date,
                    d.dt::timestamp
             from check_params cp
                      left join intrpl_dt_now_wrt_interval on true
                      cross join
                  generate_series(intrpl_dt_now_wrt_interval.value - cp.depth_stddev,
                                  intrpl_dt_now_wrt_interval.value,
                                  cp.dt_interval) as d(dt)
         ),

    /*
    * for cancel natural market price movements in measuring ticker anomaly price movements by "returns" (percentage price change d2d)
    * we need to first subtract market returns from ticker returns (and only after that do some stat measurements by stdev of returns)
    * so we need to guarantee that market(target) price will be presented in any dt that can occur in any ticker dt
    * and if we haven't row for market price when the ticker has - that mean we lost the market price data row
    * so we need to guarantee some reasonable intermediate value. The linear interpolation trick is all about it.
    */
     intrpl_symbol_asmarket_dt_prices as
         (
             select d.intrpl_symbol_asmarket,
                    date,
                    --t.adjusted_close as adjusted_close_raw, --have left it here if anyone wants to check
                    coalesce(t.adjusted_close,
                             linear_interpolate(
                                     extract(epoch from d.dt),
                                     LAST_VALUE_IGNORENULLS(
                                     case when t.adjusted_close is not null then extract(epoch from d.dt) else null end)
                                     over (lookback),
                                     LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
                                     LAST_VALUE_IGNORENULLS(
                                     case when t.adjusted_close is not null then extract(epoch from d.dt) else null end)
                                     over (lookforward),
                                     LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward)
                                 ),
                             LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward),
                             LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
                             0) as intrpl_symbol_asmarket_adjusted_close
             from intrpl_symbol_asmarket_dt as d
                      left join (
                                    select code as intrpl_symbol_asmarket,
                                           date,
                                           adjusted_close
                                    from {{ source('eod', 'eod_historical_prices') }} ehp
                                             join check_params cp on true
                                    where code = any (array [cp.symbol_asmarket_other, cp.symbol_asmarket_crypto])
                                ) as t using (intrpl_symbol_asmarket, date)
                 window
                     lookback as (partition by d.intrpl_symbol_asmarket order by date asc),
                     lookforward as (partition by d.intrpl_symbol_asmarket order by date desc)
         ),

     -- Execution Time: 19542.805 ms
     tickers_data as materialized
         (
             select code                                  as symbol,
                    date,
                    adjusted_close,
                    volume,
                    case
                        when t."type" = 'crypto' then cp.symbol_asmarket_crypto
                        else cp.symbol_asmarket_other end as intrpl_symbol_asmarket
             from {{ source('eod', 'eod_historical_prices') }} ehp
                      join {{ ref('tickers') }} t
                           on t.symbol = ehp.code -- we are interested in all tickers that are available in the app, so tickers table
                      left join check_params cp on true
             where date >= depth_stddev_threshold
         ),

     -- Execution Time: 108800.858 ms
     tickers_lag as
         (
             select t.*, -- symbol, dt, adjusted_close, volume, intrpl_symbol_asmarket
                    lag(t.adjusted_close, 1, t.adjusted_close)
                    over (partition by t.symbol order by t.date asc)                            as adjusted_close_pre,
                    lag(t.volume, 1, t.volume) over (partition by t.symbol order by t.date asc) as volume_pre,
                    sam.intrpl_symbol_asmarket_adjusted_close,
                    lag(sam.intrpl_symbol_asmarket_adjusted_close, 1, sam.intrpl_symbol_asmarket_adjusted_close)
                    over (partition by t.symbol order by t.date asc)                            as intrpl_symbol_asmarket_adjusted_close_pre
             from tickers_data t
                      join intrpl_symbol_asmarket_dt_prices sam using (intrpl_symbol_asmarket, date)
         ), -- select * from tickers_lag;

     -- Execution Time: 115141.280 ms
     tickers_difs as
         (
             select *,
                    adjusted_close - adjusted_close_pre                            as adjusted_close_dif,
                    (adjusted_close - adjusted_close_pre)
                        / (1e-30 + abs(adjusted_close_pre))                        as adjusted_close_perc_change,
                    volume - volume_pre                                            as volume_dif,
                    intrpl_symbol_asmarket_adjusted_close -
                    intrpl_symbol_asmarket_adjusted_close_pre                      as intrpl_symbol_asmarket_adjusted_close_dif,
                    (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
                        / (1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre)) as intrpl_symbol_asmarket_adjusted_close_perc_change,
                    (adjusted_close - adjusted_close_pre) / (1e-30 + abs(adjusted_close_pre))
                        - (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
                        / (1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre)) as adjusted_close_perc_change_wom
             from tickers_lag
         ), -- select * from tickers_difs;

     -- Execution Time: 125082.553 ms
     tickers_stddevs_means_devs as materialized
         (
             select *,
                    stddev_pop(adjusted_close_perc_change_wom) over (w_s)                                as stddev_adjusted_close_perc_change_wom,
                    avg(adjusted_close_perc_change_wom) over (w_s)                                       as mean_adjusted_close_perc_change_wom,
                    abs(adjusted_close_perc_change_wom - avg(adjusted_close_perc_change_wom) over (w_s)) as dev_adjusted_close_perc_change_wom,
                    stddev_pop(volume_dif) over (w_s)                                                    as stddev_volume_dif,
                    avg(volume_dif) over (w_s)                                                           as mean_volume_dif,
                    abs(volume_dif - avg(volume_dif) over (w_s))                                         as dev_volume_dif
             from tickers_difs
                 window w_s as (partition by symbol)
         ), -- select * from tickers_stddevs_means_devs;

     -- Execution Time: 147882.394 ms
     tickers_checks as materialized
         (
             select *,
                    case
                        when dev_adjusted_close_perc_change_wom >
                             stddev_adjusted_close_perc_change_wom * cp.allowable_sigma_dev_adjclose_percchange
                            then 1
                        else 0 end                                                  as iserror_adjusted_close_perc_change_dev_wom,
                    case
                        when dev_volume_dif >
                             stddev_volume_dif * cp.allowable_sigma_dev_volume_dif
                            then 1
                        else 0 end                                                  as iserror_volume_dif_dev,
                    case when adjusted_close = adjusted_close_pre then 1 else 0 end as iserror_adjusted_close_twice_same,
                    case when adjusted_close <= 0 then 1 else 0 end                 as iserror_adjusted_close_is_notpositive,
                    case when adjusted_close is null then 1 else 0 end              as iserror_adjusted_close_is_null,
                    case when volume is null then 1 else 0 end                      as iserror_volume_is_null
             from tickers_stddevs_means_devs
                      left join check_params cp on true
             where date >= depth_check_threshold
         ), -- select * from tickers_checks;

     tickers_checks_verbose_union as
         (
             (
                 select distinct on (
                     symbol
                     )-- symbols that was used as market will give 0 here and will not trigger (until allowable sigma > 0)
                      symbol,
                      table_name || '__adjusted_close_perc_change_dev_wom' as code,
                      'daily'                                              as "period",
                      json_build_object(
                          'table_name', table_name,
                          'stddev_threshold', allowable_sigma_dev_adjclose_percchange * stddev_adjusted_close_perc_change_wom,
                          'mu', mean_adjusted_close_perc_change_wom,
                          'stddev', stddev_adjusted_close_perc_change_wom,
                          'value', dev_adjusted_close_perc_change_wom,
                          'date', date
                      )::text                                              as message
                 from tickers_checks
                 where iserror_adjusted_close_perc_change_dev_wom > 0
                 order by symbol, date desc
             )
             union all
             (
                 select distinct on (
                     symbol
                     ) symbol,
                       table_name || '__volume_dif_dev' as code,
                       'daily'                          as "period",
                       json_build_object(
                           'table_name', table_name,
                           'stddev_threshold', allowable_sigma_dev_volume_dif * stddev_volume_dif,
                           'mu', mean_volume_dif,
                           'stddev', stddev_volume_dif,
                           'value', dev_volume_dif,
                           'date', date
                       )::text                          as message
                 from tickers_checks
                 where iserror_volume_dif_dev > 0
                 order by symbol, date desc
             )
             union all
             (
                 select distinct on (
                     symbol
                     ) -- agg >0 && distinct on (symbol) -> example case
                       symbol,
                       table_name || '__adjusted_close_twice_same'                       as code,
                       'daily'                                                           as "period",
                       'Ticker ' || symbol || ' in table ' ||
                       table_name || ' has ' || iserror_adjusted_close_twice_same ||
                       ' pairs of consecutive rows with same price. Example at ' || date as message
                 from tickers_checks
                 where iserror_adjusted_close_twice_same > 0
                 order by symbol, date desc
             )
             union all
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

union all

select 'old_historical_prices_' || symbol                    as id,
       symbol,
       'old_historical_prices'                                as code,
       'daily'                                                as period,
       'Ticker ' || symbol || ' has old historical prices.' as message,
       now()                                                 as updated_at
from old_historical_prices
