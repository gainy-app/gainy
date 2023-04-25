{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      index('symbol'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


-- Execution Time: 164441.197 ms
with check_params(table_name, dt_interval,
                  depth_stddev, depth_stddev_threshold,
                  depth_check, depth_check_threshold, -- depth_stddev >= depth_check !!
                  allowable_sigma_dev_adjclose_percchange, allowable_sigma_dev_volume_dif,
                  symbol_asmarket_crypto, symbol_asmarket_other) as
         (
             values ('public_*.historical_prices', interval '1 day',
                     interval '5 year', now()::timestamp - interval '5 year',
                     interval '5 year', now()::timestamp - interval '5 year',
                     5., 10.,
                     'BTC.CC', 'SPY')
         ),

     intrpl_dt_now_wrt_interval as -- generating time frames
         ( -- now()='2022-06-22 15:15:14' with dt_interval='15 minutes'::interval will give: '2022-06-22 15:15:00' (it drops last not full frame)
             select (to_timestamp(
                                 (extract(epoch from now()::timestamp)::int / extract(epoch from cp.dt_interval)::int
                                     ) * extract(epoch from cp.dt_interval)::int
                         ) AT TIME ZONE
                     'UTC' --to cancel out TZ in to_timestamp(0)='1970-01-01 00:00:00+TZ' (will be shifted by 3 hrs if TZ was +3, ie '1970-01-01 03:00:00+03')
                        )::timestamp as value
             from check_params cp
         ),

     intrpl_symbol_asmarket_dt as
         (
             select unnest(array [cp.symbol_asmarket_other, cp.symbol_asmarket_crypto]) as intrpl_symbol_asmarket,
                    d.dt::date                                                          as date,
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
                             public.linear_interpolate(
                                     extract(epoch from d.dt),
                                     public.LAST_VALUE_IGNORENULLS(
                                     case when t.adjusted_close is not null then extract(epoch from d.dt) else null end)
                                     over (lookback),
                                     public.LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
                                     public.LAST_VALUE_IGNORENULLS(
                                     case when t.adjusted_close is not null then extract(epoch from d.dt) else null end)
                                     over (lookforward),
                                     public.LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward)
                                 ),
                             public.LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookforward),
                             public.LAST_VALUE_IGNORENULLS(t.adjusted_close) over (lookback),
                             0) as intrpl_symbol_asmarket_adjusted_close
             from intrpl_symbol_asmarket_dt as d
                      left join (
                                    select symbol as intrpl_symbol_asmarket,
                                           date,
                                           adjusted_close
                                    from {{ ref('historical_prices') }} ehp
                                             join check_params cp on true
                                    where symbol = any (array [cp.symbol_asmarket_other, cp.symbol_asmarket_crypto])
                                ) as t using (intrpl_symbol_asmarket, date)
                 window
                     lookback as (partition by d.intrpl_symbol_asmarket order by date asc),
                     lookforward as (partition by d.intrpl_symbol_asmarket order by date desc)
         ),

     -- Execution Time: 19542.805 ms
     tickers_data as
         (
             select symbol,
                    date,
                    adjusted_close,
                    volume,
                    case
                        when t."type" = 'crypto' then cp.symbol_asmarket_crypto
                        else cp.symbol_asmarket_other end as intrpl_symbol_asmarket
             from {{ ref('historical_prices') }} ehp
                      join {{ ref('tickers') }} t using (symbol) -- we are interested in all tickers that are available in the app, so tickers table
                      left join check_params cp on true
             where date >= depth_stddev_threshold
         ),

     -- Execution Time: 108800.858 ms
     tickers_lag as
         (
             select t.*, -- symbol, dt, adjusted_close, volume, intrpl_symbol_asmarket
                    lag(t.adjusted_close) over (partition by t.symbol order by t.date)                          as adjusted_close_pre,
                    lag(t.volume) over (partition by t.symbol order by t.date)                                  as volume_pre,
                    sam.intrpl_symbol_asmarket_adjusted_close,
                    lag(sam.intrpl_symbol_asmarket_adjusted_close) over (partition by t.symbol order by t.date) as intrpl_symbol_asmarket_adjusted_close_pre
             from tickers_data t
                      join intrpl_symbol_asmarket_dt_prices sam using (intrpl_symbol_asmarket, date)
         ), -- select * from tickers_lag;

     -- Execution Time: 115141.280 ms
     tickers_difs as
         (
             select *,
                    adjusted_close - adjusted_close_pre                            as adjusted_close_dif,
                    (adjusted_close - adjusted_close_pre)
                        / ({{ var('epsilon') }} + abs(adjusted_close_pre))                        as adjusted_close_perc_change,
                    volume - volume_pre                                            as volume_dif,
                    (adjusted_close - adjusted_close_pre) / ({{ var('epsilon') }} + abs(adjusted_close_pre))
                        - (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
                        / ({{ var('epsilon') }} + abs(intrpl_symbol_asmarket_adjusted_close_pre)) as adjusted_close_perc_change_wom
             from tickers_lag
         ), -- select * from tickers_difs;
     -- Execution Time: 125082.553 ms
     tickers_stddevs_means_devs as
         (
             select *,
                    stddev_pop(adjusted_close_perc_change_wom) over (w_s) as stddev_adjusted_close_perc_change_wom,
                    avg(adjusted_close_perc_change_wom) over (w_s)        as mean_adjusted_close_perc_change_wom,
                    abs(adjusted_close_perc_change_wom
                        - avg(adjusted_close_perc_change_wom) over (w_s)) as dev_adjusted_close_perc_change_wom,
                    stddev_pop(volume_dif) over (w_s)                     as stddev_volume_dif,
                    avg(volume_dif) over (w_s)                            as mean_volume_dif,
                    abs(volume_dif
                        - avg(volume_dif) over (w_s))                     as dev_volume_dif
             from tickers_difs
                 window w_s as (partition by symbol)
         ), -- select * from tickers_stddevs_means_devs;

     -- Execution Time: 147882.394 ms
     tickers_checks as materialized
         (
             select *,
                    (
                        dev_adjusted_close_perc_change_wom > stddev_adjusted_close_perc_change_wom * cp.allowable_sigma_dev_adjclose_percchange
                    )::int                                     as iserror_adjusted_close_perc_change_dev_wom,
                    (
                        dev_volume_dif > stddev_volume_dif * cp.allowable_sigma_dev_volume_dif
                    )::int                                     as iserror_volume_dif_dev,
                    (adjusted_close <= 0)::int                 as iserror_adjusted_close_is_notpositive,
                    (adjusted_close is null)::int              as iserror_adjusted_close_is_null,
                    (volume is null)::int                      as iserror_volume_is_null
             from tickers_stddevs_means_devs
                      left join check_params cp on true
         ), -- select * from tickers_checks;

     tickers_checks_verbose_union as
         (
             (
                 select symbol,
                        table_name || '__adjusted_close_perc_change_dev_wom' as code,
                        'daily'                          as "period",
                        json_build_object(
                            'table_name', table_name,
                            'stddev_threshold', max(allowable_sigma_dev_adjclose_percchange * stddev_adjusted_close_perc_change_wom),
                            'mu', max(mean_adjusted_close_perc_change_wom),
                            'stddev', max(stddev_adjusted_close_perc_change_wom),
                            'value', max(dev_adjusted_close_perc_change_wom),
                            'date', max(date)
                        )::text                          as message
                 from tickers_checks
                         join (
                                  select date
                                  from tickers_checks
                                  where date not between '2020-03-13' and '2020-03-24'
                                    and date not in ('2020-11-09')
                                  group by date
                                  having avg(iserror_adjusted_close_perc_change_dev_wom) > 0.1
                              ) t using (date)
                 where iserror_adjusted_close_perc_change_dev_wom > 0
                 group by symbol, table_name
             )
             union all
             (
                 select symbol,
                        table_name || '__volume_dif_dev' as code,
                        'daily'                          as "period",
                        json_build_object(
                            'table_name', table_name,
                            'stddev_threshold', max(allowable_sigma_dev_volume_dif * stddev_volume_dif),
                            'mu', max(mean_volume_dif),
                            'stddev', max(stddev_volume_dif),
                            'value', max(dev_volume_dif),
                            'date', max(date)
                        )::text                          as message
                 from tickers_checks
                         join (
                                  select date
                                  from tickers_checks
                                  group by date
                                  having avg(iserror_volume_dif_dev) > 0.05
                                     and date not between '2018-01-06' and '2018-01-07'
                              ) t using (date)
                 where iserror_volume_dif_dev > 0
                 group by symbol, table_name
             )
             union all
             (
                 select distinct on (
                     symbol
                     ) -- agg >0 && distinct on (symbol) -> example case
                       symbol,
                       table_name || '__adjusted_close_is_notpositive'                            as code,
                       'daily'                                                                    as "period",
                       'Tickers ' || symbol || ' in table ' || table_name || ' has ' ||
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
                       'Tickers ' || symbol || ' in table ' ||
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
                       'Tickers ' || symbol || ' in table ' ||
                       table_name || ' has ' || iserror_volume_is_null ||
                       ' rows with volume = NULL values. Example at ' || date as message
                 from tickers_checks
                 where iserror_volume_is_null > 0
                 order by symbol, date desc
             )
         ),
     latest_trading_day as
         (
             select symbol, date
             from {{ ref('week_trading_sessions_static') }}
             where index = 0
         ),
     previous_trading_day as
         (
             select symbol, date
             from {{ ref('week_trading_sessions_static') }}
             where index = 1
         ),
     old_historical_prices as
         (
             select symbol
             from (
                      select symbol,
                             max(historical_prices.date) as date
                      from {{ ref('tickers') }}
                               left join {{ ref('historical_prices') }} using (symbol)
                      group by symbol
                  ) t
                      join {{ ref('tickers') }} using (symbol)
                      join latest_trading_day using (symbol)
                      join previous_trading_day using (symbol)
                      join {{ ref('ticker_metrics') }} using (symbol)
             where ticker_metrics.avg_volume_90d_money > 1000000
               and ticker_metrics.avg_volume_10d_money > 1000000
               and (t.date is null
                 or (now() > latest_trading_day.date + interval '1 day 6 hours' and t.date < latest_trading_day.date)
                 or (now() > previous_trading_day.date + interval '1 day 6 hours' and t.date < previous_trading_day.date))
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
       'Tickers ' || symbol || ' has old historical prices.' as message,
       now()                                                 as updated_at
from old_historical_prices
         join (select count(*) as cnt from old_historical_prices) t on true
where cnt > 20 -- if less than 20, consider them just being delisted
