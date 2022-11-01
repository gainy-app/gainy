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
                  allowable_sigma_dev_adjclose_percchange, symbol_asmarket_default) as
         (
             values ('public_*.collection_historical_values', interval '1 day',
                     interval '5 year', now()::timestamp - interval '5 year',
                     interval '5 year', now()::timestamp - interval '5 year',
                     15., 'SPY')
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
             select unnest(array [cp.symbol_asmarket_default]) as intrpl_symbol_asmarket,
                    d.dt::date                                 as date,
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
                                    select symbol as intrpl_symbol_asmarket,
                                           date,
                                           adjusted_close
                                    from {{ ref('historical_prices') }} ehp
                                             join check_params cp on true
                                    where symbol = any (array [cp.symbol_asmarket_default])
                                ) as t using (intrpl_symbol_asmarket, date)
                 window
                     lookback as (partition by d.intrpl_symbol_asmarket order by date asc),
                     lookforward as (partition by d.intrpl_symbol_asmarket order by date desc)
     ),

     collections_data as
         (
             select collection_uniq_id,
                    date,
                    value,
                    symbol_asmarket_default as intrpl_symbol_asmarket
             from {{ ref('collection_historical_values') }} ehp
                      left join check_params cp on true
             where date >= depth_stddev_threshold
     ),

     collections_lag as
         (
             select t.*,
                    lag(t.value) over (partition by t.collection_uniq_id order by t.date) as value_pre,
                    sam.intrpl_symbol_asmarket_adjusted_close,
                    lag(sam.intrpl_symbol_asmarket_adjusted_close)
                    over (partition by t.collection_uniq_id order by t.date)              as intrpl_symbol_asmarket_adjusted_close_pre
             from collections_data t
                      join intrpl_symbol_asmarket_dt_prices sam using (intrpl_symbol_asmarket, date)
     ), -- select * from collections_lag;

     collections_difs as
         (
             select *,
                    value - value_pre                                              as value_dif,
                    (value - value_pre)
                        / (1e-30 + abs(value_pre))                                 as value_perc_change,
                    (value - value_pre) / (1e-30 + abs(value_pre))
                        - (intrpl_symbol_asmarket_adjusted_close - intrpl_symbol_asmarket_adjusted_close_pre)
                        / (1e-30 + abs(intrpl_symbol_asmarket_adjusted_close_pre)) as value_perc_change_wom
             from collections_lag
     ), -- select * from collections_difs;

     collection_stddevs_means_devs as
         (
             select *,
                    stddev_pop(value_perc_change_wom) over (w_s) as stddev_value_perc_change_wom,
                    avg(value_perc_change_wom) over (w_s)        as mean_value_perc_change_wom,
                    abs(value_perc_change_wom
                        - avg(value_perc_change_wom) over (w_s)) as dev_value_perc_change_wom
             from collections_difs
                 window w_s as (partition by collection_uniq_id)
     ), -- select * from collection_stddevs_means_devs where collection_uniq_id = '0_115' and date between '2020-11-22' and '2020-11-26';

     collection_checks as materialized
         (
             select *,
                    (
                            dev_value_perc_change_wom >
                            stddev_value_perc_change_wom * cp.allowable_sigma_dev_adjclose_percchange
                        )::int               as iserror_value_perc_change_dev_wom,
                    (value = value_pre)::int as iserror_value_twice_same,
                    (value <= 0)::int        as iserror_value_is_notpositive,
                    (value is null)::int     as iserror_value_is_null
             from collection_stddevs_means_devs
                      left join check_params cp on true
     ), -- select * from collection_checks;

     collection_checks_verbose_union as
         (
             (
                 select collection_uniq_id,
                        table_name || '__value_perc_change_dev_wom' as code,
                        'daily'                                     as "period",
                        json_build_object(
                                'table_name', table_name,
                                'stddev_threshold',
                                max(allowable_sigma_dev_adjclose_percchange * stddev_value_perc_change_wom),
                                'mu', max(mean_value_perc_change_wom),
                                'stddev', max(stddev_value_perc_change_wom),
                                'value', max(dev_value_perc_change_wom),
                                'date', max(date)
                            )::text                                 as message
                 from collection_checks
                 where iserror_value_perc_change_dev_wom > 0
                 group by collection_uniq_id, table_name
             )
             union all
             (
                 select collection_uniq_id,
                        table_name || '__value_twice_same'                                     as code,
                        'daily'                                                                as "period",
                        'Collection ' || collection_uniq_id || ' has ' || sum(iserror_value_twice_same) ||
                        ' pairs of consecutive rows with same price. Example at ' || max(date) as message
                 from collection_checks
                 where iserror_value_twice_same > 0
                 group by collection_uniq_id, table_name
             )
             union all
             (
                 select distinct on (
                     collection_uniq_id
                     ) -- agg >0 && distinct on (collection_uniq_id) -> example case
                       collection_uniq_id,
                       table_name || '__value_is_notpositive'                            as code,
                       'daily'                                                           as "period",
                       'Collection ' || collection_uniq_id || ' has ' ||
                       iserror_value_is_notpositive ||
                       ' rows with value not positive values (<=0). Example at ' || date as message
                 from collection_checks
                 where iserror_value_is_notpositive > 0
                 order by collection_uniq_id, date desc
             )
             union all
             (
                 select distinct on (
                     collection_uniq_id
                     ) -- agg >0 && distinct on (collection_uniq_id) -> example case
                       collection_uniq_id,
                       table_name || '__value_is_null'                       as code,
                       'daily'                                               as "period",
                       'Collection ' || collection_uniq_id || ' has ' || iserror_value_is_null ||
                       ' rows with value = NULL values. Example at ' || date as message
                 from collection_checks
                 where iserror_value_is_null > 0
                 order by collection_uniq_id, date desc
             )
     ),
     latest_trading_day as
         (
             select collection_uniq_id,
                    max(week_trading_sessions_static.date) as date
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('week_trading_sessions_static') }} using (symbol)
             where index = 0
             group by collection_uniq_id
     ),
     previous_trading_day as
         (
             select collection_uniq_id,
                    max(week_trading_sessions_static.date) as date
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('week_trading_sessions_static') }} using (symbol)
             where index = 1
             group by collection_uniq_id
     ),
     old_collection_historical_values as
         (
             select collection_uniq_id
             from (
                      select collection_uniq_id,
                             max(collection_historical_values.date) as date
                      from {{ ref('collection_historical_values') }}
                      group by collection_uniq_id
                  ) t
                      join latest_trading_day using (collection_uniq_id)
                      join previous_trading_day using (collection_uniq_id)
             where t.date is null
                or (now() > latest_trading_day.date + interval '1 day 6 hours' and t.date < latest_trading_day.date)
                or (now() > previous_trading_day.date + interval '1 day 6 hours' and t.date < previous_trading_day.date)
     )
select code || '__' || collection_uniq_id as id,
       null                               as symbol,
       code,
       "period",
       message,
       now()                              as updated_at
from collection_checks_verbose_union

union all

select 'old_collection_historical_values_' || collection_uniq_id            as id,
       null                                                                 as symbol,
       'old_collection_historical_values'                                   as code,
       'daily'                                                              as period,
       'Collection ' || collection_uniq_id || ' has old historical prices.' as message,
       now()                                                                as updated_at
from old_collection_historical_values
