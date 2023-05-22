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
      'delete from {{this}} where id = \'fake_row_allowing_deletion\'',
    ]
  )
}}


with collection_distinct_tickers as
         (
             select distinct symbol
             from {{ ref('collection_ticker_actual_weights') }}
                      join {{ ref('collections') }}
                           on collection_ticker_actual_weights.collection_id = collections.id
             where collections.enabled = '1'
               and collections.personalized = '0'
         ),
     matchscore_distinct_tickers as
         (
             select symbol
             from {{ source('app', 'profile_ticker_match_score') }}
             group by symbol
         ),
     errors as
         (
             select symbol,
                    'ttf_ticker_no_interest' as code,
                    'daily' as period
             from collection_distinct_tickers
                      join {{ ref('tickers') }} using (symbol)
                      left join {{ ref('ticker_interests') }} using (symbol)
                      left join {{ ref('interests') }} on interests.id = ticker_interests.interest_id
             where interests.id is null

             union all

             select symbol,
                   'ttf_ticker_no_industry' as code,
                   'daily' as period
             from collection_distinct_tickers
                      join {{ ref('tickers') }} using (symbol)
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null
               and (tickers.type is null or tickers.type != 'etf')

             union all

             select collection_distinct_tickers.symbol,
                    'ttf_ticker_hidden' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('tickers') }} using (symbol)
             where tickers.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_risk_score' as code,
                    'daily' as period
             from collection_distinct_tickers
                      join {{ ref('tickers') }} using (symbol)
                      left join {{ ref('ticker_risk_scores') }} using (symbol)
             where ticker_risk_scores.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_category_continuous' as code,
                    'daily' as period
             from collection_distinct_tickers
                      join {{ ref('tickers') }} using (symbol)
                      left join {{ ref('ticker_categories_continuous') }} using (symbol)
             where ticker_categories_continuous.symbol is null

             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_matchscore' as code,
                    'daily' as period
             from collection_distinct_tickers
                      join {{ ref('tickers') }} using (symbol)
                      left join matchscore_distinct_tickers using (symbol)
             where matchscore_distinct_tickers.symbol is null
         )
select (code || '_' || symbol) as id,
       symbol,
       code,
       period,
       case
           when code = 'ttf_ticker_no_interest'
               then 'TTF ticker ' || symbol || ' is not linked to any interest.'
           when code = 'ttf_ticker_no_industry'
               then 'TTF ticker ' || symbol || ' is not linked to any industry.'
           when code = 'ttf_ticker_hidden'
               then 'TTF ticker ' || symbol || ' not present in the tickers table.'
           when code = 'ttf_ticker_no_risk_score'
               then 'TTF ticker ' || symbol || ' not present in the ticker_risk_scores table.'
           when code = 'ttf_ticker_no_category_continuous'
               then 'TTF ticker ' || symbol || ' not present in the ticker_categories_continuous table.'
           when code = 'ttf_ticker_no_matchscore'
               then 'TTF ticker ' || symbol || ' not present in the app.profile_ticker_match_score table.'
           when code = 'ttf_ticker_no_matchscore'
               then 'TTF ticker ' || symbol || ' not present in the app.profile_ticker_match_score table.'
           end                 as message,
       now()                   as updated_at
from errors

union all

(
    with gainy_collections as
             (
                 select *,
                        '0_' || gainy_collections.id as collection_uniq_id
                 from {{ source('gainy', 'gainy_collections') }}
                 where gainy_collections.enabled = '1'
                   and gainy_collections._sdc_extracted_at > (select max(_sdc_extracted_at)from raw_data.gainy_collections) - interval '1 minute'
             ),
         errors as
             (
                 select distinct on (
                     gainy_collections.id
                     ) gainy_collections.id                                 as collection_id,
                       'ttf_no_weights'                                     as code,
                       'TTF ' || gainy_collections.id || ' has no weights.' as message,
                       'daily'                                              as period
                 from gainy_collections
                          left join {{ source('gainy', 'ticker_collections') }}
                                    on ticker_collections.ttf_name = gainy_collections.name
                          left join {{ source('gainy', 'ticker_collections_weights') }}
                                    on ticker_collections_weights.ttf_name = gainy_collections.name
                 where ticker_collections_weights.ttf_name is null
                   and ticker_collections.ttf_name is null

                 union all

                 (
                     with collection_daily_latest_chart_point as
                              (
                                  select collection_chart.*,
                                         row_number() over (partition by collection_uniq_id order by t.date desc) as idx
                                  from (
                                           select collection_uniq_id, period, date, max(datetime) as datetime
                                           from {{ ref('collection_chart') }}
                                           where period = '1w'
                                           group by collection_uniq_id, period, date
                                       ) t
                                           join {{ ref('collection_chart') }} using (collection_uniq_id, period, datetime)
                              )
                     select distinct on (
                         gainy_collections.id
                         ) gainy_collections.id                                          as collection_id,
                           'ttf_wrong_previous_day_close_price'                          as code,
                           'TTF ' || gainy_collections.id || ' has wrong previous_day_close_price. ' ||
                           json_build_array(collection_metrics.previous_day_close_price,
                                            collection_daily_latest_chart_point.adjusted_close) as message,
                           'daily'                                                       as period
                     from gainy_collections
                              join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
                              left join {{ ref('collection_metrics') }} using (collection_uniq_id)
                              left join collection_daily_latest_chart_point using (collection_uniq_id)
                     where profile_collections.enabled = '1'
                       and (collection_metrics.collection_uniq_id is null
                         or collection_daily_latest_chart_point.collection_uniq_id is null
                         or collection_metrics.previous_day_close_price < {{ var('price_precision') }}
                         or abs(collection_daily_latest_chart_point.adjusted_close / collection_metrics.previous_day_close_price - 1) > 0.2)
                 )

                 union all

                 (
                     with wrong_collection_ticker_weights as
                              (
                                  select collection_uniq_id,
                                         json_agg(json_build_array(date, weight_sum)) as data
                                  from (
                                           select collection_uniq_id, date, sum(weight) as weight_sum
                                           from {{ ref('collection_ticker_weights') }}
                                           group by collection_uniq_id, date
                                           having abs(sum(weight) - 1) > {{ var('weight_precision') }} * count(collection_uniq_id)
                                       ) t
                                  group by collection_uniq_id
                              )
                     select gainy_collections.id                                                     as collection_id,
                            'collection_tickers_wrong_weight_sum'                                    as code,
                            'TTF ' || gainy_collections.id || ' has wrong weights at dates ' || data as message,
                            'daily'                                                                  as period
                     from wrong_collection_ticker_weights
                              join gainy_collections using (collection_uniq_id)
                 )
             )
    select (code || '_' || collection_id) as id,
           null                           as symbol,
           code,
           period,
           message,
           now()                          as updated_at
    from errors
)

union all

(
     with collection_ticker_weights_expanded as
              (

                  select collection_id,
                         symbol,
                         date,
                         weight,
                         price,
                         prev_weight,
                         prev_weight / prev_price * price as new_weight,
                         rnk = 1 as is_first_day_after_rebalance
                  from (
                           select *,
                                  rank() over wnd2 as rnk,
                                  lag(weight) over wnd as prev_weight,
                                  lag(price) over wnd  as prev_price
                           from {{ ref('collection_ticker_weights') }}
                               window wnd as (partition by collection_id, symbol order by date),
                                   wnd2 as (partition by collection_id, symbol, period_id order by date)
                       ) t
          )
     select ('collection_tickers_wrong_weight_' || collection_id ||
             '_' || symbol || '_' || date)         as id,
            symbol,
            'collection_tickers_wrong_weight',
            'daily'                                as period,
            json_build_array(weight,
                new_weight / new_weight_sum)::text as message,
            now()                                  as updated_at
     from collection_ticker_weights_expanded
              join (
                       select *,
                              lag(symbols_cnt) over (partition by collection_id order by date) as prev_symbols_cnt
                       from (
                                select collection_id, date, count(*) as symbols_cnt, sum(new_weight) as new_weight_sum
                                from collection_ticker_weights_expanded
                                group by collection_id, date
                            ) t
                   ) t using (collection_id, date)
     where abs(weight - new_weight / new_weight_sum) > {{ var('weight_precision') }} * symbols_cnt
       and not is_first_day_after_rebalance
       and t.symbols_cnt = t.prev_symbols_cnt
)

union all

-- add one fake record to allow post_hook above to clean other rows
select 'fake_row_allowing_deletion' as id,
       null                         as symbol,
       null                         as code,
       period,
       null                         as message,
       now()                        as updated_at
from (values('daily', 'realtime')) t(period)