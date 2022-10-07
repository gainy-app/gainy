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
                      left join {{ ref('ticker_interests') }} using (symbol)
                      left join {{ ref('interests') }} on interests.id = ticker_interests.interest_id
             where interests.id is null

             union all

             select symbol,
                   'ttf_ticker_no_industry' as code,
                   'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('base_tickers') }} using (symbol)
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null
               and (base_tickers is null or base_tickers.type != 'etf')

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
                      left join {{ ref('ticker_risk_scores') }} using (symbol)
             where ticker_risk_scores.symbol is null
           
             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_category_continuous' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join {{ ref('ticker_categories_continuous') }} using (symbol)
             where ticker_categories_continuous.symbol is null

             union all
           
             select collection_distinct_tickers.symbol,
                    'ttf_ticker_no_matchscore' as code,
                    'daily' as period
             from collection_distinct_tickers
                      left join matchscore_distinct_tickers using (symbol)
                               left join {{ ref('tickers') }} using (symbol)
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
                 where ticker_collections_weights is null
                   and ticker_collections is null

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
                       and (collection_metrics is null
                         or collection_daily_latest_chart_point is null
                         or collection_metrics.previous_day_close_price < 1e-6
                         or abs(collection_daily_latest_chart_point.adjusted_close / collection_metrics.previous_day_close_price - 1) > 0.2)
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

-- add one fake record to allow post_hook above to clean other rows
select 'fake_row_allowing_deletion' as id,
       null                         as symbol,
       null                         as code,
       period,
       null                         as message,
       now()                        as updated_at
from (values('daily', 'realtime')) t(period)