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
                      left join {{ ref('ticker_industries') }} using (symbol)
                      left join {{ ref('gainy_industries') }} on gainy_industries.id = ticker_industries.industry_id
             where gainy_industries.id is null

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
             and tickers.type <> 'crypto'
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
    with errors as
             (
                 select distinct on (
                     gainy_collections.id
                     ) gainy_collections.id as collection_id,
                       'ttf_no_weights'     as code,
                       'daily'              as period
                 from {{ source('gainy', 'gainy_collections') }}
                          left join {{ source('gainy', 'ticker_collections') }}
                                    on ticker_collections.ttf_name = gainy_collections.name
                          left join {{ source('gainy', 'ticker_collections_weights') }}
                                    on ticker_collections_weights.ttf_name = gainy_collections.name
                 where ticker_collections_weights is null
                    and ticker_collections is null
             )
    select (code || '_' || collection_id) as id,
           null                           as symbol,
           code,
           period,
           case
               when code = 'ttf_no_weights'
                   then 'TTF ' || collection_id || ' has no weights.'
               end                        as message,
           now()                          as updated_at
    from errors
)