{% set count = 100 %}

{{
  config(
    materialized = "view",
  )
}}


with all_ticker_collections as
         (
             select profile_favorite_collections.profile_id,
                    profile_favorite_collections.collection_id,
                    coalesce(ticker_collections.symbol, personalized_ticker_collections.symbol) as symbol
             from {{ source('app', 'profile_favorite_collections') }}
                      left join {{ ref('ticker_collections') }}
                                on ticker_collections.collection_id = profile_favorite_collections.collection_id
                      left join {{ source('app', 'personalized_ticker_collections') }}
                                on personalized_ticker_collections.collection_id = profile_favorite_collections.collection_id
                                    and personalized_ticker_collections.profile_id = profile_favorite_collections.profile_id
             where ticker_collections.symbol is not null
                or personalized_ticker_collections.symbol is not null
         ),
     unique_profile_collection_tickers as
         (
             select distinct on (profile_id, symbol) profile_id, symbol
             from all_ticker_collections
         ),
     ranked_profile_collection_tickers as
         (
             select profile_id,
                    unique_profile_collection_tickers.symbol,
                    ticker_realtime_metrics.relative_daily_change,
                    ticker_realtime_metrics.time                                              as updated_at,
                    rank() over (partition by profile_id order by relative_daily_change desc) as gainer_rank,
                    rank() over (partition by profile_id order by relative_daily_change)      as loser_rank
             from unique_profile_collection_tickers
                      join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = unique_profile_collection_tickers.symbol
         )
select profile_id,
       symbol,
       relative_daily_change,
       updated_at,
       gainer_rank::int,
       loser_rank::int
from ranked_profile_collection_tickers
where gainer_rank <= {{ count }} or loser_rank <= {{ count }}
