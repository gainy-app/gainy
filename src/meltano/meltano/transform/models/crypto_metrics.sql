{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

select base_tickers.symbol,
       -- general
       country_origin,
       categories,
       asset_platform_id,
       hashing_algorithm,
       links,

       -- ranks / scores
       market_cap_rank::int,
       coingecko_rank::int,
       coingecko_score,
       public_interest_score,
       (public_interest_stats ->> 'alexa_rank')::int                               as alexa_rank,
       (public_interest_stats ->> 'bing_matches')::int                             as bing_matches,
       liquidity_score,
       sentiment_votes_up_percentage,
       sentiment_votes_down_percentage,

       -- ico
       (ico_data ->> 'ico_start_date')::date                                       as ico_start_date,
       (ico_data ->> 'ico_end_date')::date                                         as ico_end_date,

       -- image
       (image ->> 'large')::varchar                                                as image_large,
       (image ->> 'small')::varchar                                                as image_small,
       (image ->> 'thumb')::varchar                                                as image_thumb,

       -- community
       community_score                                                             as community_score,
       (community_data ->> 'facebook_likes')::int                                  as community_facebook_likes,
       (community_data ->> 'twitter_followers')::int                               as community_twitter_followers,
       (community_data ->> 'reddit_subscribers')::int                              as community_reddit_subscribers,
       (community_data ->> 'reddit_average_posts_48h')::double precision           as community_reddit_average_posts_48h,
       (community_data ->> 'reddit_accounts_active_48h')::int                      as community_reddit_accounts_active_48h,
       (community_data ->> 'reddit_average_comments_48h')::double precision        as community_reddit_average_comments_48h,
       (community_data ->> 'telegram_channel_user_count')::int                     as community_telegram_channel_user_count,

       -- development
       developer_score                                                             as development_score,
       (developer_data ->> 'forks')::int                                           as development_forks,
       (developer_data ->> 'stars')::int                                           as development_stars,
       (developer_data ->> 'subscribers')::int                                     as development_subscribers,
       (developer_data ->> 'total_issues')::int                                    as development_total_issues,
       (developer_data ->> 'closed_issues')::int                                   as development_closed_issues,
       (developer_data ->> 'commit_count_4_weeks')::int                            as development_commit_count_4_weeks,
       (developer_data ->> 'pull_requests_merged')::int                            as development_pull_requests_merged,
       (developer_data ->> 'pull_request_contributors')::int                       as development_pull_request_contributors,
       (developer_data -> 'code_additions_deletions_4_weeks' ->> 'additions')::int as development_code_additions_deletions_4_weeks_additions,
       (developer_data -> 'code_additions_deletions_4_weeks' ->> 'deletions')::int as development_code_additions_deletions_4_weeks_deletions,

       case
           when (market_data ->> 'mcap_to_tvl_ratio') not in ('?', '-')
               then (market_data ->> 'mcap_to_tvl_ratio')::double precision
           end                                                                     as market_mcap_to_tvl_ratio,
       case
           when (market_data ->> 'fdv_to_tvl_ratio') not in ('?', '-')
               then (market_data ->> 'fdv_to_tvl_ratio')::double precision
           end                                                                     as market_fdv_to_tvl_ratio,
       (market_data -> 'total_value_locked' ->> 'usd')::double precision           as market_total_value_locked,

       ticker_metrics.price_change_1w,
       ticker_metrics.price_change_1m,
       ticker_metrics.price_change_3m,
       ticker_metrics.price_change_1y,
       ticker_metrics.price_change_5y,
       ticker_metrics.price_change_all
from {{ ref('crypto_coins') }}
         join {{ ref('base_tickers') }} using(symbol)
         left join {{ ref('ticker_metrics') }} on ticker_metrics.symbol = base_tickers.symbol
where base_tickers.type = 'crypto'
