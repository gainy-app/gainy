{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

select distinct on (
    upper(symbol)
    ) additional_notices,
      asset_platform_id,
      block_time_in_minutes,
      categories,
      coingecko_rank,
      coingecko_score,
      community_data,
      community_score,
      country_origin,
      description,
      developer_data,
      developer_score,
      genesis_date,
      hashing_algorithm,
      id,
      image,
      last_updated,
      links,
      liquidity_score,
      market_cap_rank,
      market_data,
      name,
      platforms,
      public_interest_score,
      public_interest_stats,
      public_notice,
      sentiment_votes_down_percentage,
      sentiment_votes_up_percentage,
      status_updates,
      (upper(symbol) || '.CC')::varchar as symbol,
      contract_address,
      ico_data
from {{ source('coingecko', 'coingecko_coin') }}
order by upper(symbol), coingecko_rank
