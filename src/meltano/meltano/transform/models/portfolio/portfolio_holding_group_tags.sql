{{
  config(
    materialized = "table",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      index('holding_group_id'),
    ]
  )
}}


with ticker_selected_collection as
         (
             select distinct on (
                 symbol
                 ) symbol,
                   collection_id
             from {{ ref('profile_collections') }}
                      join {{ ref('collection_ticker_actual_weights') }}
                           on collection_ticker_actual_weights.collection_id = profile_collections.id
                      join {{ ref('collection_metrics') }}
                           on collection_metrics.collection_uniq_id = profile_collections.uniq_id
             where profile_collections.personalized = '0'
             order by symbol, collection_metrics.market_capitalization_sum desc
         ),
     holding_group_collections as
         (
             select t.*
             from (-- tickers
                      select portfolio_holding_group_details.holding_group_id,
                             portfolio_holding_group_details.profile_id,
                             portfolio_holding_group_details.ticker_symbol as symbol,
                             profile_ticker_collections.collection_id,
                             profile_ticker_collections.collection_uniq_id
                      from {{ ref('portfolio_holding_group_details') }}
                               join {{ ref('profile_ticker_collections') }}
                                    on profile_ticker_collections.symbol = portfolio_holding_group_details.ticker_symbol
                                        and (profile_ticker_collections.profile_id =
                                             portfolio_holding_group_details.profile_id or
                                             profile_ticker_collections.profile_id is null)

                      union all

                      -- ttfs
                      select portfolio_holding_group_details.holding_group_id,
                             portfolio_holding_group_details.profile_id,
                             portfolio_holding_group_details.ticker_symbol as symbol,
                             portfolio_holding_group_details.collection_id,
                             profile_collections.uniq_id                   as collection_uniq_id
                      from {{ ref('portfolio_holding_group_details') }}
                               join {{ ref('profile_collections') }}
                                    on profile_collections.id = portfolio_holding_group_details.collection_id
                                        and (profile_collections.profile_id = portfolio_holding_group_details.profile_id or profile_collections.profile_id is null)
                  ) t
                      join {{ ref('collection_metrics') }} using (collection_uniq_id)
             order by holding_group_id, collection_metrics.market_capitalization_sum desc
     ),
     holding_group_collection_tags as
         (
             select portfolio_holding_group_details.holding_group_id,
                    portfolio_holding_group_details.profile_id,
                    portfolio_holding_group_details.ticker_symbol as symbol,
                    collection_match_score_explanation.category_id,
                    collection_match_score_explanation.interest_id
             from {{ ref('portfolio_holding_group_details') }}
                      left join ticker_selected_collection
                                on ticker_selected_collection.symbol = portfolio_holding_group_details.ticker_symbol
                      join {{ ref('collection_match_score_explanation') }}
                           on collection_match_score_explanation.profile_id = portfolio_holding_group_details.profile_id
                               and collection_match_score_explanation.collection_id in
                                   (portfolio_holding_group_details.collection_id,
                                    ticker_selected_collection.collection_id)
     ),
     ticker_tags_ranked as
         (
             select *,
                    row_number() over (partition by symbol order by sim_dif desc) as row_num
             from (
                      select symbol,
                             category_id,
                             null as interest_id,
                             sim_dif
                      from {{ ref('ticker_categories_continuous') }}

                      union all

                      select symbol,
                             null as category_id,
                             ticker_interests.interest_id,
                             sim_dif
                      from {{ ref('ticker_interests') }}
                  ) t
     ),
     all_rows as
         (
             select holding_group_id,
                    profile_id,
                    symbol,
                    collection_id,
                    collection_uniq_id,
                    null::int                                           as category_id,
                    null::int                                           as interest_id,
                    1                                                   as priority,
                    row_number() over (partition by profile_id, symbol) as row_num
             from holding_group_collections

             union all

             select holding_group_id,
                    profile_id,
                    symbol,
                    null::int                                           as collection_id,
                    null::text                                          as collection_uniq_id,
                    category_id,
                    interest_id,
                    0                                                   as priority,
                    row_number() over (partition by profile_id, symbol) as row_num
             from holding_group_collection_tags

             union all

             select holding_group_id,
                    profile_id,
                    symbol,
                    null::int                                           as collection_id,
                    null::text                                          as collection_uniq_id,
                    ticker_tags_ranked.category_id,
                    ticker_tags_ranked.interest_id,
                    0                                                   as priority,
                    row_number() over (partition by profile_id, symbol) as row_num
             from {{ ref('portfolio_holding_group_details') }}
                      join ticker_tags_ranked
                                on ticker_tags_ranked.symbol = portfolio_holding_group_details.ticker_symbol
             where row_num <= 5
     )
select distinct on (
    all_rows.holding_group_id,
    all_rows.collection_id,
    all_rows.category_id,
    all_rows.interest_id
    ) all_rows.holding_group_id,
      all_rows.profile_id,
      all_rows.symbol,
      all_rows.collection_id,
      all_rows.collection_uniq_id,
      all_rows.category_id,
      all_rows.interest_id,
      -(row_number()
        over (partition by all_rows.holding_group_id order by all_rows.priority desc, all_rows.row_num))::int as priority,
      now()                                                                                                   as updated_at,
      (all_rows.holding_group_id || '_' ||
       coalesce(all_rows.collection_id, 0) || '_' ||
       coalesce(all_rows.category_id, 0) || '_' ||
       coalesce(all_rows.interest_id, 0))                                                                     as id,
       {{ var('realtime') }}                                                                                  as is_realtime
from all_rows
