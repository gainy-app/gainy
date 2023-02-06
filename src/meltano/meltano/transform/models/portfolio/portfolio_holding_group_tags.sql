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


with holding_group_collection_tags as
         (
             select portfolio_holding_group_details.holding_group_id,
                    portfolio_holding_group_details.profile_id,
                    portfolio_holding_group_details.ticker_symbol as symbol,
                    collection_match_score_explanation.category_id,
                    collection_match_score_explanation.interest_id
             from {{ ref('portfolio_holding_group_details') }}
                      join {{ ref('collection_match_score_explanation') }} using (profile_id, collection_id)
         ),
     all_rows as
         (
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
                    ticker_categories.category_id,
                    null::int                                           as interest_id,
                    0                                                   as priority,
                    row_number() over (partition by profile_id, symbol) as row_num
             from {{ ref('portfolio_holding_group_details') }}
                      join {{ ref('ticker_categories') }}
                           on ticker_categories.symbol = portfolio_holding_group_details.ticker_symbol

             union all

             select holding_group_id,
                    profile_id,
                    symbol,
                    null::int                                           as collection_id,
                    null::text                                          as collection_uniq_id,
                    null::int                                           as category_id,
                    ticker_interests.interest_id,
                    0                                                   as priority,
                    row_number() over (partition by profile_id, symbol) as row_num
             from {{ ref('portfolio_holding_group_details') }}
                      join {{ ref('ticker_interests') }}
                           on ticker_interests.symbol = portfolio_holding_group_details.ticker_symbol
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
       coalesce(all_rows.interest_id, 0))                                                                     as id
from all_rows
