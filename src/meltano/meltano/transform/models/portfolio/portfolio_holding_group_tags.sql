{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      'create index if not exists "profile_id__symbol" ON {{ this }} (profile_id, symbol)',
      'delete from {{ this }} where updated_at < (select max(updated_at) from {{ this }} where is_realtime = false)',
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
                      join {{ ref('ticker_collections') }} on ticker_collections.collection_id = profile_collections.id
                      join {{ ref('collection_metrics') }}
                           on collection_metrics.collection_uniq_id = profile_collections.uniq_id
             where profile_collections.personalized = '0'
             order by symbol, collection_metrics.market_capitalization_sum desc
         ),
     holding_group_collection_tags as
         (
             select profile_id,
                    ticker_symbol as symbol,
                    collection_match_score_explanation.collection_id,
                    collection_match_score_explanation.collection_uniq_id,
                    collection_match_score_explanation.category_id,
                    collection_match_score_explanation.interest_id
             from {{ ref('portfolio_holding_group_details') }}
                      left join ticker_selected_collection
                                on ticker_selected_collection.symbol =
                                   portfolio_holding_group_details.ticker_symbol
                      left join {{ ref('collection_match_score_explanation') }} using (profile_id, collection_id)
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
             select profile_id,
                    symbol,
                    collection_id,
                    collection_uniq_id,
                    category_id,
                    interest_id
             from holding_group_collection_tags
             where holding_group_collection_tags.collection_id is not null

             union all

             select profile_id,
                    symbol,
                    collection_id,
                    collection_uniq_id,
                    ticker_tags_ranked.category_id,
                    ticker_tags_ranked.interest_id
             from holding_group_collection_tags
                      join ticker_tags_ranked using (symbol)
             where holding_group_collection_tags.collection_id is null
               and row_num <= 5
         )
select *,
       now()                      as updated_at,
       (profile_id || '_' ||
        symbol || '_' ||
        coalesce(collection_id, 0) || '_' ||
        coalesce(category_id, 0) || '_' ||
        coalesce(interest_id, 0)) as id,
       {{ var('realtime') }}      as is_realtime
from all_rows
{% if is_incremental() and var('realtime') %}
         left join {{ this }} old_data using (profile_id, symbol)
where old_data is null
{% endif %}