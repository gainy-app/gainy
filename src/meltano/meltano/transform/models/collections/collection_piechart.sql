{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


select profile_id,
       user_id,
       collection_id,
       collection_uniq_id,
       weight::double precision,
       'ticker'::varchar                             as entity_type,
       symbol                                        as entity_id,
       base_tickers.name                             as entity_name,
       ticker_realtime_metrics.absolute_daily_change as absolute_daily_change,
       ticker_realtime_metrics.relative_daily_change as relative_daily_change,
       actual_price * weight                         as absolute_value
from {{ ref('collection_ticker_actual_weights') }}
         join {{ ref('base_tickers') }} using (symbol)
         join {{ ref('ticker_realtime_metrics') }} using (symbol)
         left join {{ source('app', 'profiles') }} on profiles.id = profile_id

union all

(
    with collection_stats as
             (
                 select collection_uniq_id,
                        sum(weight) as weight_symbol_in_collection_sum
                 from {{ ref('collection_ticker_actual_weights') }}
                 group by collection_uniq_id
             ),
         collection_symbol_stats as
             (
                 select collection_uniq_id,
                        symbol,
                        sum(sim_dif + 1) as weight_category_in_symbol_sum
                 from {{ ref('collection_ticker_actual_weights') }}
                          join {{ ref('ticker_categories_continuous') }} using (symbol)
                 group by collection_uniq_id, symbol
             ),
         data as materialized
             (
                 select profile_id,
                        user_id,
                        collection_id,
                        collection_uniq_id,
                        category_id,
                        symbol,
                        weight / weight_symbol_in_collection_sum      as weight_symbol_in_collection,
                        (sim_dif + 1) / weight_category_in_symbol_sum as weight_category_in_symbol
                 from {{ ref('collection_ticker_actual_weights') }}
                          left join {{ source('app', 'profiles') }} on profiles.id = profile_id
                          join collection_stats using (collection_uniq_id)
                          join collection_symbol_stats using (collection_uniq_id, symbol)
                          join {{ ref('ticker_categories_continuous') }} using (symbol)
                 where weight_symbol_in_collection_sum > 0
                   and weight_category_in_symbol_sum > 0
             ),
         data2 as
             (
                 select profile_id,
                        user_id,
                        collection_id,
                        collection_uniq_id,
                        sum(weight_symbol_in_collection * weight_category_in_symbol)                         as weight,
                        category_id,
                        sum(weight_symbol_in_collection * weight_category_in_symbol * absolute_daily_change) as absolute_daily_change,
                        sum(weight_symbol_in_collection * weight_category_in_symbol * actual_price)          as actual_price,
                        sum(weight_symbol_in_collection * weight_category_in_symbol *
                            coalesce(previous_day_close_price, actual_price))                                as previous_day_close_price,
                        sum(weight_symbol_in_collection * weight_category_in_symbol * actual_price)          as absolute_value
                 from data
                          join {{ ref('ticker_realtime_metrics') }} using (symbol)
                 group by profile_id, user_id, collection_id, collection_uniq_id, category_id
                 having sum(weight_symbol_in_collection * weight_category_in_symbol) > 0
             )

    select profile_id,
           user_id,
           data2.collection_id,
           collection_uniq_id,
           weight::double precision,
           'category'::varchar        as entity_type,
           category_id::varchar       as entity_id,
           categories.name            as entity_name,
           absolute_daily_change::double precision,
           (case
               when previous_day_close_price > 0
                   then actual_price / previous_day_close_price - 1
               else 0
               end)::double precision as relative_daily_change,
           absolute_value::double precision
    from data2
             join {{ ref('categories') }} on data2.category_id = categories.id
)

union all

(
    with collection_stats as
             (
                 select collection_uniq_id,
                        sum(weight) as weight_symbol_in_collection_sum
                 from {{ ref('collection_ticker_actual_weights') }}
                 group by collection_uniq_id
             ),
         collection_symbol_stats as
             (
                 select collection_uniq_id,
                        symbol,
                        sum(sim_dif + 1) as weight_interest_in_symbol_sum
                 from {{ ref('collection_ticker_actual_weights') }}
                          join {{ ref('ticker_interests_continuous') }} using (symbol)
                 group by collection_uniq_id, symbol
             ),
         data as materialized
             (
                 select profile_id,
                        user_id,
                        collection_id,
                        collection_uniq_id,
                        interest_id,
                        symbol,
                        weight / weight_symbol_in_collection_sum      as weight_symbol_in_collection,
                        (sim_dif + 1) / weight_interest_in_symbol_sum as weight_interest_in_symbol
                 from {{ ref('collection_ticker_actual_weights') }}
                          left join {{ source('app', 'profiles') }} on profiles.id = profile_id
                          join collection_stats using (collection_uniq_id)
                          join collection_symbol_stats using (collection_uniq_id, symbol)
                          join {{ ref('ticker_interests_continuous') }} using (symbol)
                 where weight_symbol_in_collection_sum > 0
                   and weight_interest_in_symbol_sum > 0
             ),
         data2 as
             (
                 select profile_id,
                        user_id,
                        collection_id,
                        collection_uniq_id,
                        sum(weight_symbol_in_collection * weight_interest_in_symbol)                         as weight,
                        interest_id,
                        sum(weight_symbol_in_collection * weight_interest_in_symbol * absolute_daily_change) as absolute_daily_change,
                        sum(weight_symbol_in_collection * weight_interest_in_symbol * actual_price)          as actual_price,
                        sum(weight_symbol_in_collection * weight_interest_in_symbol *
                            coalesce(previous_day_close_price, actual_price))                                as previous_day_close_price,
                        sum(weight_symbol_in_collection * weight_interest_in_symbol * actual_price)          as absolute_value
                 from data
                          join {{ ref('ticker_realtime_metrics') }} using (symbol)
                 group by profile_id, user_id, collection_id, collection_uniq_id, interest_id
                 having sum(weight_symbol_in_collection * weight_interest_in_symbol) > 0
             )

    select profile_id,
           user_id,
           data2.collection_id,
           collection_uniq_id,
           weight::double precision,
           'interest'::varchar        as entity_type,
           interest_id::varchar       as entity_id,
           interests.name             as entity_name,
           absolute_daily_change::double precision,
           (case
               when previous_day_close_price > 0
                   then actual_price / previous_day_close_price - 1
               else 0
               end)::double precision as relative_daily_change,
           absolute_value::double precision
    from data2
             join {{ ref('interests') }} on data2.interest_id = interests.id
)
