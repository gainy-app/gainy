{{
  config(
    materialized = "view",
  )
}}

select *,
       user_id
from (
         select profile_id,
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

         union all

         (
             with ticker_categories_continuous_normalized as
                      (
                          select symbol,
                                 category_id,
                                 (sim_dif + 1) / 2 / sim_dif_norm_sum as category_in_symbol_weight
                          from {{ ref('ticker_categories_continuous') }}
                                   join (
                                            select symbol, sum((sim_dif + 1) / 2) as sim_dif_norm_sum
                                            from {{ ref('ticker_categories_continuous') }}
                                            where sim_dif > 0
                                            group by symbol
                                        ) collection_categories_stats using (symbol)
                          where sim_dif > 0
                      ),
                  data as
                      (
                          select profile_id,
                                 collection_id,
                                 collection_uniq_id,
                                 category_id,
                                 sum(weight * coalesce(category_in_symbol_weight, 1))                         as weight,
                                 sum(weight * coalesce(category_in_symbol_weight, 1) * absolute_daily_change) as absolute_daily_change,
                                 sum(weight * coalesce(category_in_symbol_weight, 1) * actual_price)          as absolute_value,
                                 sum(weight * coalesce(category_in_symbol_weight, 1) * relative_daily_change) as relative_daily_change
                          from {{ ref('collection_ticker_actual_weights') }}
                                   left join ticker_categories_continuous_normalized using (symbol)
                                   left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                          group by collection_uniq_id, collection_id, category_id, profile_id
                  )
             select profile_id,
                    data.collection_id,
                    collection_uniq_id,
                    weight::double precision,
                    'category'::varchar     as entity_type,
                    category_id::varchar    as entity_id,
                    coalesce(name, 'Other') as entity_name,
                    absolute_daily_change::double precision,
                    relative_daily_change::double precision,
                    absolute_value::double precision
             from data
                      left join {{ ref('categories') }} on data.category_id = categories.id
         )

         union all

         (
             with ticker_interests_continuous_normalized as
                      (
                          select symbol,
                                 interest_id,
                                 (sim_dif + 1) / 2 / sim_dif_norm_sum as interest_in_symbol_weight
                          from {{ ref('ticker_interests_continuous') }}
                                   join (
                                            select symbol, sum((sim_dif + 1) / 2) as sim_dif_norm_sum
                                            from {{ ref('ticker_interests_continuous') }}
                                            where sim_dif > 0
                                            group by symbol
                                        ) collection_interests_stats using (symbol)
                          where sim_dif > 0
                      ),
                  data as
                      (
                          select profile_id,
                                 collection_id,
                                 collection_uniq_id,
                                 interest_id,
                                 sum(weight * coalesce(interest_in_symbol_weight, 1))                         as weight,
                                 sum(weight * coalesce(interest_in_symbol_weight, 1) * absolute_daily_change) as absolute_daily_change,
                                 sum(weight * coalesce(interest_in_symbol_weight, 1) * actual_price)          as absolute_value,
                                 sum(weight * coalesce(interest_in_symbol_weight, 1) * relative_daily_change) as relative_daily_change
                          from {{ ref('collection_ticker_actual_weights') }}
                                   left join ticker_interests_continuous_normalized using (symbol)
                                   left join {{ ref('ticker_realtime_metrics') }} using (symbol)
                          group by collection_uniq_id, collection_id, interest_id, profile_id
                  )
             select profile_id,
                    data.collection_id,
                    collection_uniq_id,
                    weight::double precision,
                    'interest'::varchar     as entity_type,
                    interest_id::varchar    as entity_id,
                    coalesce(name, 'Other') as entity_name,
                    absolute_daily_change::double precision,
                    relative_daily_change::double precision,
                    absolute_value::double precision
             from data
                      left join {{ ref('interests') }} on data.interest_id = interests.id
         )
     ) t
         left join {{ source('app', 'profiles') }} on profiles.id = profile_id
