{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


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
from {{ ref('collection_tickers_weighted') }}
         join {{ ref('base_tickers') }} using (symbol)
         join {{ ref('ticker_realtime_metrics') }} using (symbol)
union all
select profile_id,
       t.collection_id,
       collection_uniq_id,
       weight::double precision,
       'category'::varchar                                                                         as entity_type,
       category_id::varchar                                                                        as entity_id,
       categories.name                                                                             as entity_name,
       absolute_daily_change                                                                       as absolute_daily_change,
       (actual_price / case when prev_close_price > 0 then prev_close_price end)::double precision as relative_daily_change,
       actual_price                                                                                as absolute_value
from (
         select profile_id,
                collection_id,
                collection_uniq_id,
                category_id,
                sum(weight)                                                      as weight,
                sum(actual_price * weight)                                       as actual_price,
                sum(absolute_daily_change * weight)::double precision            as absolute_daily_change,
                sum(actual_price * weight) - sum(absolute_daily_change * weight) as prev_close_price
         from {{ ref('collection_tickers_weighted') }}
                  join {{ ref('ticker_categories') }} using (symbol)
                  join {{ ref('ticker_realtime_metrics') }} using (symbol)
         group by profile_id, collection_id, collection_uniq_id, ticker_categories.category_id
     ) t
         join {{ ref('categories') }} on t.category_id = categories.id
union all
select profile_id,
       collection_id,
       collection_uniq_id,
       weight::double precision,
       'interest'::varchar                                                                         as entity_type,
       interest_id::varchar                                                                        as entity_id,
       interests.name                                                                              as entity_name,
       absolute_daily_change                                                                       as absolute_daily_change,
       (actual_price / case when prev_close_price > 0 then prev_close_price end)::double precision as relative_daily_change,
       actual_price                                                                                as absolute_value
from (
         select profile_id,
                collection_id,
                collection_uniq_id,
                interest_id,
                sum(weight)                                                      as weight,
                sum(actual_price * weight)                                       as actual_price,
                sum(absolute_daily_change * weight)::double precision            as absolute_daily_change,
                sum(actual_price * weight) - sum(absolute_daily_change * weight) as prev_close_price
         from {{ ref('collection_tickers_weighted') }}
                  join {{ ref('ticker_interests') }} using (symbol)
                  join {{ ref('ticker_realtime_metrics') }} using (symbol)
         group by profile_id, collection_id, collection_uniq_id, ticker_interests.interest_id
     ) t
         join interests on t.interest_id = interests.id
