{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

-- Daily movers in your TTFs or portfolio during the trading day
-- 1) 10 am daily 2) The 3 biggest gainers and losers in TTFs or portfolio you follow
-- 6 biggest changes in your portfolio[better do it after market opens ~10am EST]
-- Two separate notifications
-- / Top losers: +3% TSLA, +5% MDB ...
with individual_notifications as
         (
             select email,
                    ('top_gainers_' || (string_agg(symbol, '_' order by relative_daily_change desc, symbol)) ||
                     '_' || now()::date)::varchar                                                                 as uniq_id,
                    min(exchange_schedule.open_at) + interval '30 minutes'                                        as send_at,
                    json_build_object('en', 'Top gainers: ' ||
                                            (string_agg(text, ', ' order by relative_daily_change desc, symbol))) as text,
                    json_build_object('t', 0)                                                                     as data
             from (
                      select relative_daily_change,
                             email,
                             symbol,
                             '+' || round(relative_daily_change * 100) || '% ' || symbol as text
                      from {{ source('app', 'profiles')}}
                               join {{ ref('profile_collection_tickers_performance_ranked') }}
                                    on profile_collection_tickers_performance_ranked.profile_id = profiles.id
                                        and gainer_rank <= 3
                      where relative_daily_change > 0.01
                  ) t
                      join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and
                                                exchange_schedule.date = now()::date
-- where now() > exchange_schedule.open_at
             group by email

             union all

             select email,
                    ('top_losers_' || (string_agg(symbol, '_' order by relative_daily_change, symbol)) ||
                     '_' || now()::date)::varchar                                                                 as uniq_id,
                    min(exchange_schedule.open_at) + interval '30 minutes'                                        as send_at,
                    json_build_object('en', 'Top losers: ' ||
                                            (string_agg(text, ', ' order by relative_daily_change, symbol))) as text,
                    json_build_object('t', 0)                                                                     as data
             from (
                      select relative_daily_change,
                             email,
                             symbol,
                             round(relative_daily_change * 100) || '% ' || symbol as text
                      from {{ source('app', 'profiles')}}
                               join {{ ref('profile_collection_tickers_performance_ranked') }}
                                    on profile_collection_tickers_performance_ranked.profile_id = profiles.id
                                        and loser_rank <= 3
                      where relative_daily_change < -0.01
                  ) t
                      join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and
                                                exchange_schedule.date = now()::date
-- where now() > exchange_schedule.open_at
             group by email
         ),
     grouped_notifications as
         (
             select json_agg(email)       as emails,
                    uniq_id               as original_uniq_id,
                    uniq_id || '_' || grp as uniq_id,
                    min(send_at)          as send_at
             from (
                      select *,
                             ((row_number() over (partition by uniq_id) - 1) / 100)::int as grp -- max 100 emails in group
                      from individual_notifications
                  ) t
             group by uniq_id, grp
         ),
     distinct_notifications as
         (
             select distinct on (
                 uniq_id
                 ) uniq_id,
                   text,
                   data
             from individual_notifications
         )
select emails,
       grouped_notifications.uniq_id::varchar,
       send_at,
       text,
       data
from grouped_notifications
         join distinct_notifications
              on grouped_notifications.original_uniq_id = distinct_notifications.uniq_id

union all

-- Most performing TTF daily in all TTFs
-- 1) MIN 3% diff for TTF to change 2) #1 TTF
-- Notify with the performance of the top TTF on the platform  [better do it aftermarket opens and trades a bit ~12pm EST]
select null                                                       as emails,
       ('top_ttf_' || now()::date)::varchar                       as uniq_id,
       exchange_schedule.open_at + interval '2 hours 30 minutes'  as send_at,
       json_build_object('en', 'The most performing TTF today is ' || collection_name || ' +' || round(relative_daily_change * 100) ||
       '%. Check it out!')                                        as text,
       json_build_object('t', 1, 'ttf_id', collection_id)         as data
from (
         select collection_uniq_id,
                profile_collections.id as collection_id,
                name                   as collection_name,
                relative_daily_change
         from {{ ref('collection_metrics') }}
                  join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
         where relative_daily_change > 0.03
           and personalized = '0'
         order by relative_daily_change desc
         limit 1
     ) t
         join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
where now() > exchange_schedule.open_at
