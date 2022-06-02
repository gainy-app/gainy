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
select profile_id,
       (profile_id || '_top_gainers_' || now()::date)::varchar                                                      as uniq_id,
       min(exchange_schedule.open_at) + interval '30 minutes'                                                       as send_at,
       json_build_object('en', 'Morning gainers: ' ||
                               string_agg(text, ', ' order by relative_daily_change desc, symbol))                  as text,
       json_build_object('t', 0)                                                                                    as data
from (
         select relative_daily_change,
                symbol,
                profile_id,
                email,
                '+' || round(relative_daily_change * 100) || '% ' || symbol as text
         from {{ source('app', 'profiles')}}
                  join {{ ref('profile_collection_tickers_performance_ranked') }}
                       on profile_collection_tickers_performance_ranked.profile_id = profiles.id
                           and gainer_rank <= 3
         where relative_daily_change > 0.01
         order by relative_daily_change desc
     ) t
         join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
where now() > exchange_schedule.open_at
group by profile_id

union all

select profile_id,
       (profile_id || '_top_losers_' || now()::date)::varchar                                                       as uniq_id,
       min(exchange_schedule.open_at) + interval '30 minutes'                                                       as send_at,
       json_build_object('en', 'Morning losers: ' ||
                               string_agg(text, ', ' order by relative_daily_change, symbol))                       as text,
       json_build_object('t', 0)                                                                                    as data
from (
         select relative_daily_change,
                symbol,
                profile_id,
                email,
                round(relative_daily_change * 100) || '% ' || symbol as text
         from {{ source('app', 'profiles')}}
                  join {{ ref('profile_collection_tickers_performance_ranked') }}
                       on profile_collection_tickers_performance_ranked.profile_id = profiles.id
                           and loser_rank <= 3
         where relative_daily_change < -0.01
         order by relative_daily_change desc
     ) t
         join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
where now() > exchange_schedule.open_at
group by profile_id

union all

-- Most performing TTF daily in all TTFs
-- 1) MIN 3% diff for TTF to change 2) #1 TTF
-- Notify with the performance of the top TTF on the platform  [better do it aftermarket opens and trades a bit ~12pm EST]
select null                                            as profile_id,
       ('top_ttf_' || now()::date)::varchar            as uniq_id,
       exchange_schedule.close_at - interval '2 hours' as send_at,
       json_build_object('en', 'The most performing TTF today is ' || collection_name || ' +' || round(relative_daily_change * 100) ||
       '%. Check this out!')                           as text,
       json_build_object('t', 1, 'id', collection_id)  as data
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
