{{
  config(
    materialized = "view",
  )
}}

with data as
        (
            -- Daily TTF movers during the trading day
            -- 1) 10 am daily 2) The 3 biggest TTF gainers and losers
            -- Two separate notifications
            -- Top morning TTF gainers are Cryptocurrencies mining (+9%), Silver mining (+7%), Cannabis (+7%)
            -- Top morning TTF losers are Cryptocurrencies mining (+9%), Silver mining (+7%), Cannabis (+7%)
            select null::int                                                                                    as profile_id,
                   'top_ttf_gainers_' || now()::date                                                            as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                                       as send_at,
                   json_build_object('en', 'TTF ' ||
                                           case when count(text) > 1 then 'gainers' else 'gainer' end)          as title,
                   json_build_object('en', string_agg('- ' || text, E'\n' order by relative_daily_change desc)) as text,
                   json_build_object('t', 1, 'id', min(collection_id))                                          as data,
                   false                                                                                        as is_test,
                   true                                                                                         as is_push,
                   false                                                                                        as is_shown_in_app,
                   '4c70442b-ff04-475f-9a63-97d442127707'                                                       as template_id
            from (
                     select first_value(profile_collections.id) over (order by relative_daily_change desc) as collection_id,
                            relative_daily_change,
                            name || ' (+' || round(relative_daily_change * 100) || '%)'                    as text
                     from {{ ref('collection_metrics') }}
                              join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
                     where relative_daily_change > 0.03
                       and personalized = '0'
                     order by relative_daily_change desc
                     limit 3
                 ) t
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '30 minutes' and exchange_schedule.open_at + interval '1 hour'
            having count(collection_id) > 0

            union all
            
            select null::int                                                                               as profile_id,
                   'top_ttf_losers_' || now()::date                                                        as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                                  as send_at,
                   json_build_object('en', 'TTF ' ||
                                           case when count(text) > 1 then 'losers' else 'loser' end)       as title,
                   json_build_object('en', string_agg('- ' || text, E'\n' order by relative_daily_change)) as text,
                   json_build_object('t', 1, 'id', min(collection_id))                                     as data,
                   false                                                                                   as is_test,
                   true                                                                                    as is_push,
                   false                                                                                   as is_shown_in_app,
                   '4c806577-88db-4f1e-a4d1-232fac0aa58a'                                                  as template_id
            from (
                     select first_value(profile_collections.id) over (order by relative_daily_change) as collection_id,
                            relative_daily_change,
                            name || ' (' || round(relative_daily_change * 100) || '%)'                as text
                     from {{ ref('collection_metrics') }}
                              join {{ ref('profile_collections') }} on profile_collections.uniq_id = collection_uniq_id
                     where relative_daily_change < -0.03
                       and personalized = '0'
                     order by relative_daily_change
                     limit 3
                 ) t
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '30 minutes' and exchange_schedule.open_at + interval '1 hour'
            having count(collection_id) > 0

            union all

            -- Most performing TTF daily in all TTFs
            -- 1) MIN 3% diff for TTF to change 2) #1 TTF
            -- Notify with the performance of the top TTF on the platform  [better do it aftermarket opens and trades a bit ~12pm EST]
            select null                                            as profile_id,
                   ('top_ttf_' || now()::date)                     as uniq_id,
                   exchange_schedule.close_at - interval '2 hours' as send_at,
                   null::json                                      as title,
                   json_build_object('en', 'The most performing TTF today is ' || collection_name || ' +' ||
                                           round(relative_daily_change * 100) ||
                                           '%. Check it out!')     as text,
                   json_build_object('t', 1, 'id', collection_id)  as data,
                   false                                           as is_test,
                   true                                            as is_push,
                   false                                           as is_shown_in_app,
                   'e1b4dd4e-3310-403b-bdc8-b51f56f54045'          as template_id
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
            where now() between exchange_schedule.open_at and exchange_schedule.close_at

            union all

            (
                -- New article
                select null::int                                                    as profile_id,
                       'new_article_' || blogs.slug                                 as uniq_id,
                       date_trunc('week', now())::date + interval '5 days 17 hours' as send_at,
                       null::json                                                   as title,
                       json_build_object('en', 'Read ' || trim(blogs.name))         as text,
                       json_build_object('t', 4, 'id', blogs.id)                    as data,
                       false                                                        as is_test,
                       true                                                         as is_push,
                       false                                                        as is_shown_in_app,
                       '07b00e92-a1ae-44ea-bde0-c0715a991f2f'                       as template_id
                from {{ source('website', 'blogs') }}
                         left join {{ source('website', 'blogs') }} article_duplicate
                                   on (article_duplicate.id = blogs.id or article_duplicate.name = blogs.name or article_duplicate.slug = blogs.slug)
                                       and (article_duplicate.id != blogs.id or article_duplicate.name != blogs.name or article_duplicate.slug != blogs.slug)
                         left join {{ source('app', 'notifications') }} this_article_notifications
                                   on this_article_notifications.uniq_id = 'new_article_' || blogs.id
                                       or this_article_notifications.uniq_id = 'new_article_' || blogs.slug
                         left join {{ source('app', 'notifications') }} all_article_notifications
                                   on all_article_notifications.uniq_id like 'new_article_%'
                                       and all_article_notifications.updated_at > now() - interval '1 week'
                 where article_duplicate.id is null -- the article has no duplicates
                   and this_article_notifications.uniq_id is null -- this article has not been sent
                   and all_article_notifications.uniq_id is null -- previous article notification was sent more than a week ago
                 order by blogs.published_on desc
                 limit 1
            )

            union all

            -- Invited user joined, free month granted
            select from_profile_id                             as profile_id,
                   ('invited_user_joined_' || id || '_sender') as uniq_id,
                   now()                                       as send_at,
                   null::json                                  as title,
                   json_build_object('en',
                       'Your friend has just joined the app via your invitation. ' ||
                       'Free month granted!')                  as text,
                   json_build_object('t', 5)                   as data,
                   false                                       as is_test,
                   true                                        as is_push,
                   false                                       as is_shown_in_app,
                   'ed86815f-3391-498c-875a-ea974342dc46'      as template_id
            from {{ source('app', 'invitations') }}
            where created_at > now() - interval '1 hour'

            union all

            select to_profile_id                                 as profile_id,
                   ('invited_user_joined_' || id || '_receiver') as uniq_id,
                   now()                                         as send_at,
                   null::json                                    as title,
                   json_build_object('en',
                       'Thanks for accepting the invitation. ' ||
                       'Free month granted!')                    as text,
                   json_build_object('t', 5)                     as data,
                   false                                         as is_test,
                   true                                          as is_push,
                   false                                         as is_shown_in_app,
                   '3c5f6ae0-1c69-4dbe-bb73-0d7f07595c95'        as template_id
            from {{ source('app', 'invitations') }}
            where created_at > now() - interval '1 hour'

            union all

            -- Worst portfolio stock
            select profile_id,
                   ('worst_portfolio_stock' || profile_id || '_' ||
                        date_trunc('month', now()::date))                               as uniq_id,
                   now()                                                                as send_at,
                   null::json                                                           as title,
                   json_build_object('en', 'What’s the worst stock in your portfolio?') as text,
                   json_build_object('t', 6, 's', symbol)                               as data,
                   false                                                                as is_test,
                   true                                                                 as is_push,
                   false                                                                as is_shown_in_app,
                   'f4c2e5bb-5cff-4776-8abf-dd320f91800b'                               as template_id
            from (
                     select distinct on (
                         profile_id
                         ) profile_id,
                           ticker_symbol as symbol
                     from {{ ref('portfolio_holding_group_gains') }}
                     where relative_gain_1m < 0
                     order by profile_id, relative_gain_1m
                ) t
                     join {{ source('app', 'profiles') }} on profiles.id = t.profile_id
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '2 hours' and exchange_schedule.close_at

            union all

            -- Stock that you own falls sharp. You have already made 30%+. Maybe sell? (=trailing stop loss)
            select profile_id,
                   ('portfolio_stock_falls_sharp' || profile_id || '_' ||
                        date_trunc('week', now()::date))                          as uniq_id,
                   exchange_schedule.open_at + interval '1 hour'                  as send_at,
                   null::json                                                     as title,
                   json_build_object('en', symbol || ' is ' || (relative_daily_change * 100)::int ||
                            '% today. You have already made ' ||
                            (relative_gain_total * 100)::int || '%. Maybe sell?') as text,
                   json_build_object('t', 7, 's', symbol)                         as data,
                   false                                                          as is_test,
                   true                                                           as is_push,
                   false                                                          as is_shown_in_app,
                   '11dc7a5a-aa96-4835-893a-cea11581ab6c'                         as template_id
            from (
                     select distinct on (
                         profile_id
                         ) profile_holdings_normalized.profile_id,
                           profile_holdings_normalized.symbol,
                           portfolio_holding_gains.relative_gain_total,
                           ticker_realtime_metrics.relative_daily_change
                     from {{ ref('profile_holdings_normalized') }}
                              join {{ ref('portfolio_holding_gains') }} using (holding_id_v2)
                              join {{ ref('ticker_realtime_metrics') }} using (symbol)
                     where relative_gain_total > 0.30
                       and relative_daily_change < -0.05
                       and profile_holdings_normalized.symbol is not null
                     order by profile_id, relative_daily_change
                ) t
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '2 hours' and exchange_schedule.close_at

            union all

            select profile_id,
                   uniq_id,
                   send_at,
                   title,
                   text,
                   data,
                   is_test,
                   is_push,
                   is_shown_in_app,
                   template_id
            from {{ ref('trading_notifications') }}
        ),
    profiles as
        (
            SELECT profiles.id as profile_id,
                   email ilike '%gainy.app'
                       or email ilike '%test%'
                       or last_name ilike '%test%'
                       or first_name ilike '%test%' as is_test
            FROM {{ source('app', 'profiles') }}
        )
select data.profile_id,
       data.uniq_id,
       data.send_at,
       data.title,
       data.text,
       data.data,
       data.is_test,
       data.is_push,
       data.is_shown_in_app,
       data.template_id
from data
         left join profiles using (profile_id)
where data.profile_id is null -- send broadcast
   or data.is_test = false -- send direct non-test
   or profiles.is_test = true -- send direct test to test users
