{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}

with all_push_notifications as
        (
            -- Daily movers in your TTFs or portfolio during the trading day
            -- 1) 10 am daily 2) The 3 biggest gainers and losers in TTFs or portfolio you follow
            -- 6 biggest changes in your portfolio[better do it after market opens ~10am EST]
            -- Two separate notifications
            -- / Top losers: +3% TSLA, +5% MDB ...
            select profile_id,
                   (profile_id || '_top_gainers_' || now()::date)                                              as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                                      as send_at,
                   json_build_object('en', 'Morning gainers: ' ||
                                           string_agg(text, ', ' order by relative_daily_change desc, symbol)) as text,
                   json_build_object('t', 0)                                                                   as data,
                   false                                                                                       as is_test,
                   'a6283759-d903-4abd-a964-65aba98154cd'                                                      as template_id
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
            where now() between exchange_schedule.open_at and exchange_schedule.close_at
            group by profile_id
            having count(symbol) > 0

            union all

            select profile_id,
                   (profile_id || '_top_losers_' || now()::date)                                          as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                                 as send_at,
                   json_build_object('en', 'Morning losers: ' ||
                                           string_agg(text, ', ' order by relative_daily_change, symbol)) as text,
                   json_build_object('t', 0)                                                              as data,
                   false                                                                                  as is_test,
                   '40818fa7-10c7-41b3-9952-800e9eea1a06'                                                 as template_id
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
            where now() between exchange_schedule.open_at and exchange_schedule.close_at
            group by profile_id
            having count(symbol) > 0

            union all

            -- Daily TTF movers during the trading day
            -- 1) 10 am daily 2) The 3 biggest TTF gainers and losers
            -- Two separate notifications
            -- Top morning TTF gainers are Cryptocurrencies mining (+9%), Silver mining (+7%), Cannabis (+7%)
            -- Top morning TTF losers are Cryptocurrencies mining (+9%), Silver mining (+7%), Cannabis (+7%)
            select null                                                                                as profile_id,
                   'top_ttf_gainers_' || now()::date                                                   as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                              as send_at,
                   json_build_object('en', 'The top morning TTF ' ||
                                           case when count(text) > 1 then 'gainers are ' else 'gainer is ' end ||
                                           string_agg(text, ', ' order by relative_daily_change desc)) as text,
                   json_build_object('t', 1, 'id', min(collection_id))                                 as data,
                   true                                                                                as is_test,
                   '4c70442b-ff04-475f-9a63-97d442127707'                                              as template_id
            from (
                     select first_value(profile_collections.id) over (order by relative_daily_change desc) as collection_id,
                            relative_daily_change,
                            name || ' (+' || round(relative_daily_change * 100) || '%)'                    as text
                     from collection_metrics
                              join profile_collections on profile_collections.uniq_id = collection_uniq_id
                     where relative_daily_change > 0.03
                       and personalized = '0'
                     order by relative_daily_change desc
                     limit 3
                 ) t
                     join exchange_schedule on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '30 minutes' and exchange_schedule.open_at + interval '1 hour'
            having count(collection_id) > 0

            union all
            
            select null                                                                                as profile_id,
                   'top_ttf_losers_' || now()::date                                                    as uniq_id,
                   min(exchange_schedule.open_at) + interval '30 minutes'                              as send_at,
                   json_build_object('en', 'The top morning TTF ' ||
                                           case when count(text) > 1 then 'losers are ' else 'loser is ' end ||
                                           string_agg(text, ', ' order by relative_daily_change desc)) as text,
                   json_build_object('t', 1, 'id', min(collection_id))                                 as data,
                   true                                                                                as is_test,
                   '4c806577-88db-4f1e-a4d1-232fac0aa58a'                                              as template_id
            from (
                     select first_value(profile_collections.id) over (order by relative_daily_change) as collection_id,
                            relative_daily_change,
                            name || ' (' || round(relative_daily_change * 100) || '%)'                as text
                     from collection_metrics
                              join profile_collections on profile_collections.uniq_id = collection_uniq_id
                     where relative_daily_change < -0.03
                       and personalized = '0'
                     order by relative_daily_change
                     limit 3
                 ) t
                     join exchange_schedule on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '30 minutes' and exchange_schedule.open_at + interval '1 hour'
            having count(collection_id) > 0

            union all

            -- Most performing TTF daily in all TTFs
            -- 1) MIN 3% diff for TTF to change 2) #1 TTF
            -- Notify with the performance of the top TTF on the platform  [better do it aftermarket opens and trades a bit ~12pm EST]
            select null                                            as profile_id,
                   ('top_ttf_' || now()::date)                     as uniq_id,
                   exchange_schedule.close_at - interval '2 hours' as send_at,
                   json_build_object('en', 'The most performing TTF today is ' || collection_name || ' +' ||
                                           round(relative_daily_change * 100) ||
                                           '%. Check it out!')   as text,
                   json_build_object('t', 1, 'id', collection_id)  as data,
                   false                                           as is_test,
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
                       json_build_object('en', 'Read ' || trim(blogs.name))         as text,
                       json_build_object('t', 4, 'id', blogs.id)                    as data,
                       false                                                        as is_test,
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
                 where article_duplicate is null -- the article has no duplicates
                   and this_article_notifications is null -- this article has not been sent
                   and all_article_notifications is null -- previous article notification was sent more than a week ago
                 order by blogs.published_on desc
                 limit 1
            )

            union all

            -- Invited user joined, free month granted
            select from_profile_id                             as profile_id,
                   ('invited_user_joined_' || id || '_sender') as uniq_id,
                   now()                                       as send_at,
                   json_build_object('en',
                       'Your friend has just joined the app via your invitation. ' ||
                       'Free month granted!')                  as text,
                   json_build_object('t', 5)                   as data,
                   false                                       as is_test,
                   'ed86815f-3391-498c-875a-ea974342dc46'      as template_id
            from {{ source('app', 'invitations') }}
            where created_at > now() - interval '1 hour'
            union all
            select to_profile_id                                 as profile_id,
                   ('invited_user_joined_' || id || '_receiver') as uniq_id,
                   now()                                         as send_at,
                   json_build_object('en',
                       'Thanks for accepting the invitation. ' ||
                       'Free month granted!')                  as text,
                   json_build_object('t', 5)                   as data,
                   false                                       as is_test,
                   '3c5f6ae0-1c69-4dbe-bb73-0d7f07595c95'      as template_id
            from {{ source('app', 'invitations') }}
            where created_at > now() - interval '1 hour'

            union all

            -- Worst portfolio stock
            select profile_id,
                   ('worst_portfolio_stock' || profile_id || '_' ||
                        date_trunc('month', now()::date))                               as uniq_id,
                   now()                                                                as send_at,
                   json_build_object('en', 'What’s the worst stock in your portfolio?') as text,
                   json_build_object('t', 6, 's', symbol)                               as data,
                   false                                                                as is_test,
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
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '2 hours' and exchange_schedule.close_at

            union all

            -- Stock that you own falls sharp. You have already made 30%+. Maybe sell? (=trailing stop loss)
            select profile_id,
                   ('portfolio_stock_falls_sharp' || profile_id || '_' ||
                        date_trunc('week', now()::date))                             as uniq_id,
                   exchange_schedule.open_at + interval '1 hour'                     as send_at,
                   json_build_object('en', original_ticker_symbol || ' is ' || (relative_daily_change * 100)::int ||
                            '% today. You have already made ' ||
                            (relative_position_gain * 100)::int || '%. Maybe sell?') as text,
                   json_build_object('t', 7, 's', original_ticker_symbol)            as data,
                   false                                                             as is_test,
                   '11dc7a5a-aa96-4835-893a-cea11581ab6c'                            as template_id
            from (
                     with raw_positions as
                              (
                                  select profile_id,
                                         portfolio_expanded_transactions.date,
                                         quantity_norm,
                                         original_ticker_symbol,
                                         sum(case when quantity_norm > 0 then abs(amount) end)
                                         over (partition by profile_id, account_id, security_id order by portfolio_expanded_transactions.date, quantity_norm desc) as cost_sum,
                                         sum(case when quantity_norm < 0 then abs(amount) end)
                                         over (partition by profile_id, account_id, security_id order by portfolio_expanded_transactions.date, quantity_norm desc) as take_profit_sum,
                                         sum(quantity_norm)
                                         over (partition by profile_id, account_id, security_id order by portfolio_expanded_transactions.date, quantity_norm desc) as quantity_sum,
                                         sum((uniq_id like 'auto%')::int)
                                         over (partition by profile_id, account_id, security_id order by portfolio_expanded_transactions.date, quantity_norm desc) as auto_cnt,
                                         account_id,
                                         security_id
                                  from {{ ref('portfolio_expanded_transactions') }}
                                           join {{ ref('portfolio_securities_normalized') }}
                                                on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                                           left join {{ ref('historical_prices') }}
                                                     on historical_prices.symbol = portfolio_securities_normalized.original_ticker_symbol
                                                         and historical_prices.date = portfolio_expanded_transactions.date
                              ),
                          distinct_positions as
                              (
                                  select distinct on (
                                      profile_id, account_id, security_id
                                      ) profile_id,
                                        original_ticker_symbol,
                                        cost_sum,
                                        take_profit_sum,
                                        quantity_sum,
                                        auto_cnt
                                  from raw_positions
                                  order by profile_id, account_id, security_id, date desc, quantity_norm
                              ),
                         positions_with_profit as
                             (
                                 select distinct_positions.profile_id,
                                        distinct_positions.original_ticker_symbol,
                                        cost_sum                                                                        as cost,
                                        coalesce(take_profit_sum, 0) + quantity_sum * historical_prices_marked.price_0d as profit,
                                        (coalesce(take_profit_sum, 0) + quantity_sum * historical_prices_marked.price_0d - cost_sum) / cost_sum as relative_position_gain,
                                        ticker_realtime_metrics.relative_daily_change
                                 from distinct_positions
                                          join {{ ref('historical_prices_marked') }} on historical_prices_marked.symbol = distinct_positions.original_ticker_symbol
                                          join {{ ref('ticker_realtime_metrics') }} on ticker_realtime_metrics.symbol = distinct_positions.original_ticker_symbol
                                 where auto_cnt = 0
                                   and cost_sum > 0
                                   and quantity_sum > 0
                             )
                     select distinct on (profile_id) *
                     from positions_with_profit
                     where relative_position_gain > 0.30
                       and relative_daily_change < -0.05
                     order by profile_id, relative_daily_change
                ) t
                     join {{ ref('exchange_schedule') }} on exchange_schedule.country_name = 'USA' and exchange_schedule.date = now()::date
            where now() between exchange_schedule.open_at + interval '2 hours' and exchange_schedule.close_at
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
select all_push_notifications.profile_id,
       all_push_notifications.uniq_id,
       all_push_notifications.send_at,
       all_push_notifications.text,
       all_push_notifications.data,
       all_push_notifications.is_test,
       all_push_notifications.template_id
from all_push_notifications
left join profiles using (profile_id)
where all_push_notifications.profile_id is null -- send broadcast
   or all_push_notifications.is_test = false -- send direct non-test
   or profiles.is_test = true -- send direct test to test users
