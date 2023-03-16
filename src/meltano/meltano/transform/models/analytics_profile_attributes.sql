with profiles as
         (
             select id as profile_id, email as user_email
             from {{ source('app', 'profiles') }}
         ),
     profile_interests as
         (
             select profile_id, json_agg(name) as interests
             from {{ source('app', 'profile_interests') }}
                      join {{ ref('interests') }} on interests.id = interest_id
             group by profile_id
     ),
     profile_categories as
         (
             select profile_id, json_agg(name) as categories
             from {{ source('app', 'profile_categories') }}
                      join {{ ref('categories') }} on profile_categories.category_id = categories.id
             group by profile_id
     ),
     profile_flags as
         (
             select profile_id, is_personalization_enabled as ms_created from {{ ref('profile_flags') }}
     ),
     trading_profile_status as
         (
             select profile_id,
                    kyc_status,
                    (account_no is not null)  as dw_account_opened,
                    funding_account_connected as funding_acc_connected
             from {{ ref('trading_profile_status') }}
     ),
     trading_funding_accounts as
         (
             select trading_funding_accounts.profile_id,
                    count(*)                                             as number_of_funding_acc,
                    json_agg(plaid_institutions.name)
                    filter ( where plaid_institutions.name is not null ) as bank_funding_acc
             from {{ source('app', 'trading_funding_accounts') }}
                      left join {{ source('app', 'profile_plaid_access_tokens') }}
                                on profile_plaid_access_tokens.id = trading_funding_accounts.plaid_access_token_id
                      left join {{ source('app', 'plaid_institutions') }}
                                on plaid_institutions.id = profile_plaid_access_tokens.institution_id
             group by trading_funding_accounts.profile_id
     ),
     product_type_purchased as
         (
             select profile_id, json_agg(type) as product_type_purchased
             from (
                      select distinct profile_id, 'ttf' as type
                      from {{ source('app', 'trading_collection_versions') }}
                      where status = 'EXECUTED_FULLY'
                      union all
                      select distinct profile_id, type
                      from {{ source('app', 'trading_orders') }}
                               join {{ ref('base_tickers') }} using (symbol)
                      where status = 'EXECUTED_FULLY'
                  ) t
             group by profile_id
     ),
     total_invested_ttfs as
         (
             select distinct profile_id, count(distinct collection_id) as total_invested_ttfs
             from {{ source('app', 'trading_collection_versions') }}
             where status = 'EXECUTED_FULLY'
               and target_amount_delta is not null
             group by profile_id
     ),
     total_invested_tickers as
         (
             select distinct profile_id, count(distinct symbol) as total_invested_tickers
             from {{ source('app', 'trading_orders') }}
             where status = 'EXECUTED_FULLY'
               and target_amount_delta is not null
             group by profile_id
     ),
     total_wishlist_ttfs as
         (
             select distinct profile_id, count(collection_id) as total_wishlist_ttfs
             from {{ source('app', 'profile_favorite_collections') }}
             group by profile_id
     ),
     total_wishlist_ticker as
         (
             select distinct profile_id, count(symbol) as total_wishlist_ticker
             from {{ source('app', 'profile_watchlist_tickers') }}
             group by profile_id
     ),
     total_withdraws as
         (
             select distinct profile_id, count(*) as total_withdraws
             from {{ source('app', 'trading_money_flow') }}
             where status = 'SUCCESS'
               and amount < 0
             group by profile_id
     ),
     deposits as
         (
             select distinct profile_id, count(*) as total_deposits, sum(amount) as total_amount_deposit
             from {{ source('app', 'trading_money_flow') }}
             where status = 'SUCCESS'
               and amount > 0
             group by profile_id
     ),
     buys as
         (
             select profile_id,
                    sum(cnt)          as total_invests,
                    sum(amount)       as total_amount_invest,
                    min(min_datetime) as first_purchased_date,
                    max(max_datetime) as last_purchased_date
             from (
                      select distinct profile_id,
                                      count(*)                 as cnt,
                                      sum(target_amount_delta) as amount,
                                      min(created_at)          as min_datetime,
                                      max(created_at)          as max_datetime
                      from {{ source('app', 'trading_orders') }}
                      where status = 'EXECUTED_FULLY'
                        and target_amount_delta is not null
                        and target_amount_delta > 0
                      group by profile_id

                      union all

                      select distinct profile_id,
                                      count(*)                 as cnt,
                                      sum(target_amount_delta) as amount,
                                      min(created_at)          as min_datetime,
                                      max(created_at)          as max_datetime
                      from {{ source('app', 'trading_collection_versions') }}
                      where status = 'EXECUTED_FULLY'
                        and target_amount_delta is not null
                        and target_amount_delta > 0
                      group by profile_id
                  ) t
             group by profile_id
     ),
     sells as
         (
             select profile_id,
                    sum(cnt)          as total_sells,
                    sum(amount)       as total_amount_sell,
                    min(min_datetime) as first_sell_date,
                    max(max_datetime) as last_sell_date
             from (
                      select distinct profile_id,
                                      count(*)                 as cnt,
                                      sum(target_amount_delta) as amount,
                                      min(created_at)          as min_datetime,
                                      max(created_at)          as max_datetime
                      from {{ source('app', 'trading_orders') }}
                      where status = 'EXECUTED_FULLY'
                        and target_amount_delta is not null
                        and target_amount_delta < 0
                      group by profile_id

                      union all

                      select distinct profile_id,
                                      count(*)                 as cnt,
                                      sum(target_amount_delta) as amount,
                                      min(created_at)          as min_datetime,
                                      max(created_at)          as max_datetime
                      from {{ source('app', 'trading_collection_versions') }}
                      where status = 'EXECUTED_FULLY'
                        and target_amount_delta is not null
                        and target_amount_delta < 0
                      group by profile_id
                  ) t
             group by profile_id
     ),
     kyc as
         (
             select profile_id,
                    investor_profile_experience            as kyc_invest_experience,
                    investor_profile_annual_income         as kyc_income,
                    investor_profile_net_worth_total       as kyc_net_worth,
                    investor_profile_net_worth_liquid      as kyc_net_liquid,
                    investor_profile_risk_tolerance        as kyc_risk_tolerance,
                    employment_status                      as kyc_employment_status,
                    extract(year from age(birthdate))::int as age,
                    extract(year from birthdate)::int      as birthday_year
             from {{ source('app', 'kyc_form') }}
     ),
     profile_scoring_settings as
         (
             select profile_id,
                    risk_level                  as investment_goal,
                    average_market_return,
                    investment_horizon          as invest_horizon,
                    unexpected_purchases_source as urgent_money_source,
                    damage_of_failure,
                    stock_market_risk_level     as stock_market_risks,
                    trading_experience
             from {{ source('app', 'profile_scoring_settings') }}
     )
select profile_id,
       user_email,
       age,
       birthday_year,
       coalesce(interests, '[]'::json)                              as interests,
       coalesce(categories, '[]'::json)                             as categories,
       ms_created,
       dw_account_opened,
       funding_acc_connected,
       number_of_funding_acc::int,
       bank_funding_acc,
       coalesce(product_type_purchased, '[]'::json)                 as product_type_purchased,
       coalesce(total_invested_ttfs, 0)::int                        as total_invested_ttfs,
       coalesce(total_invested_tickers, 0)::int                     as total_invested_tickers,
       coalesce(total_wishlist_ttfs, 0)::int                        as total_wishlist_ttfs,
       coalesce(total_wishlist_ticker, 0)::int                      as total_wishlist_ticker,
       (coalesce(total_withdraws, 0) + coalesce(total_deposits, 0) + coalesce(total_invests, 0) +
        coalesce(total_sells, 0))::int                              as total_transactions,
       coalesce(total_withdraws, 0)::int                            as total_withdraws,
       coalesce(total_deposits, 0)::int                             as total_deposits,
       coalesce(total_invests, 0)::int                              as total_invests,
       coalesce(total_sells, 0)::int                                as total_sells,
       (coalesce(total_invests, 0) + coalesce(total_sells, 0))::int as total_trades,
       coalesce(total_amount_deposit, 0)::double precision          as total_amount_deposit,
       coalesce(total_amount_invest, 0)::double precision           as total_amount_invest,
       coalesce(total_amount_sell, 0)::double precision             as total_amount_sell,
       first_purchased_date,
       first_sell_date,
       last_purchased_date,
       last_sell_date,
       kyc_invest_experience,
       kyc_income,
       kyc_net_worth,
       kyc_net_liquid,
       kyc_risk_tolerance,
       kyc_employment_status,
       kyc_status,
       investment_goal,
       average_market_return,
       invest_horizon,
       urgent_money_source,
       damage_of_failure,
       stock_market_risks,
       trading_experience
from profiles
         left join profile_interests using (profile_id)
         left join profile_categories using (profile_id)
         left join profile_flags using (profile_id)
         left join trading_profile_status using (profile_id)
         left join trading_funding_accounts using (profile_id)
         left join product_type_purchased using (profile_id)
         left join total_invested_ttfs using (profile_id)
         left join total_invested_tickers using (profile_id)
         left join total_wishlist_ttfs using (profile_id)
         left join total_wishlist_ticker using (profile_id)
         left join total_withdraws using (profile_id)
         left join deposits using (profile_id)
         left join buys using (profile_id)
         left join sells using (profile_id)
         left join kyc using (profile_id)
         left join profile_scoring_settings using (profile_id)
