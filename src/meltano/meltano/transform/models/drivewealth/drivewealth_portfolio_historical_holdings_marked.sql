{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('profile_id, holding_id_v2, symbol, date'),
      index('id', true),
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_week" ON {{ this }} (profile_id, holding_id_v2, symbol, date_week)',
      'create index if not exists "dphh_profile_id_holding_id_v2_symbol_date_month" ON {{ this }} (profile_id, holding_id_v2, symbol, date_month)',
    ]
  )
}}

with raw_data_0d as
         (
             select profile_id, holding_id_v2, symbol,
                    date  as date_0d,
                    value as value_0d,
                    updated_at
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
                      join (
                               select profile_id, holding_id_v2, symbol,
                                      max(drivewealth_portfolio_historical_holdings.date) as date
                               from {{ ref('drivewealth_portfolio_historical_holdings') }}
                               group by profile_id, holding_id_v2, symbol
                           ) t using (profile_id, holding_id_v2, symbol, date)
         ),
     raw_data_1w as
         (
             select raw_data_0d.*,
                    drivewealth_portfolio_historical_holdings.date  as date_1w,
                    drivewealth_portfolio_historical_holdings.value as value_1w
             from raw_data_0d
                      left join (
                                    select profile_id, holding_id_v2, symbol,
                                           max(drivewealth_portfolio_historical_holdings.date) as date
                                    from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                    where date < now()::date - interval '1 week'
                                    group by profile_id, holding_id_v2, symbol
                                ) t using (profile_id, holding_id_v2, symbol)
                      left join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_1m as
         (
             select distinct on (
                 raw_data_1w.profile_id, holding_id_v2, symbol
                 ) raw_data_1w.*,
                   drivewealth_portfolio_historical_holdings.date  as date_1m,
                   drivewealth_portfolio_historical_holdings.value as value_1m
             from raw_data_1w
                      left join {{ ref('drivewealth_portfolio_historical_holdings') }}
                                on drivewealth_portfolio_historical_holdings.profile_id = raw_data_1w.profile_id
                                    and drivewealth_portfolio_historical_holdings.holding_id_v2 = raw_data_1w.holding_id_v2
                                    and drivewealth_portfolio_historical_holdings.symbol = raw_data_1w.symbol
                                    and date < now()::date - interval '1 month'
                                    and date > now()::date - interval '1 month' - interval '1 week'
             order by profile_id, holding_id_v2, symbol, date desc
     ),
     raw_data_3m as
         (
             select distinct on (
                 raw_data_1m.profile_id, holding_id_v2, symbol
                 ) raw_data_1m.*,
                   drivewealth_portfolio_historical_holdings.date  as date_3m,
                   drivewealth_portfolio_historical_holdings.value as value_3m
             from raw_data_1m
                      left join {{ ref('drivewealth_portfolio_historical_holdings') }}
                                on drivewealth_portfolio_historical_holdings.profile_id = raw_data_1m.profile_id
                                    and drivewealth_portfolio_historical_holdings.holding_id_v2 = raw_data_1m.holding_id_v2
                                    and drivewealth_portfolio_historical_holdings.symbol = raw_data_1m.symbol
                                    and date < now()::date - interval '3 month'
                                    and date > now()::date - interval '3 month' - interval '1 week'
             order by profile_id, holding_id_v2, symbol, date desc
     ),
     raw_data_1y as
         (
             select raw_data_3m.*,
                    drivewealth_portfolio_historical_holdings.date  as date_1y,
                    drivewealth_portfolio_historical_holdings.value as value_1y
             from raw_data_3m
                      left join (
                                    select profile_id, holding_id_v2, symbol,
                                           max(drivewealth_portfolio_historical_holdings.date) as date
                                    from {{ ref('drivewealth_portfolio_historical_holdings') }}
                                    where date < now()::date - interval '1 year'
                                    group by profile_id, holding_id_v2, symbol
                                ) t using (profile_id, holding_id_v2, symbol)
                      left join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     ),
     raw_data_5y as
         (
             select distinct on (
                 raw_data_1y.profile_id, holding_id_v2, symbol
                 ) raw_data_1y.*,
                   drivewealth_portfolio_historical_holdings.date  as date_5y,
                   drivewealth_portfolio_historical_holdings.value as value_5y
             from raw_data_1y
                      left join {{ ref('drivewealth_portfolio_historical_holdings') }}
                                on drivewealth_portfolio_historical_holdings.profile_id = raw_data_1y.profile_id
                                    and drivewealth_portfolio_historical_holdings.holding_id_v2 = raw_data_1y.holding_id_v2
                                    and drivewealth_portfolio_historical_holdings.symbol = raw_data_1y.symbol
                                    and date < now()::date - interval '5 year'
                                    and date > now()::date - interval '5 year' - interval '1 week'
             order by profile_id, holding_id_v2, symbol, date desc
     ),
     raw_data_all as
         (
             select raw_data_5y.*,
                    drivewealth_portfolio_historical_holdings.date  as date_all,
                    drivewealth_portfolio_historical_holdings.value as value_all
             from raw_data_5y
                      join (
                               select profile_id, holding_id_v2, symbol,
                                      min(drivewealth_portfolio_historical_holdings.date) as date
                               from {{ ref('drivewealth_portfolio_historical_holdings') }}
                               group by profile_id, holding_id_v2, symbol
                           ) t
                           using (profile_id, holding_id_v2, symbol)
                      join {{ ref('drivewealth_portfolio_historical_holdings') }} using (profile_id, holding_id_v2, symbol, date)
     )
select profile_id, holding_id_v2, symbol,
       date_0d,
       value_0d,
       date_1w,
       value_1w,
       date_1m,
       value_1m,
       date_3m,
       value_3m,
       date_1y,
       value_1y,
       date_5y,
       value_5y,
       date_all,
       value_all,
       updated_at
from raw_data_all
