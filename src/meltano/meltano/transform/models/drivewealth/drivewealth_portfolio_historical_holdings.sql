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

with portfolio_statuses as
         (
             select distinct on (profile_id, date) *
             from (
                      select profile_id,
                             drivewealth_portfolio_statuses.created_at                              as updated_at,
                             (drivewealth_portfolio_statuses.created_at - interval '5 hours')::date as date,
                             cash_value / drivewealth_portfolio_statuses.cash_actual_weight         as value,
                             drivewealth_portfolio_statuses.data -> 'holdings'                      as holdings
                      from {{ source('app', 'drivewealth_portfolio_statuses')}}
                               join {{ source('app', 'drivewealth_portfolios')}}
                                    on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                  ) t
             order by profile_id, date desc, updated_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    date,
                    value,
                    updated_at,
                    json_array_elements(holdings) as portfolio_holding_data
             from portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_status_funds.profile_id,
                    portfolio_status_funds.date,
                    drivewealth_funds.collection_id,
                    portfolio_status_funds.updated_at,
                    portfolio_holding_data,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
                      join {{ source('app', 'drivewealth_funds')}}
                           on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     data as
         (
             select profile_id,
                    case
                        when collection_id is null
                            then 'dw_ticker_' || profile_id || '_' || normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        else 'dw_ttf_' || profile_id || '_' || collection_id || '_' || normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        end                                                         as holding_id_v2,
                    normalize_drivewealth_symbol(fund_holding_data ->> 'symbol') as symbol,
                    date,
                    (fund_holding_data ->> 'value')::numeric                        as value,
                    updated_at
             from fund_holdings
     ),
     schedule as
         (
             select profile_id, holding_id_v2, symbol, dd::date as date
             from (
                      select profile_id,
                             holding_id_v2,
                             symbol,
                             min(date) as min_date
                      from data
                      group by profile_id, holding_id_v2, symbol
                  ) t
                      join {{ ref('ticker_realtime_metrics') }} using (symbol)
                      join generate_series(min_date, ticker_realtime_metrics.time::date, interval '1 day') dd on true
     ),
     data_extended0 as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date,
                    coalesce(relative_daily_gain, 0)                     as relative_daily_gain,
                    exp(sum(ln(coalesce(relative_daily_gain, 0) + 1))
                        over (partition by holding_id_v2 order by date)) as cumulative_daily_relative_gain,
                    value                                                as value,
                    data.updated_at
             from schedule
                      left join data using (profile_id, holding_id_v2, symbol, date)
                      left join {{ ref('historical_prices') }} using (symbol, date)
     ),
     data_extended as
         (
             select profile_id,
                    holding_id_v2,
                    symbol,
                    date,
                    relative_daily_gain,
                    cumulative_daily_relative_gain *
                    (public.last_value_ignorenulls(value / cumulative_daily_relative_gain) over wnd) as value,
                    public.last_value_ignorenulls(data_extended0.updated_at) over wnd                as updated_at
             from data_extended0
                 window wnd as (partition by profile_id, holding_id_v2 order by date rows between unbounded preceding and current row)
     )
select data_extended.*,
       date_trunc('week', date)::date                    as date_week,
       date_trunc('month', date)::date                   as date_month,
       profile_id || '_' || holding_id_v2 || '_' || date as id
from data_extended

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, date)
where old_data.profile_id is null
   or abs(data_extended.relative_daily_gain - old_data.relative_daily_gain) > 1e-3
   or abs(data_extended.value - old_data.value) > 1e-3
{% endif %}
