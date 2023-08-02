{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      index(['profile_id', 'holding_id_v2', 'symbol', 'date', 'datetime_3min']),
      index(['profile_id', 'holding_id_v2', 'symbol', 'date', 'datetime_15min']),
      'delete from {{ this }} where date < now() - interval \'2 weeks\'',
    ]
  )
}}


with portfolio_statuses as
         (
             select distinct on (profile_id, datetime_3min) *
             from (
                      select profile_id,
                             (date_trunc('minute', drivewealth_portfolio_statuses.created_at) -
                              interval '1 minute' *
                              mod(extract(minutes from drivewealth_portfolio_statuses.created_at)::int, 3)
                                 )::timestamp                                  as datetime_3min,
                             (date_trunc('minute', drivewealth_portfolio_statuses.created_at) -
                              interval '1 minute' *
                              mod(extract(minutes from drivewealth_portfolio_statuses.created_at)::int, 15)
                                 )::timestamp                                  as datetime_15min,
                             drivewealth_portfolio_statuses.created_at         as updated_at,
                             drivewealth_portfolio_statuses.id                 as portfolio_status_id,
                             drivewealth_portfolio_statuses.date,
                             drivewealth_portfolio_statuses.data -> 'holdings' as holdings
                      from {{ source('app', 'drivewealth_portfolio_statuses') }}
                               join {{ source('app', 'drivewealth_portfolios') }}
                                    on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
{% if var('realtime') %}
                               left join (
                                             select profile_id, max(date) as max_date
                                             from {{ source('app', 'drivewealth_portfolio_statuses') }}
                                                      join {{ source('app', 'drivewealth_portfolios') }}
                                                           on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                                             group by profile_id
                                         ) last_portfolio_update using (profile_id)
                      where drivewealth_portfolio_statuses.date >= coalesce(greatest(last_portfolio_update.max_date, now() - interval '5 days'), now() - interval '5 days')
{% else %}
                      where drivewealth_portfolio_statuses.created_at > now() - interval '10 days'
{% endif %}
                  ) t
             order by profile_id, datetime_3min, updated_at desc
         ),
     portfolio_status_funds as
         (
             select profile_id,
                    date,
                    datetime_3min,
                    datetime_15min,
                    portfolio_status_id,
                    updated_at,
                    json_array_elements(holdings) as portfolio_holding_data
             from portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_status_funds.profile_id,
                    portfolio_status_funds.date,
                    portfolio_status_funds.datetime_3min,
                    portfolio_status_funds.datetime_15min,
                    portfolio_status_id,
                    portfolio_status_funds.updated_at,
                    drivewealth_funds.collection_id,
                    portfolio_holding_data,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
                      join {{ source('app', 'drivewealth_funds') }}
                           on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     data as
         (
             select profile_id,
                    case
                        when collection_id is null
                            then 'dw_ticker_' || profile_id || '_' ||
                                 normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        else 'dw_ttf_' || profile_id || '_' || collection_id || '_' ||
                             normalize_drivewealth_symbol(fund_holding_data ->> 'symbol')
                        end                                                         as holding_id_v2,
                    portfolio_status_id,
                    normalize_drivewealth_symbol(fund_holding_data ->> 'symbol') as symbol,
                    collection_id,
                    date,
                    datetime_3min,
                    datetime_15min,
                    updated_at,
                    (fund_holding_data ->> 'value')::numeric                        as value
             from fund_holdings

             union all

             select profile_id,
                    profile_id || '_cash_CUR:USD'                 as holding_id_v2,
                    portfolio_status_id,
                    'CUR:USD'                                     as symbol,
                    null                                          as collection_id,
                    date,
                    datetime_3min,
                    datetime_15min,
                    updated_at,
                    (portfolio_holding_data ->> 'value')::numeric as value
             from portfolio_status_funds
             where portfolio_holding_data ->> 'type' = 'CASH_RESERVE'

             union all

             select profile_id,
                    holding_id_v2,
                    portfolio_status_id,
                    symbol,
                    collection_id,
                    (date + interval '1 day')::date      as date,
                    (date + interval '1 day')::timestamp as datetime_3min,
                    (date + interval '1 day')::timestamp as datetime_15min,
                    null as updated_at,
                    t.value
             from {{ ref('drivewealth_portfolio_historical_holdings') }} t
{% if var('realtime') %}
                      left join (
                                    select profile_id, max(date) as max_date
                                    from {{ source('app', 'drivewealth_portfolio_statuses') }}
                                             join {{ source('app', 'drivewealth_portfolios') }}
                                                  on drivewealth_portfolios.ref_id = drivewealth_portfolio_id
                                    group by profile_id
                                ) last_portfolio_update using (profile_id)
             where date >= coalesce(greatest(last_portfolio_update.max_date, now() - interval '10 days'),
                                    now() - interval '10 days')
{% else %}
             where date >= now() - interval '10 days'
{% endif %}
     )
select data.*,
       profile_id || '_' || holding_id_v2 || '_' || datetime_3min as id
from data

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, symbol, datetime_3min)
{% endif %}

where data.value is not null
  
{% if is_incremental() %}
  and (old_data.profile_id is null
    or abs(data.value - old_data.value) > {{ var('price_precision') }})
{% endif %}
