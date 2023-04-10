{{
  config(
    materialized = "view",
  )
}}


with latest_price_row as materialized
      (
             select distinct on (
                 holding_id_v2
                 ) *
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             order by holding_id_v2, date desc
      )

(
    select distinct on (
        holding_id_v2, datetime
        ) profile_id,
          holding_id_v2,
          date,
          datetime,
          '1d'::varchar as period,
          open,
          high,
          low,
          close,
          adjusted_close,
          relative_gain,
          updated_at
    from (
             select profile_id,
                    holding_id_v2,
                    date,
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    relative_gain,
                    updated_at,
                    0 as priority
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             where drivewealth_portfolio_historical_prices_aggregated.period = '3min'

             union all

             select profile_id,
                    holding_id_v2,
                    date::date                                    as date,
                    date_trunc('minute', updated_at) -
                    interval '1 minute' *
                    mod(extract(minutes from updated_at)::int, 3) as datetime,
                    null                                          as open,
                    null                                          as high,
                    null                                          as low,
                    null                                          as close,
                    value                                         as adjusted_close,
                    null                                          as relative_gain,
                    updated_at,
                    1                                             as priority
             from latest_price_row
         ) t
    order by holding_id_v2, datetime, priority
)

union all

(
    select distinct on (
        holding_id_v2, datetime
        ) profile_id,
          holding_id_v2,
          date,
          datetime,
          '1w'::varchar as period,
          open,
          high,
          low,
          close,
          adjusted_close,
          relative_gain,
          updated_at
    from (
             select profile_id,
                    holding_id_v2,
                    date,
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    relative_gain,
                    updated_at,
                    0 as priority
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             where drivewealth_portfolio_historical_prices_aggregated.period = '15min'

             union all

             select profile_id,
                    holding_id_v2,
                    date::date                                     as date,
                    date_trunc('minute', updated_at) -
                    interval '1 minute' *
                    mod(extract(minutes from updated_at)::int, 15) as datetime,
                    null                                           as open,
                    null                                           as high,
                    null                                           as low,
                    null                                           as close,
                    value                                          as adjusted_close,
                    null                                           as relative_gain,
                    updated_at,
                    1                                              as priority
             from latest_price_row
         ) t
    order by holding_id_v2, datetime, priority
)

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '1m'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now() - interval '1 month + 1 week'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '3m'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now() - interval '3 month + 1 week'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '1y'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '5y'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.open,
       drivewealth_portfolio_historical_prices_aggregated.high,
       drivewealth_portfolio_historical_prices_aggregated.low,
       drivewealth_portfolio_historical_prices_aggregated.close,
       drivewealth_portfolio_historical_prices_aggregated.adjusted_close,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1w'

union all

select profile_id,
       holding_id_v2,
       datetime::date                as date,
       datetime,
       'all'::varchar                as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1m'
