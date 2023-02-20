{{
  config(
    materialized = "view",
  )
}}

    
select profile_id,
       holding_id_v2,
       date,
       datetime,
       datetime + interval '3 minutes' as close_datetime,
       '1d'::varchar as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       relative_gain,
       updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '3min'

union all

select profile_id,
       holding_id_v2,
       date,
       datetime,
       datetime + interval '15 minutes' as close_datetime,
       '1w'::varchar as period,
       open,
       high,
       low,
       close,
       adjusted_close,
       relative_gain,
       updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '15min'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
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
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
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
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 day' as close_datetime,
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
       drivewealth_portfolio_historical_prices_aggregated.datetime + interval '1 week' as close_datetime,
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
       datetime + interval '1 month' as close_datetime,
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
