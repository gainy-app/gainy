{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2, datetime'),
      index('id', true),
    ],
  )
}}


select data.*,
       data.holding_id_v2 || '_' || data.datetime as id
from (
{% if var('realtime') %}
         with stats as
                  (
                      select holding_id_v2,
                             max(datetime)   as datetime,
                             max(updated_at) as updated_at
                      from {{ this }}
                      group by holding_id_v2
                  )
{% endif %}
         select portfolio_holding_chart_1d.holding_id_v2,
                portfolio_holding_chart_1d.date,
                historical_prices_aggregated_15min.datetime,
                portfolio_holding_chart_1d.open * historical_prices_aggregated_15min.open /
                    historical_prices_aggregated_1d.open           as open,
                portfolio_holding_chart_1d.open * historical_prices_aggregated_15min.high /
                    historical_prices_aggregated_1d.open           as high,
                portfolio_holding_chart_1d.open * historical_prices_aggregated_15min.low /
                    historical_prices_aggregated_1d.open           as low,
                portfolio_holding_chart_1d.open * historical_prices_aggregated_15min.close /
                    historical_prices_aggregated_1d.open           as close,
                portfolio_holding_chart_1d.open * historical_prices_aggregated_15min.adjusted_close /
                    historical_prices_aggregated_1d.open           as adjusted_close,
                portfolio_holding_chart_1d.quantity                as quantity,
                portfolio_holding_chart_1d.transaction_count::int,
                historical_prices_aggregated_15min.relative_gain,
                greatest(portfolio_holding_chart_1d.updated_at,
                    historical_prices_aggregated_1d.updated_at,
                    historical_prices_aggregated_15min.updated_at) as updated_at
         from {{ ref('portfolio_holding_chart_1d') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                  join {{ ref('historical_prices_aggregated_1d') }}
                       on historical_prices_aggregated_1d.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_1d.date = portfolio_holding_chart_1d.date
                  join {{ ref('historical_prices_aggregated_15min') }}
                       on historical_prices_aggregated_15min.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_15min.date = portfolio_holding_chart_1d.date
                  join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.symbol = profile_holdings_normalized_all.symbol
                               and week_trading_sessions_static.date = historical_prices_aggregated_15min.date
{% if var('realtime') %}
                  left join stats using (holding_id_v2)
{% endif %}

         where week_trading_sessions_static.index != 0

{% if var('realtime') %}
           and (stats.holding_id_v2 is null
             or historical_prices_aggregated_15min.datetime > stats.datetime
             or portfolio_holding_chart_1d.updated_at> stats.updated_at
             or historical_prices_aggregated_1d.updated_at> stats.updated_at
             or historical_prices_aggregated_15min.updated_at > stats.updated_at)
{% endif %}

         union all

         select portfolio_holding_chart_1d.holding_id_v2,
                historical_prices_aggregated_15min.date,
                historical_prices_aggregated_15min.datetime,
                portfolio_holding_chart_1d.adjusted_close * historical_prices_aggregated_15min.open /
                    historical_prices_aggregated_1d.adjusted_close as open,
                portfolio_holding_chart_1d.adjusted_close * historical_prices_aggregated_15min.high /
                    historical_prices_aggregated_1d.adjusted_close as high,
                portfolio_holding_chart_1d.adjusted_close * historical_prices_aggregated_15min.low /
                    historical_prices_aggregated_1d.adjusted_close as low,
                portfolio_holding_chart_1d.adjusted_close * historical_prices_aggregated_15min.close /
                    historical_prices_aggregated_1d.adjusted_close as close,
                portfolio_holding_chart_1d.adjusted_close * historical_prices_aggregated_15min.adjusted_close /
                    historical_prices_aggregated_1d.adjusted_close as adjusted_close,
                portfolio_holding_chart_1d.quantity                as quantity,
                portfolio_holding_chart_1d.transaction_count::int,
                historical_prices_aggregated_15min.relative_gain,
                greatest(portfolio_holding_chart_1d.updated_at,
                    historical_prices_aggregated_1d.updated_at,
                    historical_prices_aggregated_15min.updated_at) as updated_at
         from {{ ref('portfolio_holding_chart_1d') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                  join {{ ref('historical_prices_aggregated_1d') }}
                       on historical_prices_aggregated_1d.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_1d.date = portfolio_holding_chart_1d.date
                  join {{ ref('week_trading_sessions_static') }}
                           on week_trading_sessions_static.symbol = profile_holdings_normalized_all.symbol
                               and week_trading_sessions_static.prev_date = portfolio_holding_chart_1d.date
                  join {{ ref('historical_prices_aggregated_15min') }}
                       on historical_prices_aggregated_15min.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_15min.date = week_trading_sessions_static.date
{% if var('realtime') %}
                  left join stats using (holding_id_v2)
{% endif %}

         where week_trading_sessions_static.index = 0

{% if var('realtime') %}
           and (stats.holding_id_v2 is null
             or historical_prices_aggregated_15min.datetime > stats.datetime
             or portfolio_holding_chart_1d.updated_at> stats.updated_at
             or historical_prices_aggregated_1d.updated_at> stats.updated_at
             or historical_prices_aggregated_15min.updated_at > stats.updated_at)
{% endif %}
    ) data

{% if is_incremental() %}
         left join {{ this }} old_data using (holding_id_v2, datetime)
where old_data.adjusted_close is null
   or (data.relative_gain is not null and old_data.relative_gain is null)
   or abs(data.quantity * data.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
