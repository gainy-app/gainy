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
         select portfolio_holding_chart_1d.holding_id_v2,
                historical_prices_aggregated_3min.datetime,
                sum(portfolio_holding_chart_1d.open * historical_prices_aggregated_3min.open /
                    historical_prices_aggregated_1d.open)          as open,
                sum(portfolio_holding_chart_1d.open * historical_prices_aggregated_3min.high /
                    historical_prices_aggregated_1d.open)          as high,
                sum(portfolio_holding_chart_1d.open * historical_prices_aggregated_3min.low /
                    historical_prices_aggregated_1d.open)          as low,
                sum(portfolio_holding_chart_1d.open * historical_prices_aggregated_3min.close /
                    historical_prices_aggregated_1d.open)          as close,
                sum(portfolio_holding_chart_1d.open * historical_prices_aggregated_3min.adjusted_close /
                    historical_prices_aggregated_1d.open)          as adjusted_close,
                sum(portfolio_holding_chart_1d.quantity)           as quantity,
                sum(portfolio_holding_chart_1d.transaction_count)  as transaction_count,
                max(greatest(portfolio_holding_chart_1d.updated_at,
                    historical_prices_aggregated_1d.updated_at,
                    historical_prices_aggregated_3min.updated_at)) as updated_at
         from {{ ref('portfolio_holding_chart_1d') }}
                  join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                  join {{ ref('historical_prices_aggregated_1d') }}
                       on historical_prices_aggregated_1d.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_1d.date = portfolio_holding_chart_1d.date
                  join {{ ref('historical_prices_aggregated_3min') }}
                       on historical_prices_aggregated_3min.symbol = profile_holdings_normalized_all.symbol
                           and historical_prices_aggregated_3min.date = portfolio_holding_chart_1d.date
         group by portfolio_holding_chart_1d.holding_id_v2, historical_prices_aggregated_3min.datetime
    ) data

{% if is_incremental() %}
         left join {{ this }} old_data using (holding_id_v2, datetime)
where old_data.adjusted_close is null
   or abs(data.quantity * data.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
