{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('transactions_uniq_id, period, datetime'),
      index(this, 'id', true),
      'delete from {{this}}
        using {{this}} ptc
        left join {{ ref("portfolio_expanded_transactions") }} on portfolio_expanded_transactions.uniq_id = ptc.transactions_uniq_id
        where ptc.transactions_uniq_id = portfolio_transaction_chart.transactions_uniq_id
        and portfolio_expanded_transactions.uniq_id is null',
    ]
  )
}}


-- Execution Time: 39525.849 ms
-- Execution Time: 47097.705 ms realtime
with first_profile_transaction_date as
         (
             select profile_id,
                    min(date) as datetime
             from {{ source('app', 'profile_portfolio_transactions') }}
             group by profile_id
         ),
    chart as
        (
             (
                  select historical_prices_aggregated_3min.symbol,
                         week_trading_sessions.date,
                         historical_prices_aggregated_3min.datetime,
                         historical_prices_aggregated_3min.datetime + interval '3 minutes' as close_datetime,
                         '1d'::varchar as period,
                         historical_prices_aggregated_3min.open,
                         historical_prices_aggregated_3min.high,
                         historical_prices_aggregated_3min.low,
                         historical_prices_aggregated_3min.close,
                         historical_prices_aggregated_3min.adjusted_close,
                         historical_prices_aggregated_3min.volume,
                         historical_prices_aggregated_3min.updated_at
                  from {{ ref('historical_prices_aggregated_3min') }}
                           join {{ ref('week_trading_sessions') }} using (symbol)
                  where historical_prices_aggregated_3min.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at - interval '1 microsecond'
             )
             union all
             (
                  select symbol,
                         date,
                         datetime,
                         close_datetime,
                         period,
                         open,
                         high,
                         low,
                         close,
                         adjusted_close,
                         volume,
                         updated_at
                  from {{ ref('chart') }}
                  where period != '1d'
            )
     )

select t.*
from (
         select (portfolio_expanded_transactions.uniq_id || '_' || chart.period || '_' || chart.datetime) as id,
                portfolio_expanded_transactions.uniq_id                                                   as transactions_uniq_id,
                chart.datetime,
                chart.period,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.open                  as open,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.high                  as high,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.low                   as low,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.close                 as close,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.adjusted_close        as adjusted_close
         from {{ ref('portfolio_expanded_transactions') }}
                  left join first_profile_transaction_date using (profile_id)
                  join {{ ref('portfolio_securities_normalized') }}
                       on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                  join chart
                       on chart.symbol = portfolio_securities_normalized.original_ticker_symbol
                           and (chart.close_datetime > portfolio_expanded_transactions.date or portfolio_expanded_transactions.date is null)
                           and (chart.close_datetime > first_profile_transaction_date.datetime or first_profile_transaction_date.profile_id is null)
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (transactions_uniq_id, period, datetime)
where old_data is null
   or abs(old_data.adjusted_close - old_data.adjusted_close) > 1e-2 -- new / updated transaction - recalc all
{% endif %}
