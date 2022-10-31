{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('transaction_uniq_id, period, datetime'),
      'create index if not exists "ptc_profile_id" ON {{ this }} (profile_id)',
      index('id', true),
      'delete from {{this}}
        using {{this}} ptc
        left join {{ ref("portfolio_expanded_transactions") }} using (transaction_uniq_id)
        where ptc.transaction_uniq_id = portfolio_transaction_chart.transaction_uniq_id
          and portfolio_expanded_transactions.transaction_uniq_id is null',
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
         select portfolio_expanded_transactions.transaction_uniq_id || '_' ||
                chart.period || '_' || chart.datetime                                             as id,
                portfolio_expanded_transactions.profile_id,
                portfolio_expanded_transactions.transaction_uniq_id,
                chart.datetime::timestamp,
                chart.period,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * chart.open
                    )::double precision                                                           as open,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * chart.high
                    )::double precision                                                           as high,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * chart.low
                    )::double precision                                                           as low,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * chart.close
                    )::double precision                                                           as close,
                (portfolio_expanded_transactions.quantity_norm_for_valuation * chart.adjusted_close
                    )::double precision                                                           as adjusted_close,
                greatest(portfolio_expanded_transactions.updated_at, chart.updated_at)::timestamp as updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  left join first_profile_transaction_date using (profile_id)
                  join chart
                       on chart.symbol = portfolio_expanded_transactions.symbol
                           and (chart.close_datetime > portfolio_expanded_transactions.datetime or portfolio_expanded_transactions.datetime is null)
                           and (chart.close_datetime > first_profile_transaction_date.datetime or first_profile_transaction_date.profile_id is null)
         where portfolio_expanded_transactions.security_type != 'ttf'

         union all

         select transaction_uniq_id || '_' || period || '_' || datetime as id,
                profile_id,
                transaction_uniq_id::varchar,
                datetime::timestamp,
                period::varchar,
                open::double precision,
                high::double precision,
                low::double precision,
                close::double precision,
                adjusted_close::double precision,
                updated_at
         from {{ ref('drivewealth_portfolio_chart') }}
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (transaction_uniq_id, period, datetime)
where old_data.transaction_uniq_id is null
   or abs(old_data.adjusted_close - old_data.adjusted_close) > 1e-2 -- new / updated transaction - recalc all
{% endif %}
