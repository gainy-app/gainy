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
{% if is_incremental() %}
         ),
    old_model_stats as
         (
             select transactions_uniq_id, period,
                    max(transactions_updated_at) as max_transactions_updated_at,
                    max(datetime) as max_datetime
             from {{ this }}
             group by transactions_uniq_id, period
{% endif %}
         )

select *
from (
         select (portfolio_expanded_transactions.uniq_id || '_' || chart.period || '_' || chart.datetime) as id,
                portfolio_expanded_transactions.uniq_id                                                   as transactions_uniq_id,
                chart.datetime,
                chart.period,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.open                  as open,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.high                  as high,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.low                   as low,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.close                 as close,
                portfolio_expanded_transactions.quantity_norm_for_valuation * chart.adjusted_close        as adjusted_close,
                portfolio_expanded_transactions.updated_at                                                as transactions_updated_at
         from {{ ref('portfolio_expanded_transactions') }}
                  left join first_profile_transaction_date using (profile_id)
                  join {{ ref('portfolio_securities_normalized') }}
                       on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                  join {{ ref('chart') }}
                       on chart.symbol = portfolio_securities_normalized.original_ticker_symbol
                           and (chart.datetime >= portfolio_expanded_transactions.date or portfolio_expanded_transactions.date is null)
                           and (chart.datetime >= first_profile_transaction_date.datetime or first_profile_transaction_date.profile_id is null)
     ) t

{% if is_incremental() %}
         left join old_model_stats using (transactions_uniq_id, period)
where old_model_stats.transactions_uniq_id is null
   or t.transactions_updated_at > old_model_stats.max_transactions_updated_at -- new / updated transaction - recalc all
   or t.datetime > old_model_stats.max_datetime -- old transaction - recalc only new
{% endif %}