{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('transactions_uniq_id, datetime, period'),
      index(this, 'id', true),
      'create unique index if not exists "transactions_uniq_id__period__datetime" ON {{ this }} (transactions_uniq_id, period, datetime)',
      'delete from {{this}}
        using {{this}} ptc
        left join {{ ref("portfolio_expanded_transactions") }} on portfolio_expanded_transactions.uniq_id = ptc.transactions_uniq_id
        where ptc.transactions_uniq_id = portfolio_transaction_chart.transactions_uniq_id
        and portfolio_expanded_transactions.uniq_id is null',
    ]
  )
}}


with first_profile_transaction_date as
         (
             select profile_id,
                    min(date) as datetime
             from {{ source('app', 'profile_portfolio_transactions') }}
             group by profile_id
{% if is_incremental() %}
         ),
     latest_transaction_chart_row as
         (
             select transactions_uniq_id,
                    period,
                    max(datetime) as datetime
             from {{ this }}
             group by transactions_uniq_id, period
{% endif %}
         )
select (portfolio_expanded_transactions.uniq_id || '_' || chart.datetime || '_' || chart.period)::varchar   as id,
       portfolio_expanded_transactions.uniq_id                                                              as transactions_uniq_id,
       chart.datetime,
       chart.period,
       portfolio_expanded_transactions.quantity_norm_for_valuation::numeric * chart.open::numeric           as open,
       portfolio_expanded_transactions.quantity_norm_for_valuation::numeric * chart.high::numeric           as high,
       portfolio_expanded_transactions.quantity_norm_for_valuation::numeric * chart.low::numeric            as low,
       portfolio_expanded_transactions.quantity_norm_for_valuation::numeric * chart.close::numeric          as close,
       portfolio_expanded_transactions.quantity_norm_for_valuation::numeric * chart.adjusted_close::numeric as adjusted_close
from {{ ref('portfolio_expanded_transactions') }}
         left join first_profile_transaction_date
                   on first_profile_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
{% if is_incremental() %}
         left join latest_transaction_chart_row
              on latest_transaction_chart_row.transactions_uniq_id = portfolio_expanded_transactions.uniq_id
{% endif %}
         join {{ ref('chart') }}
              on chart.symbol = portfolio_securities_normalized.original_ticker_symbol
                  and (chart.datetime >= portfolio_expanded_transactions.date or portfolio_expanded_transactions.date is null)
                  and (chart.datetime >= first_profile_transaction_date.datetime or first_profile_transaction_date.profile_id is null)
{% if is_incremental() %}
                  and (chart.period = latest_transaction_chart_row.period or latest_transaction_chart_row.transactions_uniq_id is null)
                  and (chart.datetime >= latest_transaction_chart_row.datetime or latest_transaction_chart_row.transactions_uniq_id is null)
{% endif %}
{% if is_incremental() and var('realtime') %}
where (chart.period in ('1d', '1w') or latest_transaction_chart_row.datetime is null)
{% endif %}
