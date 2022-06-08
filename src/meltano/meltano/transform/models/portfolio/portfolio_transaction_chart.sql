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


-- Execution Time: 108162.002 ms
with first_profile_transaction_date as
         (
             select profile_id,
                    min(date) as datetime
             from {{ source('app', 'profile_portfolio_transactions') }}
             group by profile_id
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
    join {{ ref('chart') }}
    on chart.symbol = portfolio_securities_normalized.original_ticker_symbol
    and (chart.datetime >= portfolio_expanded_transactions.date or portfolio_expanded_transactions.date is null)
    and (chart.datetime >= first_profile_transaction_date.datetime or first_profile_transaction_date.profile_id is null)
