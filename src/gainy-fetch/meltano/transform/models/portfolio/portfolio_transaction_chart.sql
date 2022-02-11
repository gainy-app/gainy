{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "transactions_uniq_id__datetime__period") }} (transactions_uniq_id, datetime, period)',
      'delete from {{this}} where transactions_uniq_id not in (select uniq_id from {{ ref("portfolio_expanded_transactions") }})',
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
select (portfolio_expanded_transactions.uniq_id || '_' || chart.datetime || '_' || chart.period)::varchar as id,
       portfolio_expanded_transactions.uniq_id                                                            as transactions_uniq_id,
       chart.datetime,
       chart.period,
       portfolio_expanded_transactions.quantity_norm::numeric * chart.open::numeric                       as open,
       portfolio_expanded_transactions.quantity_norm::numeric * chart.high::numeric                       as high,
       portfolio_expanded_transactions.quantity_norm::numeric * chart.low::numeric                        as low,
       portfolio_expanded_transactions.quantity_norm::numeric * chart.close::numeric                      as close,
       portfolio_expanded_transactions.quantity_norm::numeric * chart.adjusted_close::numeric             as adjusted_close
from {{ ref('portfolio_expanded_transactions') }}
         left join first_profile_transaction_date
                   on first_profile_transaction_date.profile_id = portfolio_expanded_transactions.profile_id
         join {{ ref('portfolio_securities_normalized') }}
              on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
         join {{ ref('base_tickers') }}
              on base_tickers.symbol = portfolio_securities_normalized.original_ticker_symbol
         join {{ ref('chart') }}
              on chart.symbol = portfolio_securities_normalized.original_ticker_symbol
                  and (chart.datetime >=
                       coalesce(portfolio_expanded_transactions.date, first_profile_transaction_date.datetime) or
                       coalesce(portfolio_expanded_transactions.date,
                                first_profile_transaction_date.datetime) is null)
{% if is_incremental() %}
         left join latest_transaction_chart_row
              on latest_transaction_chart_row.transactions_uniq_id = portfolio_expanded_transactions.uniq_id
                  and latest_transaction_chart_row.period = chart.period
{% endif %}
where portfolio_expanded_transactions.type in ('buy', 'sell')
{% if is_incremental() %}
  and (latest_transaction_chart_row.datetime is null or chart.datetime >= latest_transaction_chart_row.datetime)
{% endif %}
