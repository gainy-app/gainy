{{
  config(
    materialized = "incremental",
    unique_key = "transaction_uniq_id",
    tags = ["realtime"],
    post_hook=[
      pk('transaction_uniq_id'),
    ]
  )
}}

select transaction_uniq_id,
       portfolio_transaction_values_marked.value_0d /
       case
           when coalesce(portfolio_transaction_values_marked.value_1w, portfolio_transaction_values_marked.value_all) > 0
               then coalesce(portfolio_transaction_values_marked.value_1w, portfolio_transaction_values_marked.value_all)
           end - 1 as relative_gain_1w,
       portfolio_transaction_values_marked.value_0d /
       case
           when coalesce(portfolio_transaction_values_marked.value_1m, portfolio_transaction_values_marked.value_all) > 0
               then coalesce(portfolio_transaction_values_marked.value_1m, portfolio_transaction_values_marked.value_all)
           end - 1 as relative_gain_1m,
       portfolio_transaction_values_marked.value_0d /
       case
           when coalesce(portfolio_transaction_values_marked.value_3m, portfolio_transaction_values_marked.value_all) > 0
               then coalesce(portfolio_transaction_values_marked.value_3m, portfolio_transaction_values_marked.value_all)
           end - 1 as relative_gain_3m,
       portfolio_transaction_values_marked.value_0d /
       case
           when coalesce(portfolio_transaction_values_marked.value_1y, portfolio_transaction_values_marked.value_all) > 0
               then coalesce(portfolio_transaction_values_marked.value_1y, portfolio_transaction_values_marked.value_all)
           end - 1 as relative_gain_1y,
       portfolio_transaction_values_marked.value_0d /
       case
           when coalesce(portfolio_transaction_values_marked.value_5y, portfolio_transaction_values_marked.value_all) > 0
               then coalesce(portfolio_transaction_values_marked.value_5y, portfolio_transaction_values_marked.value_all)
           end - 1 as relative_gain_5y,
       portfolio_transaction_values_marked.value_0d /
       case
           when portfolio_transaction_values_marked.value_all > 0
               then portfolio_transaction_values_marked.value_all
           end - 1 as relative_gain_all
from {{ ref('portfolio_expanded_transactions') }}
         left join {{ ref('portfolio_transaction_values_marked') }} using (transaction_uniq_id)

{% if is_incremental() and var('realtime') %}
         left join {{ this }} old_data using (transaction_uniq_id)
where old_data.transaction_uniq_id is null
{% endif %}
