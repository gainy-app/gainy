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
       date_0d,
       case
           when date_0d >= portfolio_expanded_transactions.datetime
               then quantity * price_0d
           end                                        as value_0d,
       date_1w,
       case
           when date_1w >= portfolio_expanded_transactions.datetime
               then quantity * price_1w
           end                                        as value_1w,
       date_10d,
       case
           when date_10d >= portfolio_expanded_transactions.datetime
               then quantity * price_10d
           end                                        as value_10d,
       date_1m,
       case
           when date_1m >= portfolio_expanded_transactions.datetime
               then quantity * price_1m
           end                                        as value_1m,
       date_2m,
       case
           when date_2m >= portfolio_expanded_transactions.datetime
               then quantity * price_2m
           end                                        as value_2m,
       date_3m,
       case
           when date_3m >= portfolio_expanded_transactions.datetime
               then quantity * price_3m
           end                                        as value_3m,
       date_1y,
       case
           when date_1y >= portfolio_expanded_transactions.datetime
               then quantity * price_1y
           end                                        as value_1y,
       date_13m,
       case
           when date_13m >= portfolio_expanded_transactions.datetime
               then quantity * price_13m
           end                                        as value_13m,
       date_5y,
       case
           when date_5y >= portfolio_expanded_transactions.datetime
               then quantity * price_5y
           end                                        as value_5y,

       portfolio_expanded_transactions.datetime::date as date_all,
       portfolio_expanded_transactions.amount         as value_all
from {{ ref('portfolio_expanded_transactions') }}
         join {{ ref('historical_prices_marked') }} using (symbol)

{% if is_incremental() and var('realtime') %}
         left join {{ this }} old_data using (transaction_uniq_id)
where old_data.transaction_uniq_id is null
{% endif %}
