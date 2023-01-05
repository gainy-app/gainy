{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('id'),
      index(['profile_id', 'holding_id_v2', 'period', 'datetime'], true),
    ],
  )
}}


-- Execution Time: 220791.001 ms
with portfolio_holding_chart as
         (
             select t.*
             from (
                      select portfolio_transaction_chart.profile_id,
                             holding_id_v2,
                             period,
                             portfolio_transaction_chart.datetime,
                             min(portfolio_transaction_chart.date) as date,
                             sum(quantity_norm_for_valuation)      as quantity,
                             count(transaction_uniq_id)            as transaction_count,
                             sum(open)                             as open,
                             sum(high)                             as high,
                             sum(low)                              as low,
                             sum(close)                            as close,
                             sum(adjusted_close)                   as adjusted_close,
                             max(greatest(
                                     portfolio_transaction_chart.updated_at,
                                     portfolio_expanded_transactions.updated_at
                                 ))                                as updated_at
                      from {{ ref('portfolio_transaction_chart') }}
                               join {{ ref('portfolio_expanded_transactions') }} using (transaction_uniq_id)
                      group by portfolio_transaction_chart.profile_id,
                               holding_id_v2,
                               period,
                               portfolio_transaction_chart.datetime
                  ) t
         )
select portfolio_holding_chart.*,
       case
           when portfolio_holding_chart.quantity > 0
               then portfolio_holding_chart.adjusted_close /
                    portfolio_holding_chart.quantity *
                    (last_value(portfolio_holding_chart.quantity)
                        over (partition by profile_id, holding_id_v2, period order by datetime rows between current row and unbounded following) -
                     portfolio_holding_chart.quantity)
           else 0
           end as cash_adjustment,
       profile_id || '_' || holding_id_v2 || '_' || period || '_' || datetime as id
from portfolio_holding_chart

{% if is_incremental() %}
         left join {{ this }} old_data using (profile_id, holding_id_v2, period, datetime)
where old_data.adjusted_close is null
   or abs(portfolio_holding_chart.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
