{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2, date'),
      index('id', true),
    ],
  )
}}


with raw_data as materialized
         (
             select *
             from (
                      select t.holding_id_v2,
                             date,
                             open,
                             high,
                             low,
                             close,
                             adjusted_close,
                             updated_at,
                             relative_gain,
                             sum(quantity)
                             over (partition by holding_id_v2 order by close_date, quantity desc nulls last) as quantity,
                             sum(transaction_count)
                             over (partition by holding_id_v2 order by close_date, quantity desc nulls last) as transaction_count
                      from (
                               select holding_id_v2,
                                      date,
                                      date as close_date,
                                      case
                                          when profile_portfolio_transactions.type = 'buy'
                                              then abs(profile_portfolio_transactions.quantity)
                                          when profile_portfolio_transactions.type = 'sell'
                                              then -abs(profile_portfolio_transactions.quantity)
                                          end * case
                                                    when profile_holdings_normalized_all.type = 'derivative'
                                                        then 100
                                                    else 1 end as quantity,
                                      1                        as transaction_count,
                                      null::double precision   as open,
                                      null::double precision   as high,
                                      null::double precision   as low,
                                      null::double precision   as close,
                                      null::double precision   as adjusted_close,
                                      null::numeric            as relative_gain,
                                      null::timestamp          as updated_at
                               from {{ ref('profile_holdings_normalized_all') }}
                                        join {{ source('app', 'profile_portfolio_transactions') }} using (account_id, security_id)
                               where not is_app_trading

                               union all

                               select holding_id_v2,
                                      datetime::date as date,
                                      datetime::date + interval '1 week' as close_date,
                                      null as quantity,
                                      0    as transaction_count,
                                      open,
                                      high,
                                      low,
                                      close,
                                      adjusted_close,
                                      relative_gain,
                                      historical_prices_aggregated_1m.updated_at
                               from {{ ref('profile_holdings_normalized_all') }}
                                        join {{ ref('historical_prices_aggregated_1m') }} using (symbol)
                               where (datetime >= holding_since or holding_since is null)
                                 and not is_app_trading
                           ) t
                  ) t
             where adjusted_close is not null
         ),
     holding_value_adjustment as
         (
             select distinct on (
                 holding_id_v2
                 ) holding_id_v2,
                   profile_holdings_normalized_all.quantity_norm_for_valuation -
                   coalesce(raw_data.quantity, 0) as adjustment
             from raw_data
                      join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
             order by holding_id_v2, date desc
     ),
     min_value_adjustment as
         (
             select holding_id_v2,
                    -min(quantity) as adjustment
             from raw_data
             group by holding_id_v2
             having min(quantity) < 0
     )
select t.holding_id_v2,
       t.date,
       t.quantity * t.open              as open,
       t.quantity * t.high              as high,
       t.quantity * t.low               as low,
       t.quantity * t.close             as close,
       t.quantity * t.adjusted_close    as adjusted_close,
       t.quantity,
       t.transaction_count::int,
       t.relative_gain,
       t.updated_at,
       t.holding_id_v2 || '_' || t.date as id
from (
         select raw_data.holding_id_v2,
                date,
                greatest(
                        0,
                        coalesce(quantity, 0) +
                        coalesce(holding_value_adjustment.adjustment, min_value_adjustment.adjustment, 0)
                    ) as quantity,
                transaction_count,
                relative_gain,
                open,
                high,
                low,
                close,
                adjusted_close,
                updated_at
         from raw_data
                  left join holding_value_adjustment using (holding_id_v2)
                  left join min_value_adjustment using (holding_id_v2)
     ) t

{% if is_incremental() %}
         left join {{ this }} old_data using (holding_id_v2, date)
where old_data.adjusted_close is null
   or abs(t.quantity * t.adjusted_close - old_data.adjusted_close) > 1e-3
{% endif %}
