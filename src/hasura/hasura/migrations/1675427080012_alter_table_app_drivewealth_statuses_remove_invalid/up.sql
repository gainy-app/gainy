with portfolio_status_funds as
         (
             select created_at,
                    id,
                    drivewealth_portfolio_id,
                    equity_value,
                    json_array_elements(data -> 'holdings') as portfolio_holding_data
             from app.drivewealth_portfolio_statuses
         ),
     invalid_portfolio_sums as
         (
             select id
             from app.drivewealth_portfolio_statuses
                      join (
                               select id,
                                      sum((portfolio_holding_data ->> 'actual')::numeric) as weight_sum,
                                      sum((portfolio_holding_data ->> 'target')::numeric) as target_weight_sum,
                                      sum((portfolio_holding_data ->> 'value')::numeric)  as value_sum
                               from portfolio_status_funds
                               group by id
                           ) t using (id)
             where (abs(weight_sum - 1) > 2e-3 and equity_value > 0)
                or (abs(target_weight_sum - 1) > 2e-3 and equity_value > 0)
                or abs(value_sum - equity_value) > 1
     ),
     fund_holdings as
         (
             select id,
                    portfolio_holding_data ->> 'id'                           as fund_id,
                    (portfolio_holding_data ->> 'value')::numeric             as fund_value,
                    drivewealth_portfolio_id,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_status_funds
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     invalid_fund_sums as
         (
             select distinct on (id) id
             from (
                      select id,
                             sum((fund_holding_data ->> 'actual')::numeric) as weight_sum,
                             sum((fund_holding_data ->> 'target')::numeric) as target_weight_sum,
                             min(fund_value)                                as fund_value,
                             sum((fund_holding_data ->> 'value')::numeric)  as value_sum
                      from fund_holdings
                      group by id, fund_id
                  ) t
             where (abs(weight_sum - 1) > 2e-3 and fund_value > 0)
                or (abs(target_weight_sum - 1) > 2e-3 and fund_value > 0)
                or abs(value_sum - fund_value) > 1e-3
     )
delete from app.drivewealth_portfolio_statuses
where id in (select id from invalid_portfolio_sums)
   or id in (select id from invalid_fund_sums);
