alter table app.corporate_action_adjustments
    add column date date;
update app.corporate_action_adjustments
set date = created_at::date
where date is null;
alter table app.corporate_action_adjustments
    alter column date set not null;

create index corporate_action_adjustments_profile_id_symbol_date_index on app.corporate_action_adjustments (profile_id, symbol, date);

insert into app.corporate_action_adjustments(profile_id, trading_account_id, collection_id, symbol, amount, date)
with transactions as
         (
             select account_id                               as drivewealth_account_id,
                    min(case
                            when "type" = 'MERGER_ACQUISITION'
                                and data -> 'mergerAcquisition' ->> 'type' = 'ADD_SHARES_CASH'
                                then data -> 'mergerAcquisition' -> 'acquiree' ->> 'symbol'
                            else symbol
                        end)                                 as symbol,
                    sum(account_amount_delta)                as account_amount_delta,
                    date,
                    min(drivewealth_transactions.created_at) as created_at
             from app.drivewealth_transactions
                      left join app.corporate_action_drivewealth_transaction_link
                                on corporate_action_drivewealth_transaction_link.drivewealth_transaction_id =
                                   drivewealth_transactions.id
             where "type" in ('DIVTAX', 'DIV', 'SPINOFF', 'MERGER_ACQUISITION')
               and account_amount_delta is not null
               and corporate_action_drivewealth_transaction_link.corporate_action_adjustment_id is null
             group by account_id, "type", symbol, date
         ),
     latest_portfolio_status as
         (
             select distinct on (
                 drivewealth_portfolio_id,
                 drivewealth_portfolio_statuses.date
                 ) drivewealth_portfolio_statuses.*
             from app.drivewealth_portfolio_statuses
             order by drivewealth_portfolio_id, drivewealth_portfolio_statuses.date, created_at desc
     ),
     portfolio_funds as
         (
             select drivewealth_portfolio_id,
                    date,
                    created_at,
                    json_array_elements(data -> 'holdings') as portfolio_holding_data
             from latest_portfolio_status
--              from app.drivewealth_portfolio_statuses
     ),
     fund_holdings as
         (
             select portfolio_funds.drivewealth_portfolio_id,
                    portfolio_funds.date,
                    portfolio_funds.created_at,
                    drivewealth_funds.collection_id,
                    json_array_elements(portfolio_holding_data -> 'holdings') as fund_holding_data
             from portfolio_funds
                      join app.drivewealth_funds on drivewealth_funds.ref_id = portfolio_holding_data ->> 'id'
             where portfolio_holding_data ->> 'type' != 'CASH_RESERVE'
     ),
     transaction_symbol_distribution as
         (
             select distinct on (
                 drivewealth_account_id, symbol, transactions.date

                 ) transactions.drivewealth_account_id,
                   transactions.symbol,
                   transactions.account_amount_delta,
                   transactions.date,
                   fund_holdings.collection_id,
                   (fund_holding_data ->> 'value')::numeric as value
             from transactions
                      join app.drivewealth_portfolios using (drivewealth_account_id)
                      join fund_holdings
                           on fund_holdings.drivewealth_portfolio_id = drivewealth_portfolios.ref_id
                               and fund_holdings.date <= transactions.date
                               and fund_holding_data ->> 'symbol' = transactions.symbol
             order by drivewealth_account_id, symbol, transactions.date, fund_holdings.date desc
     ),
     transaction_symbol_distribution_stats as
         (
             select drivewealth_account_id, symbol, date, sum(value) as value_sum
             from transaction_symbol_distribution
             group by drivewealth_account_id, symbol, date
     )
select profile_id,
       trading_account_id,
       collection_id,
       symbol,
       account_amount_delta * value / value_sum as amount,
       date
from transaction_symbol_distribution
         join transaction_symbol_distribution_stats using (drivewealth_account_id, symbol, date)
         join app.drivewealth_accounts on drivewealth_accounts.ref_id = drivewealth_account_id
         join app.drivewealth_users on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
where abs(value_sum) > 0;

insert into app.corporate_action_drivewealth_transaction_link(corporate_action_adjustment_id, drivewealth_transaction_id)
select corporate_action_adjustments.id, drivewealth_transactions.id
from app.corporate_action_adjustments
         left join app.corporate_action_drivewealth_transaction_link
                   on corporate_action_adjustments.id =
                      corporate_action_drivewealth_transaction_link.corporate_action_adjustment_id
         left join app.drivewealth_accounts using (trading_account_id)
         left join app.drivewealth_transactions
                   on corporate_action_adjustments.symbol = drivewealth_transactions.symbol
                       and drivewealth_accounts.ref_id = drivewealth_transactions.account_id
                       and corporate_action_adjustments.date = drivewealth_transactions.date
where corporate_action_drivewealth_transaction_link.drivewealth_transaction_id is null;

insert into app.trading_collection_versions(profile_id, collection_id, status, target_amount_delta, trading_account_id,
                                            source, use_static_weights, note)
select corporate_action_adjustments.profile_id,
       corporate_action_adjustments.collection_id,
       'EXECUTED_FULLY'                           as status,
       amount                                     as target_amount_delta,
       corporate_action_adjustments.trading_account_id,
       'AUTOMATIC'                                as source,
       false                                      as use_static_weights,
       'caa #' || corporate_action_adjustments.id as note
from app.corporate_action_adjustments
         left join app.trading_collection_versions
                   on trading_collection_versions.note = 'caa #' || corporate_action_adjustments.id
where corporate_action_adjustments.collection_id is not null
  and trading_collection_versions.id is null;

insert into app.trading_orders(profile_id, symbol, status, target_amount_delta, trading_account_id, source, note)
select corporate_action_adjustments.profile_id,
       corporate_action_adjustments.symbol,
       'EXECUTED_FULLY'                           as status,
       amount                                     as target_amount_delta,
       corporate_action_adjustments.trading_account_id,
       'AUTOMATIC'                                as source,
       'caa #' || corporate_action_adjustments.id as note
from app.corporate_action_adjustments
         left join app.trading_orders
                   on trading_orders.note = 'caa #' || corporate_action_adjustments.id
where corporate_action_adjustments.collection_id is null
  and trading_orders.id is null;
