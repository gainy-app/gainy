{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', false),
    ]
  )
}}

with expanded_quaterly_cash_flow as (
    select code as symbol,
           (json_each((financials -> 'Cash_Flow' ->> 'quarterly')::json)).*
    from {{ source('eod', 'eod_fundamentals') }} f
             inner join {{ ref ('tickers') }} as t
    on f.code = t.symbol
)
select symbol,
       key::date                                                  as date,
       (value ->> 'netIncome')::float                             as net_income,
       (value ->> 'investments')::float                           as investments,
       (value ->> 'changeInCash')::float                          as change_in_cash,
       (value ->> 'depreciation')::float                          as depreciation,
       (value ->> 'freeCashFlow')::float                          as free_cash_flow,
       ABS((value ->> 'dividendsPaid')::float)                    as dividends_paid,
       (value ->> 'netBorrowings')::float                         as net_borrowings,
       NULLIF((value ->> 'currency_symbol')::varchar, 'USD')      as currency_symbol,
       (value ->> 'changeReceivables')::float                     as change_receivables,
       (value ->> 'changeToInventory')::float                     as change_to_inventory,
       (value ->> 'changeToNetincome')::float                     as change_to_netincome,
       (value ->> 'endPeriodCashFlow')::float                     as end_period_cash_flow,
       (value ->> 'otherNonCashItems')::float                     as other_non_cash_items,
       (value ->> 'beginPeriodCashFlow')::float                   as begin_period_cash_flow,
       (value ->> 'capitalExpenditures')::float                   as capital_expenditures,
       (value ->> 'changeToLiabilities')::float                   as change_to_liabilities,
       (value ->> 'exchangeRateChanges')::float                   as exchange_rate_changes,
       (value ->> 'salePurchaseOfStock')::float                   as sale_purchase_of_stock,
       (value ->> 'changeInWorkingCapital')::float                as change_in_working_capital,
       (value ->> 'cashFlowsOtherOperating')::float               as cash_flows_other_operating,
       (value ->> 'changeToAccountReceivables')::float            as change_to_account_receivables,
       (value ->> 'changeToOperatingActivities')::float           as change_to_operating_activities,
       (value ->> 'cashAndCashEquivalentsChanges')::float         as cash_and_cash_equivalents_changes,
       (value ->> 'totalCashFromFinancingActivities')::float      as total_cash_from_financing_activities,
       (value ->> 'totalCashFromOperatingActivities')::float      as total_cash_from_operating_activities,
       (value ->> 'otherCashflowsFromFinancingActivities')::float as other_cashflows_from_financing_activities,
       (value ->> 'otherCashflowsFromInvestingActivities')::float as other_cashflows_from_investing_activities,
       (value ->> 'totalCashflowsFromInvestingActivities')::float as total_cashflows_from_investing_activities
from expanded_quaterly_cash_flow
where key != '0000-00-00'
