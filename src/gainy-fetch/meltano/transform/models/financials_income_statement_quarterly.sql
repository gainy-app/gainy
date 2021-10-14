{{
  config(
    materialized = "incremental",
    unique_key = "symbol",
    incremental_strategy='insert_overwrite',
    post_hook=[
      index(this, 'symbol', false),
    ]
  )
}}

with
--      max_updated_at as
--     (select max(updated_at) as date from {{ this }}),
     expanded as
    (
    select code as symbol,
           (json_each((financials -> 'Income_Statement' ->> 'quarterly')::json)).*,
           updatedat::date as updated_at
    from {{ source('eod', 'fundamentals') }} f
             inner join {{ ref('tickers') }} as t on f.code = t.symbol
-- {% if is_incremental() %}
--     join max_updated_at on true
--     where updated_at::date >= max_updated_at.date
-- {% endif %}
)
select symbol,
       key::date                                              as date,
       (value ->> 'ebit')::float                              as ebit,
       (value ->> 'ebitda')::text                             as ebitda,
       (value ->> 'netIncome')::float                         as net_income,
       (value ->> 'otherItems')::float                        as other_items,
       (value ->> 'filing_date')::timestamp                   as filing_date,
       (value ->> 'grossProfit')::float                       as gross_profit,
       (value ->> 'nonRecurring')::float                      as non_recurring,
       (value ->> 'taxProvision')::float                      as tax_provision,
       (value ->> 'totalRevenue')::float                      as total_revenue,
       (value ->> 'costOfRevenue')::float                     as cost_of_revenue,
       (value ->> 'interestIncome')::float                    as interest_income,
       (value ->> 'currency_symbol')::varchar                 as currency_symbol,
       (value ->> 'incomeBeforeTax')::float                   as income_before_tax,
       (value ->> 'interestExpense')::float                   as interest_expense,
       (value ->> 'operatingIncome')::float                   as operating_income,
       (value ->> 'incomeTaxExpense')::float                  as income_tax_expense,
       (value ->> 'minorityInterest')::float                  as minorityInterest,
       (value ->> 'netInterestIncome')::float                 as net_interest_income,
       (value ->> 'extraordinaryItems')::float                as extraordinary_items,
       (value ->> 'researchDevelopment')::float               as research_development,
       (value ->> 'discontinuedOperations')::float            as discontinued_operations,
       (value ->> 'otherOperatingExpenses')::float            as other_operating_expenses,
       (value ->> 'reconciledDepreciation')::float            as reconciled_depreciation,
       (value ->> 'totalOperatingExpenses')::float            as total_operating_expenses,
       (value ->> 'effectOfAccountingCharges')::float         as effect_of_accounting_charges,
       (value ->> 'netIncomeFromContinuingOps')::float        as net_income_from_continuing_ops,
       (value ->> 'nonOperatingIncomeNetOther')::float        as non_operating_income_net_other,
       (value ->> 'totalOtherIncomeExpenseNet')::float        as total_other_income_expense_net,
       (value ->> 'depreciationAndAmortization')::float       as depreciation_and_amortization,
       (value ->> 'sellingAndMarketingExpenses')::float       as selling_and_marketing_expenses,
       (value ->> 'sellingGeneralAdministrative')::float      as selling_general_administrative,
       (value ->> 'netIncomeApplicableToCommonShares')::float as net_income_applicable_to_common_shares,
       (value ->> 'preferredStockAndOtherAdjustments')::float as preferred_stock_and_other_adjustments,
       updated_at                                             as updated_at
from expanded