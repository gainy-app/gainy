{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index(this, 'id', true),
    ]
  )
}}


with
{% if is_incremental() %}
     max_updated_at as (select symbol, max(date) as max_date from {{ this }} group by symbol),
{% endif %}
     expanded as
         (
             select code    as symbol,
                    (json_each((financials -> 'Income_Statement'->>'quarterly')::json)).*,
                    case
                        when is_date(updatedat)
                            then updatedat::timestamp
                        else _sdc_batched_at
                        end as updated_at
             from {{ source('eod', 'eod_fundamentals') }}
         )
select expanded.symbol,
       key::date                                              as date,
       (expanded.symbol || '_' || key)                        as id,
--        (value->>'ebit')::numeric                              as ebit,
       (value->>'ebitda')::numeric                            as ebitda,
       (value->>'netIncome')::numeric                         as net_income,
--        (value->>'otherItems')::numeric                        as other_items,
--        (value->>'filing_date')::timestamp                     as filing_date,
       (value->>'grossProfit')::numeric                       as gross_profit,
--        (value->>'nonRecurring')::numeric                      as non_recurring,
--        (value->>'taxProvision')::numeric                      as tax_provision,
       (value->>'totalRevenue')::numeric                      as total_revenue,
       (value->>'costOfRevenue')::numeric                     as cost_of_revenue,
--        (value->>'interestIncome')::numeric                    as interest_income,
--        (value->>'currency_symbol')::varchar                   as currency_symbol,
--        (value->>'incomeBeforeTax')::numeric                   as income_before_tax,
--        (value->>'interestExpense')::numeric                   as interest_expense,
--        (value->>'operatingIncome')::numeric                   as operating_income,
--        (value->>'incomeTaxExpense')::numeric                  as income_tax_expense,
--        (value->>'minorityInterest')::numeric                  as minorityInterest,
--        (value->>'netInterestIncome')::numeric                 as net_interest_income,
--        (value->>'extraordinaryItems')::numeric                as extraordinary_items,
--        (value->>'researchDevelopment')::numeric               as research_development,
--        (value->>'discontinuedOperations')::numeric            as discontinued_operations,
--        (value->>'otherOperatingExpenses')::numeric            as other_operating_expenses,
--        (value->>'reconciledDepreciation')::numeric            as reconciled_depreciation,
--        (value->>'totalOperatingExpenses')::numeric            as total_operating_expenses,
--        (value->>'effectOfAccountingCharges')::numeric         as effect_of_accounting_charges,
--        (value->>'netIncomeFromContinuingOps')::numeric        as net_income_from_continuing_ops,
--        (value->>'nonOperatingIncomeNetOther')::numeric        as non_operating_income_net_other,
--        (value->>'totalOtherIncomeExpenseNet')::numeric        as total_other_income_expense_net,
--        (value->>'depreciationAndAmortization')::numeric       as depreciation_and_amortization,
--        (value->>'sellingAndMarketingExpenses')::numeric       as selling_and_marketing_expenses,
--        (value->>'sellingGeneralAdministrative')::numeric      as selling_general_administrative,
--        (value->>'netIncomeApplicableToCommonShares')::numeric as net_income_applicable_to_common_shares,
--        (value->>'preferredStockAndOtherAdjustments')::numeric as preferred_stock_and_other_adjustments,
       updated_at                                             as updated_at
from expanded
{% if is_incremental() %}
    left join max_updated_at on expanded.symbol = max_updated_at.symbol
    where key::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
