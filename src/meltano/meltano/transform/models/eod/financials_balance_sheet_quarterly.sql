{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index('id', true),
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
                    (json_each((financials -> 'Balance_Sheet'->>'quarterly')::json)).*,
                    case
                        when is_date(updatedat)
                            then updatedat::timestamp
                        else _sdc_batched_at
                        end as updated_at
             from {{ source('eod', 'eod_fundamentals') }}
         )
select expanded.symbol,
       (expanded.symbol || '_' || (value->>'date'))                          as id,
--        (value->>'accountsPayable')::numeric                                  as accounts_payable,
--        (value->>'accumulatedAmortization')::numeric                          as accumulated_amortization,
--        (value->>'accumulatedDepreciation')::numeric                          as accumulated_depreciation,
--        (value->>'accumulatedOtherComprehensiveIncome')::numeric              as accumulated_other_comprehensive_income,
--        (value->>'additionalPaidInCapital')::numeric                          as additional_paid_in_capital,
--        (value->>'capitalLeaseObligations')::numeric                          as capital_lease_obligations,
--        (value->>'capitalSurpluse')::numeric                                  as capital_surpluse,
       (value->>'cash')::numeric                                             as cash,
--        (value->>'cashAndShortTermInvestments')::numeric                      as cash_and_short_term_investments,
--        (value->>'commonStock')::numeric                                      as common_stock,
--        (value->>'commonStockSharesOutstanding')::numeric                     as common_stock_shares_outstanding,
--        (value->>'commonStockTotalEquity')::numeric                           as common_stock_total_equity,
--        (value->>'currency_symbol')::varchar                                  as currency_symbol,
       (value->>'date')::date                                                as date,
--        (value->>'deferredLongTermAssetCharges')::numeric                     as deferred_long_term_asset_charges,
--        (value->>'deferredLongTermLiab')::numeric                             as deferred_long_term_liab,
--        (value->>'earningAssets')::numeric                                    as earning_assets,
--        (value->>'filing_date')::date                                         as filing_date,
--        (value->>'goodWill')::numeric                                         as good_will,
--        (value->>'intangibleAssets')::numeric                                 as intangible_assets,
--        (value->>'inventory')::numeric                                        as inventory,
--        (value->>'liabilitiesAndStockholdersEquity')::numeric                 as liabilities_and_stockholders_equity,
--        (value->>'longTermDebt')::numeric                                     as long_term_debt,
--        (value->>'longTermDebtTotal')::numeric                                as long_term_debt_total,
--        (value->>'longTermInvestments')::numeric                              as long_term_investments,
--        (value->>'negativeGoodwill')::numeric                                 as negative_goodwill,
       (value->>'netDebt')::numeric                                          as net_debt,
--        (value->>'netInvestedCapital')::numeric                               as net_invested_capital,
--        (value->>'netReceivables')::numeric                                   as net_receivables,
--        (value->>'netTangibleAssets')::numeric                                as net_tangible_assets,
--        (value->>'netWorkingCapital')::numeric                                as net_working_capital,
--        (value->>'nonCurrentAssetsTotal')::numeric                            as non_current_assets_total,
--        (value->>'nonCurrentLiabilitiesOther')::numeric                       as non_current_liabilities_other,
--        (value->>'nonCurrentLiabilitiesTotal')::numeric                       as non_current_liabilities_total,
--        (value->>'nonCurrrentAssetsOther')::numeric                           as non_currrent_assets_other,
--        (value->>'noncontrollingInterestInConsolidatedEntity')::numeric       as noncontrolling_interest_in_consolidated_entity,
--        (value->>'otherAssets')::numeric                                      as other_assets,
--        (value->>'otherCurrentAssets')::numeric                               as other_current_assets,
--        (value->>'otherCurrentLiab')::numeric                                 as other_current_liab,
--        (value->>'otherLiab')::numeric                                        as other_liab,
--        (value->>'otherStockholderEquity')::numeric                           as other_stockholder_equity,
--        (value->>'preferredStockRedeemable')::numeric                         as preferred_stock_redeemable,
--        (value->>'preferredStockTotalEquity')::numeric                        as preferred_stock_total_equity,
--        (value->>'propertyPlantAndEquipmentGross')::numeric                   as property_plant_and_equipment_gross,
--        (value->>'propertyPlantAndEquipmentNet')::numeric                     as property_plant_and_equipment_net,
--        (value->>'propertyPlantEquipment')::numeric                           as property_plant_equipment,
--        (value->>'retainedEarnings')::numeric                                 as retained_earnings,
--        (value->>'retainedEarningsTotalEquity')::numeric                      as retained_earnings_total_equity,
--        (value->>'shortLongTermDebt')::numeric                                as short_long_term_debt,
--        (value->>'shortLongTermDebtTotal')::numeric                           as short_long_term_debt_total,
--        (value->>'shortTermDebt')::numeric                                    as short_term_debt,
--        (value->>'shortTermInvestments')::numeric                             as short_term_investments,
--        (value->>'temporaryEquityRedeemableNoncontrollingInterests')::numeric as temporary_equity_redeemable_noncontrolling_interests,
       (value->>'totalAssets')::numeric                                      as total_assets,
--        (value->>'totalCurrentAssets')::numeric                               as total_current_assets,
--        (value->>'totalCurrentLiabilities')::numeric                          as total_current_liabilities,
--        (value->>'totalLiab')::numeric                                        as total_liab,
--        (value->>'totalPermanentEquity')::numeric                             as total_permanent_equity,
--        (value->>'totalStockholderEquity')::numeric                           as total_stockholder_equity,
--        (value->>'treasuryStock')::numeric                                    as treasury_stock,
--        (value->>'warrants')::numeric                                         as warrants,
       updated_at                                                            as updated_at
from expanded
{% if is_incremental() %}
    left join max_updated_at on expanded.symbol = max_updated_at.symbol
    where key::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}