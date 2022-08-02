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
                    (json_each((financials -> 'Balance_Sheet' ->> 'quarterly')::json)).*,
                    case
                        when is_date(updatedat)
                            then updatedat::timestamp
                        else _sdc_batched_at
                        end as updated_at
             from {{ source('eod', 'eod_fundamentals') }} f
             inner join {{ ref('tickers') }} as t
             on f.code = t.symbol
         )
select expanded.symbol,
       (expanded.symbol || '_' || (value ->> 'date'))::varchar               as id,
       (value ->> 'accountsPayable')::float                                  as accounts_payable,
       (value ->> 'accumulatedAmortization')::float                          as accumulated_amortization,
       (value ->> 'accumulatedDepreciation')::float                          as accumulated_depreciation,
       (value ->> 'accumulatedOtherComprehensiveIncome')::float              as accumulated_other_comprehensive_income,
       (value ->> 'additionalPaidInCapital')::float                          as additional_paid_in_capital,
       (value ->> 'capitalLeaseObligations')::float                          as capital_lease_obligations,
       (value ->> 'capitalSurpluse')::float                                  as capital_surpluse,
       (value ->> 'cash')::float                                             as cash,
       (value ->> 'cashAndShortTermInvestments')::float                      as cash_and_short_term_investments,
       (value ->> 'commonStock')::float                                      as common_stock,
       (value ->> 'commonStockSharesOutstanding')::float                     as common_stock_shares_outstanding,
       (value ->> 'commonStockTotalEquity')::float                           as common_stock_total_equity,
       (value ->> 'currency_symbol')::varchar                                as currency_symbol,
       (value ->> 'date')::date                                              as date,
       (value ->> 'deferredLongTermAssetCharges')::float                     as deferred_long_term_asset_charges,
       (value ->> 'deferredLongTermLiab')::float                             as deferred_long_term_liab,
       (value ->> 'earningAssets')::float                                    as earning_assets,
       (value ->> 'filing_date')::date                                       as filing_date,
       (value ->> 'goodWill')::float                                         as good_will,
       (value ->> 'intangibleAssets')::float                                 as intangible_assets,
       (value ->> 'inventory')::float                                        as inventory,
       (value ->> 'liabilitiesAndStockholdersEquity')::float                 as liabilities_and_stockholders_equity,
       (value ->> 'longTermDebt')::float                                     as long_term_debt,
       (value ->> 'longTermDebtTotal')::float                                as long_term_debt_total,
       (value ->> 'longTermInvestments')::float                              as long_term_investments,
       (value ->> 'negativeGoodwill')::float                                 as negative_goodwill,
       (value ->> 'netDebt')::float                                          as net_debt,
       (value ->> 'netInvestedCapital')::float                               as net_invested_capital,
       (value ->> 'netReceivables')::float                                   as net_receivables,
       (value ->> 'netTangibleAssets')::float                                as net_tangible_assets,
       (value ->> 'netWorkingCapital')::float                                as net_working_capital,
       (value ->> 'nonCurrentAssetsTotal')::float                            as non_current_assets_total,
       (value ->> 'nonCurrentLiabilitiesOther')::float                       as non_current_liabilities_other,
       (value ->> 'nonCurrentLiabilitiesTotal')::float                       as non_current_liabilities_total,
       (value ->> 'nonCurrrentAssetsOther')::float                           as non_currrent_assets_other,
       (value ->> 'noncontrollingInterestInConsolidatedEntity')::float       as noncontrolling_interest_in_consolidated_entity,
       (value ->> 'otherAssets')::float                                      as other_assets,
       (value ->> 'otherCurrentAssets')::float                               as other_current_assets,
       (value ->> 'otherCurrentLiab')::float                                 as other_current_liab,
       (value ->> 'otherLiab')::float                                        as other_liab,
       (value ->> 'otherStockholderEquity')::float                           as other_stockholder_equity,
       (value ->> 'preferredStockRedeemable')::float                         as preferred_stock_redeemable,
       (value ->> 'preferredStockTotalEquity')::float                        as preferred_stock_total_equity,
       (value ->> 'propertyPlantAndEquipmentGross')::float                   as property_plant_and_equipment_gross,
       (value ->> 'propertyPlantAndEquipmentNet')::float                     as property_plant_and_equipment_net,
       (value ->> 'propertyPlantEquipment')::float                           as property_plant_equipment,
       (value ->> 'retainedEarnings')::float                                 as retained_earnings,
       (value ->> 'retainedEarningsTotalEquity')::float                      as retained_earnings_total_equity,
       (value ->> 'shortLongTermDebt')::float                                as short_long_term_debt,
       (value ->> 'shortLongTermDebtTotal')::float                           as short_long_term_debt_total,
       (value ->> 'shortTermDebt')::float                                    as short_term_debt,
       (value ->> 'shortTermInvestments')::float                             as short_term_investments,
       (value ->> 'temporaryEquityRedeemableNoncontrollingInterests')::float as temporary_equity_redeemable_noncontrolling_interests,
       (value ->> 'totalAssets')::float                                      as total_assets,
       (value ->> 'totalCurrentAssets')::float                               as total_current_assets,
       (value ->> 'totalCurrentLiabilities')::float                          as total_current_liabilities,
       (value ->> 'totalLiab')::float                                        as total_liab,
       (value ->> 'totalPermanentEquity')::float                             as total_permanent_equity,
       (value ->> 'totalStockholderEquity')::float                           as total_stockholder_equity,
       (value ->> 'treasuryStock')::float                                    as treasury_stock,
       (value ->> 'warrants')::float                                         as warrants,
       updated_at                                                            as updated_at
from expanded
{% if is_incremental() %}
    left join max_updated_at on expanded.symbol = max_updated_at.symbol
    where key::date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}