{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol'),
    ]
  )
}}

with expanded_earnings_history as
         (
             select *,
                    (eps_difference > 0)::int as eps_difference_positive
             from {{ ref('earnings_history') }}
             where eps_difference is not null
               and report_date < now()
             order by date desc
         ),
     beaten_quarterly_eps_estimation_count_ttm as
         (
             select distinct on (
                 symbol
                 ) symbol as code,
                   sum(eps_difference_positive)
                   over (partition by symbol order by report_date desc rows between current row and 3 following) as value
             from expanded_earnings_history
             order by expanded_earnings_history.symbol, expanded_earnings_history.date desc
         )
select distinct on (code)
    code                                                           as symbol,
    (highlights ->> 'EBITDA')::numeric                             as EBITDA,
    (highlights ->> 'PERatio')::numeric                            as pe_ratio,
    (highlights ->> 'PEGRatio')::numeric                           as peg_ratio,
    (highlights ->> 'BookValue')::numeric                          as book_value,
    (highlights ->> 'RevenueTTM')::numeric                         as revenue_ttm,
    (highlights ->> 'ProfitMargin')::numeric                       as profit_margin,
    (highlights ->> 'DilutedEpsTTM')::numeric                      as diluted_eps_ttm,
    (highlights ->> 'DividendShare')::numeric                      as dividend_share,
    (highlights ->> 'DividendYield')::numeric                      as dividend_yield,
    (highlights ->> 'EarningsShare')::numeric                      as earnings_share,
    (highlights ->> 'GrossProfitTTM')::numeric                     as gross_profit_ttm,
    NULLIF(highlights ->> 'MostRecentQuarter', '0000-00-00')::date as most_recent_quarter,
    (highlights ->> 'ReturnOnAssetsTTM')::numeric                  as return_on_assets_ttm,
    (highlights ->> 'ReturnOnEquityTTM')::numeric                  as return_on_equity_ttm,
    (highlights ->> 'OperatingMarginTTM')::numeric                 as operating_margin_ttm,
    (highlights ->> 'RevenuePerShareTTM')::numeric                 as revenue_per_share_ttm,
    (highlights ->> 'EPSEstimateNextYear')::numeric                as eps_estimate_next_year,
    (highlights ->> 'MarketCapitalization')::numeric               as market_capitalization,
    (highlights ->> 'WallStreetTargetPrice')::numeric              as wall_street_target_price,
    (highlights ->> 'EPSEstimateCurrentYear')::numeric             as eps_estimate_current_year,
    (highlights ->> 'EPSEstimateNextQuarter')::numeric             as eps_estimate_next_quarter,
    (highlights ->> 'MarketCapitalizationMln')::numeric            as market_capitalization_mln,
    (highlights ->> 'EPSEstimateCurrentQuarter')::numeric          as eps_estimate_current_quarter,
    (highlights ->> 'QuarterlyRevenueGrowthYOY')::numeric          as quarterly_revenue_growth_yoy,
    (highlights ->> 'QuarterlyEarningsGrowthYOY')::numeric         as quarterly_earnings_growth_yoy,
    beaten_quarterly_eps_estimation_count_ttm.value::int           as beaten_quarterly_eps_estimation_count_ttm,
    case
        when is_date(updatedat)
            then updatedat::timestamp
        else _sdc_batched_at
        end                                                        as updated_at
from {{ source('eod', 'eod_fundamentals') }}
left join beaten_quarterly_eps_estimation_count_ttm using (code)
where (highlights ->> 'MarketCapitalization') is not null
