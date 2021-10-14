{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      index(this, 'symbol', true),
    ]
  )
}}

    select distinct code                                                           as symbol,
           (highlights ->> 'EBITDA')::real                               as EBITDA,
           (highlights ->> 'PERatio')::real                              as pe_ratio,
           (highlights ->> 'PEGRatio')::real                             as peg_ratio,
           (highlights ->> 'BookValue')::real                            as book_value,
           (highlights ->> 'RevenueTTM')::real                           as revenue_ttm,
           (highlights ->> 'ProfitMargin')::real                         as profit_margin,
           (highlights ->> 'DilutedEpsTTM')::real                        as diluted_eps_ttm,
           (highlights ->> 'DividendShare')::real                        as dividend_share,
           (highlights ->> 'DividendYield')::real                        as dividend_yield,
           (highlights ->> 'EarningsShare')::real                        as earnings_share,
           (highlights ->> 'GrossProfitTTM')::real                       as gross_profit_ttm,
           NULLIF(highlights ->> 'MostRecentQuarter', '0000-00-00')::date as most_recent_quarter,
           (highlights ->> 'ReturnOnAssetsTTM')::real                    as return_on_assets_ttm,
           (highlights ->> 'ReturnOnEquityTTM')::real                    as return_on_equity_ttm,
           (highlights ->> 'OperatingMarginTTM')::real                   as operating_margin_ttm,
           (highlights ->> 'RevenuePerShareTTM')::real                   as revenue_per_share_ttm,
           (highlights ->> 'EPSEstimateNextYear')::real                  as eps_estimate_next_year,
           (highlights ->> 'MarketCapitalization')::real                 as market_capitalization,
           (highlights ->> 'WallStreetTargetPrice')::real                as wall_street_target_price,
           (highlights ->> 'EPSEstimateCurrentYear')::real               as eps_estimate_current_year,
           (highlights ->> 'EPSEstimateNextQuarter')::real               as eps_estimate_next_quarter,
           (highlights ->> 'MarketCapitalizationMln')::real              as market_capitalization_mln,
           (highlights ->> 'EPSEstimateCurrentQuarter')::real            as eps_estimate_current_quarter,
           (highlights ->> 'QuarterlyRevenueGrowthYOY')::real            as quarterly_revenue_growth_yoy,
           (highlights ->> 'QuarterlyEarningsGrowthYOY')::real           as quarterly_earnings_growth_yoy

   from {{ source('eod', 'fundamentals') }} f inner join {{  ref('tickers') }} as t on f.code = t.symbol
