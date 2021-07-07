{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      "{{ index(this, 'symbol', true)}}",
      "{{ fk_constraint(this, 'symbol', 'tickers', 'symbol') }}"
    ]
  )
}}

    select distinct code                                                           as symbol,
           (highlights ->> 'EBITDA')::float                               as EBITDA,
           (highlights ->> 'PERatio')::float                              as pe_ratio,
           (highlights ->> 'PEGRatio')::float                             as peg_ratio,
           (highlights ->> 'BookValue')::float                            as book_value,
           (highlights ->> 'RevenueTTM')::float                           as revenue_ttm,
           (highlights ->> 'ProfitMargin')::float                         as profit_margin,
           (highlights ->> 'DilutedEpsTTM')::float                        as diluted_eps_ttm,
           (highlights ->> 'DividendShare')::float                        as dividend_share,
           (highlights ->> 'DividendYield')::float                        as dividend_yield,
           (highlights ->> 'EarningsShare')::float                        as earnings_share,
           (highlights ->> 'GrossProfitTTM')::float                       as gross_profit_ttm,
           NULLIF(highlights ->> 'MostRecentQuarter', '0000-00-00')::date as most_recent_quarter,
           (highlights ->> 'ReturnOnAssetsTTM')::float                    as return_on_assets_ttm,
           (highlights ->> 'ReturnOnEquityTTM')::float                    as return_on_equity_ttm,
           (highlights ->> 'OperatingMarginTTM')::float                   as operating_margin_ttm,
           (highlights ->> 'RevenuePerShareTTM')::float                   as revenue_per_share_ttm,
           (highlights ->> 'EPSEstimateNextYear')::float                  as eps_estimate_next_year,
           (highlights ->> 'MarketCapitalization')::float                 as market_capitalization,
           (highlights ->> 'WallStreetTargetPrice')::float                as wall_street_target_price,
           (highlights ->> 'EPSEstimateCurrentYear')::float               as eps_estimate_current_year,
           (highlights ->> 'EPSEstimateNextQuarter')::float               as eps_estimate_next_quarter,
           (highlights ->> 'MarketCapitalizationMln')::float              as market_capitalization_mln,
           (highlights ->> 'EPSEstimateCurrentQuarter')::float            as eps_estimate_current_quarter,
           (highlights ->> 'QuarterlyRevenueGrowthYOY')::float            as quarterly_revenue_growth_yoy,
           (highlights ->> 'QuarterlyEarningsGrowthYOY')::float           as quarterly_earnings_growth_yoy

   from fundamentals f inner join {{  ref('tickers') }} as t on f.code = t.symbol
