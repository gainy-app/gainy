{{
  config(
    materialized = "table",
    dist = "symbol",
    post_hook=[
      pk('symbol, date'),
    ]
  )
}}

    with expanded_trends as (
        select code as symbol,
               (json_each((earnings -> 'Trend')::json)).*,
               updatedat::date as updated_at
        from {{ source('eod', 'eod_fundamentals') }} f
                 inner join {{  ref('tickers') }} as t on f.code = t.symbol
    )
    select symbol,
           key::date                                             as date,
           (value ->> 'growth')::float                           as growth,
           (value ->> 'period')::text                            as period,
           (value ->> 'epsTrendCurrent')::float                  as eps_trend_current,
           (value ->> 'epsTrend7daysAgo')::float                 as eps_trend_7days_ago,
           (value ->> 'epsTrend30daysAgo')::float                as eps_trend_30days_ago,
           (value ->> 'epsTrend60daysAgo')::float                as eps_trend_60days_ago,
           (value ->> 'epsTrend90daysAgo')::float                as eps_trend_90days_ago,
           (value ->> 'revenueEstimateAvg')::float               as revenue_estimate_avg,
           (value ->> 'revenueEstimateLow')::float               as revenue_estimate_low,
           (value ->> 'earningsEstimateAvg')::float              as earnings_estimate_avg,
           (value ->> 'earningsEstimateLow')::float              as earnings_estimate_low,
           (value ->> 'revenueEstimateHigh')::float              as revenue_estimate_high,
           (value ->> 'earningsEstimateHigh')::float             as erarnings_estimate_high,
           (value ->> 'revenueEstimateGrowth')::float            as revenue_estimate_growth,
           (value ->> 'earningsEstimateGrowth')::float           as eranings_estimate_growth,
           (value ->> 'epsRevisionsUpLast7days')::float          as eps_revisions_ul_past_7days,
           (value ->> 'epsRevisionsUpLast30days')::float         as eps_revisions_ul_past_30days,
           (value ->> 'revenueEstimateYearAgoEps')::float        as revenue_estimate_year_ago_eps,
           (value ->> 'earningsEstimateYearAgoEps')::float       as earnings_estimate_year_ago_eps,
           (value ->> 'epsRevisionsDownLast30days')::float       as eps_revisions_down_last_30days,
           (value ->> 'epsRevisionsDownLast90days')::float       as eps_revisions_down_last_90days,
           (value ->> 'revenueEstimateNumberOfAnalysts')::float  as revenue_estimate_number_of_analysts,
           (value ->> 'earningsEstimateNumberOfAnalysts')::float as earnings_estimate_number_of_analysts,
           updated_at
    from expanded_trends
    where key != '0000-00-00'
