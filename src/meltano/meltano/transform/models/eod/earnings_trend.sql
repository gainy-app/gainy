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
        select code    as symbol,
               (json_each((earnings -> 'Trend')::json)).*,
               case
                   when is_date(updatedat)
                       then updatedat::timestamp
                   else _sdc_batched_at
                   end as updated_at
        from {{ source('eod', 'eod_fundamentals') }}
    )
    select symbol,
           key::date                                               as date,
           (value ->> 'growth')::numeric                           as growth,
           (value ->> 'period')::text                              as period,
           (value ->> 'epsTrendCurrent')::numeric                  as eps_trend_current,
           (value ->> 'epsTrend7daysAgo')::numeric                 as eps_trend_7days_ago,
           (value ->> 'epsTrend30daysAgo')::numeric                as eps_trend_30days_ago,
           (value ->> 'epsTrend60daysAgo')::numeric                as eps_trend_60days_ago,
           (value ->> 'epsTrend90daysAgo')::numeric                as eps_trend_90days_ago,
           (value ->> 'revenueEstimateAvg')::numeric               as revenue_estimate_avg,
           (value ->> 'revenueEstimateLow')::numeric               as revenue_estimate_low,
           (value ->> 'earningsEstimateAvg')::numeric              as earnings_estimate_avg,
           (value ->> 'earningsEstimateLow')::numeric              as earnings_estimate_low,
           (value ->> 'revenueEstimateHigh')::numeric              as revenue_estimate_high,
           (value ->> 'earningsEstimateHigh')::numeric             as erarnings_estimate_high,
           (value ->> 'revenueEstimateGrowth')::numeric            as revenue_estimate_growth,
           (value ->> 'earningsEstimateGrowth')::numeric           as eranings_estimate_growth,
           (value ->> 'epsRevisionsUpLast7days')::numeric          as eps_revisions_ul_past_7days,
           (value ->> 'epsRevisionsUpLast30days')::numeric         as eps_revisions_ul_past_30days,
           (value ->> 'revenueEstimateYearAgoEps')::numeric        as revenue_estimate_year_ago_eps,
           (value ->> 'earningsEstimateYearAgoEps')::numeric       as earnings_estimate_year_ago_eps,
           (value ->> 'epsRevisionsDownLast30days')::numeric       as eps_revisions_down_last_30days,
           (value ->> 'epsRevisionsDownLast90days')::numeric       as eps_revisions_down_last_90days,
           (value ->> 'revenueEstimateNumberOfAnalysts')::numeric  as revenue_estimate_number_of_analysts,
           (value ->> 'earningsEstimateNumberOfAnalysts')::numeric as earnings_estimate_number_of_analysts,
           updated_at
    from expanded_trends
    where key != '0000-00-00'
