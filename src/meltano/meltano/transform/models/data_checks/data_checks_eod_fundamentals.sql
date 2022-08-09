{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


with errors as
    (
         select distinct on (symbol)
             symbol,
             'analyst_ratings'                                       as code,
             'daily'                                                 as "period",
             'Ticker ' || symbol || ' has incorrect analyst_ratings' as message
         from {{ ref('analyst_ratings') }}
         where buy < 0
            or hold < 0
            or sell < 0
            or rating < 0
            or rating > 6
            or strong_buy < 0
            or strong_sell < 0
            or target_price < 0

         union all

         select distinct on (symbol)
             symbol,
             'highlights'                                   as code,
             'daily'                                        as "period",
             'Ticker '||symbol||' has incorrect highlights' as message
         from {{ ref('highlights') }}
         where market_capitalization <= 0
            or dividend_yield < 0
            or dividend_share < 0
            or beaten_quarterly_eps_estimation_count_ttm < 0

         union all

         select symbol,
                'ticker_components'               as code,
                'daily'                           as "period",
                'ETF ' || symbol || ' has incorrect components weight '
                    || json_agg(component_symbol) as message
         from {{ ref('ticker_components') }}
         where component_weight <= 0
         group by symbol

         union all

         select symbol,
                'ticker_shares_stats'                                       as code,
                'daily'                                                     as "period",
                'Ticker ' || symbol || ' has incorrect ticker_shares_stats' as message
         from {{ ref('ticker_shares_stats') }}
         where short_ratio < 0
            or shares_float < 0
            or shares_short < 0
            or shares_outstanding < 0
            or short_percent_outstanding < 0

         union all

         select symbol,
                'valuation'                                            as code,
                'daily'                                                as "period",
                'Ticker ' || symbol || ' has incorrect valuation data' as message
         from {{ ref('valuation') }}
         where forward_pe < 0
            or price_sales_ttm < 0

         union all

         select code as symbol,
                'eod_fundamentals'                                      as code,
                'daily'                                                 as "period",
                'Ticker ' || symbol || ' has incorrect splitsdividends' as message
         from {{ source('eod', 'eod_fundamentals') }}
         where (splitsdividends ->> 'PayoutRatio')::numeric < 0
            or (splitsdividends ->> 'ForwardAnnualDividendYield')::numeric < 0
    )
select (code || '_' || symbol) as id,
       symbol,
       code,
       period,
       message,
       now() as updated_at
from errors