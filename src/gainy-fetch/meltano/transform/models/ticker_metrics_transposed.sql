{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists {{ get_index_name(this, "symbol__metric_id") }} (symbol, metric_id)',
    ]
  )
}}

with ticker_metrics as (select * from {{ ref('ticker_metrics') }}),
     raw_metrics_double as
         (
             select symbol, 'profit_margin' as metric_id, profit_margin as value from ticker_metrics
             union all
             select symbol, 'avg_volume_10d' as metric_id, avg_volume_10d as value from ticker_metrics
             union all
             select symbol, 'avg_volume_90d' as metric_id, avg_volume_90d as value from ticker_metrics
             union all
             select symbol, 'short_ratio' as metric_id, short_ratio as value from ticker_metrics
             union all
             select symbol, 'short_percent' as metric_id, short_percent as value from ticker_metrics
             union all
             select symbol, 'beta' as metric_id, beta as value from ticker_metrics
             union all
             select symbol, 'absolute_historical_volatility_adjusted_current' as metric_id, absolute_historical_volatility_adjusted_current as value from ticker_metrics
             union all
             select symbol, 'relative_historical_volatility_adjusted_current' as metric_id, relative_historical_volatility_adjusted_current as value from ticker_metrics
             union all
             select symbol, 'absolute_historical_volatility_adjusted_min_1y' as metric_id, absolute_historical_volatility_adjusted_min_1y as value from ticker_metrics
             union all
             select symbol, 'absolute_historical_volatility_adjusted_max_1y' as metric_id, absolute_historical_volatility_adjusted_max_1y as value from ticker_metrics
             union all
             select symbol, 'relative_historical_volatility_adjusted_min_1y' as metric_id, relative_historical_volatility_adjusted_min_1y as value from ticker_metrics
             union all
             select symbol, 'relative_historical_volatility_adjusted_max_1y' as metric_id, relative_historical_volatility_adjusted_max_1y as value from ticker_metrics
             union all
             select symbol, 'implied_volatility' as metric_id, implied_volatility as value from ticker_metrics
             union all
             select symbol, 'revenue_growth_yoy' as metric_id, revenue_growth_yoy as value from ticker_metrics
             union all
             select symbol, 'revenue_growth_fwd' as metric_id, revenue_growth_fwd as value from ticker_metrics
             union all
             select symbol, 'ebitda_growth_yoy' as metric_id, ebitda_growth_yoy as value from ticker_metrics
             union all
             select symbol, 'eps_growth_yoy' as metric_id, eps_growth_yoy as value from ticker_metrics
             union all
             select symbol, 'eps_growth_fwd' as metric_id, eps_growth_fwd as value from ticker_metrics
             union all
             select symbol, 'enterprise_value_to_sales' as metric_id, enterprise_value_to_sales as value from ticker_metrics
             union all
             select symbol, 'price_to_earnings_ttm' as metric_id, price_to_earnings_ttm as value from ticker_metrics
             union all
             select symbol, 'price_to_sales_ttm' as metric_id, price_to_sales_ttm as value from ticker_metrics
             union all
             select symbol, 'price_to_book_value' as metric_id, price_to_book_value as value from ticker_metrics
             union all
             select symbol, 'enterprise_value_to_ebitda' as metric_id, enterprise_value_to_ebitda as value from ticker_metrics
             union all
             select symbol, 'price_change_1m' as metric_id, price_change_1m as value from ticker_metrics
             union all
             select symbol, 'price_change_3m' as metric_id, price_change_3m as value from ticker_metrics
             union all
             select symbol, 'price_change_1y' as metric_id, price_change_1y as value from ticker_metrics
             union all
             select symbol, 'dividend_yield' as metric_id, dividend_yield as value from ticker_metrics
             union all
             select symbol, 'dividends_per_share' as metric_id, dividends_per_share as value from ticker_metrics
             union all
             select symbol, 'dividend_payout_ratio' as metric_id, dividend_payout_ratio as value from ticker_metrics
             union all
             select symbol, 'eps_ttm' as metric_id, eps_ttm as value from ticker_metrics
             union all
             select symbol, 'eps_actual' as metric_id, eps_actual as value from ticker_metrics
             union all
             select symbol, 'eps_estimate' as metric_id, eps_estimate as value from ticker_metrics
             union all
             select symbol, 'eps_surprise' as metric_id, eps_surprise as value from ticker_metrics
             union all
             select symbol, 'eps_difference' as metric_id, eps_difference as value from ticker_metrics
             union all
             select symbol, 'revenue_estimate_avg_0y' as metric_id, revenue_estimate_avg_0y as value from ticker_metrics
             union all
             select symbol, 'revenue_actual' as metric_id, revenue_actual as value from ticker_metrics
             union all
             select symbol, 'revenue_ttm' as metric_id, revenue_ttm as value from ticker_metrics
             union all
             select symbol, 'revenue_per_share_ttm' as metric_id, revenue_per_share_ttm as value from ticker_metrics
             union all
             select symbol, 'net_income' as metric_id, net_income as value from ticker_metrics
             union all
             select symbol, 'net_income_ttm' as metric_id, net_income_ttm as value from ticker_metrics
             union all
             select symbol, 'roi' as metric_id, roi as value from ticker_metrics
             union all
             select symbol, 'asset_cash_and_equivalents' as metric_id, asset_cash_and_equivalents as value from ticker_metrics
             union all
             select symbol, 'roa' as metric_id, roa as value from ticker_metrics
             union all
             select symbol, 'total_assets' as metric_id, total_assets as value from ticker_metrics
             union all
             select symbol, 'ebitda' as metric_id, ebitda as value from ticker_metrics
             union all
             select symbol, 'ebitda_ttm' as metric_id, ebitda_ttm as value from ticker_metrics
             union all
             select symbol, 'net_debt' as metric_id, net_debt as value from ticker_metrics
         ),
     raw_metrics_string as
         (
             select symbol, 'address_city' as metric_id, address_city as value from ticker_metrics
             union all
             select symbol, 'address_state' as metric_id, address_state as value from ticker_metrics
             union all
             select symbol, 'address_county' as metric_id, address_county as value from ticker_metrics
             union all
             select symbol, 'address_full' as metric_id, address_full as value from ticker_metrics
             union all
             select symbol, 'exchange_name' as metric_id, exchange_name as value from ticker_metrics
             union all
             select symbol, 'dividend_frequency' as metric_id, dividend_frequency as value from ticker_metrics
         ),
     raw_metrics_bigint as
         (
             select symbol, 'shares_outstanding' as metric_id, shares_outstanding as value from ticker_metrics
             union all
             select symbol, 'shares_float' as metric_id, shares_float as value from ticker_metrics
             union all
             select symbol, 'market_capitalization' as metric_id, market_capitalization as value from ticker_metrics
             union all
             select symbol, 'years_of_consecutive_dividend_growth' as metric_id, years_of_consecutive_dividend_growth::bigint as value from ticker_metrics
             union all
             select symbol, 'beaten_quarterly_eps_estimation_count_ttm' as metric_id, beaten_quarterly_eps_estimation_count_ttm::bigint as value from ticker_metrics
         ),
     raw_metrics_united as
         (
             select symbol,
                    metric_id,
                    value as value_double,
                    null::varchar as value_string,
                    null::bigint as value_bigint
             from raw_metrics_double

             union all

             select symbol,
                    metric_id,
                    null as value_double,
                    value as value_string,
                    null::bigint as value_bigint
             from raw_metrics_string

             union all

             select symbol,
                    metric_id,
                    null as value_double,
                    null as value_string,
                    value as value_bigint
             from raw_metrics_bigint
         )

select concat(symbol, '_', metric_id)::varchar as id,
       symbol,
       metric_id::varchar,
       value_double,
       value_string,
       value_bigint
from raw_metrics_united
