{{
  config(
    materialized = "incremental",
    unique_key = "id",
    incremental_strategy = 'insert_overwrite',
    post_hook=[
      index(this, 'id', true),
      index(this, 'industry_id'),
      index(this, 'date'),
    ]
  )
}}


with
{% if is_incremental() %}
     max_updated_at as (select industry_id, max(date) as max_date from {{ this }} group by industry_id),
{% endif %}
     price_stats as
         (
             select hp.date::timestamp                                     as date,
                    ti.industry_id,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY adjusted_close) as median_price
             from {{ ref('historical_prices') }} hp
                      join {{ ref('tickers') }} on hp.code = tickers.symbol
                      join {{ ref('ticker_industries') }} ti on tickers.symbol = ti.symbol
             group by ti.industry_id, hp.date
         ),
     growth_rate_stats as
         (
             select hgr.date::date                                         as date,
                    ti.industry_id,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY growth_rate_1d) as median_growth_rate_1d,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY growth_rate_1w) as median_growth_rate_1w,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY growth_rate_1m) as median_growth_rate_1m,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY growth_rate_3m) as median_growth_rate_3m,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY growth_rate_1y) as median_growth_rate_1y
             from {{ ref('historical_growth_rate') }} hgr
                      join {{ ref('tickers') }} on hgr.symbol = tickers.symbol
                      join {{ ref('ticker_industries') }} ti on tickers.symbol = ti.symbol
             group by ti.industry_id, hgr.date
         )
select price_stats.date,
       price_stats.industry_id,
       CONCAT(price_stats.industry_id, '_',price_stats.date::varchar)::varchar as id,
       median_price,
       median_growth_rate_1d,
       median_growth_rate_1w,
       median_growth_rate_1m,
       median_growth_rate_3m,
       median_growth_rate_1y
from price_stats
         left join growth_rate_stats grs on grs.industry_id = price_stats.industry_id and grs.date = price_stats.date
{% if is_incremental() %}
    left join max_updated_at on price_stats.industry_id = max_updated_at.industry_id
    where price_stats.date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
