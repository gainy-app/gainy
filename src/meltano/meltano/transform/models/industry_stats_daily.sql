{{
  config(
    materialized = "incremental",
    unique_key = "id",
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
         )
select price_stats.date,
       price_stats.industry_id,
       CONCAT(price_stats.industry_id, '_', price_stats.date::varchar)::varchar as id,
      null::double precision                                                    as median_growth_rate_1d,
       median_price
from price_stats
{% if is_incremental() %}
    left join max_updated_at on price_stats.industry_id = max_updated_at.industry_id
    where price_stats.date >= max_updated_at.max_date or max_updated_at.max_date is null
{% endif %}
