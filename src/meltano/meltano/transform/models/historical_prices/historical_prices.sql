{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('symbol, date'),
      index('id', true),
      'create unique index if not exists "symbol__date_year__date" ON {{ this }} (symbol, date_year, date)',
      'create unique index if not exists "symbol__date_month__date" ON {{ this }} (symbol, date_month, date)',
      'create unique index if not exists "date_week__symbol__date" ON {{ this }} (date_week, symbol, date)',
    ]
  )
}}


with dividend_adjustment as
         (
             select symbol,
                    date,
                    sum(tmp) over wnd / lag(cumulative_relative_daily_gain) over wnd as dividend_adjustment
             from (
                      select symbol,
                             date,
                             cumulative_relative_daily_gain,
                             lag(value * cumulative_relative_daily_gain) over wnd as tmp
                      from (
                               select symbol,
                                      historical_prices_div_adjusted.date,
                                      close,
                                      adjusted_close,
                                      relative_daily_gain,
                                      historical_dividends.value,
                                      exp(sum(ln(relative_daily_gain + 1)) over wnd) as cumulative_relative_daily_gain
                               from {{ ref('historical_prices_div_adjusted') }}
                                        left join {{ ref('historical_dividends') }} using (symbol, date)
                               window wnd as ( partition by symbol order by historical_prices_div_adjusted.date desc )
                           ) t
                      window wnd as (partition by symbol order by date desc)
                  ) t
             window wnd as (partition by symbol order by date desc)
         )
select t.*
from (
         select symbol,
                (symbol || '_' || date) as id,
                date_year,
                date_month,
                date_week,
                adjusted_close + coalesce(dividend_adjustment, 0) as adjusted_close,
                case
                    when lag(adjusted_close + coalesce(dividend_adjustment, 0)) over wnd > 0
                        then coalesce((adjusted_close + coalesce(dividend_adjustment, 0)) / (lag(adjusted_close + coalesce(dividend_adjustment, 0)) over wnd) - 1, 0)
                    end                 as relative_daily_gain,
                close,
                date::date,
                high,
                low,
                open,
                volume::numeric,
                source,
                updated_at
         from {{ ref('historical_prices_div_adjusted') }}
                  left join dividend_adjustment using (symbol, date)
             window wnd as (partition by symbol order by date rows between 1 preceding and current row)
      ) t
{% if is_incremental() %}
         left join {{ this }} old_data using (symbol, date)
where old_data.symbol is null
   or (old_data.relative_daily_gain is null and t.relative_daily_gain is not null)
   or abs(t.adjusted_close - old_data.adjusted_close) > {{ var('price_precision') }}
   or abs(t.relative_daily_gain - old_data.relative_daily_gain) > {{ var('gain_precision') }}
{% endif %}
