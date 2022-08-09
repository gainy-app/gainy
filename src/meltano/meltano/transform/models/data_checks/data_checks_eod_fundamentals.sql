{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = data_checks.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
    ]
  )
}}


with errors as
    (
         select distinct on (symbol)
             symbol,
             'analyst_ratings' as code,
             'daily' as "period",
             'Ticker '||symbol||' incorrect analyst_ratings' as message
         from analyst_ratings
         where buy < 0
            or hold < 0
            or sell < 0
            or rating < 0
            or rating > 6
            or strong_buy < 0
            or strong_sell < 0
            or target_price < 0
    )
select (code || '_' || symbol) as id,
       symbol,
       code,
       period,
       message,
       now() as updated_at
from errors