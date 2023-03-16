{{
  config(
    materialized = "incremental",
    unique_key = "id",
    tags = ["realtime"],
    post_hook=[
      pk('symbol, date'),
      index('id', true),
      'delete from {{this}} where open_at < now() - interval \'2 week\'',
    ]
  )
}}


select t.*,
       now() as updated_at
from {{ ref('week_trading_sessions') }} t
{% if is_incremental() %}
         left join {{ this }} old_trading_sessions using (symbol, date, index)
where old_trading_sessions.symbol is null
{% endif %}
