{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      pk('id'),
      index('symbol'),
      'delete from {{this}}
        using (select period, max(updated_at) as max_updated_at from {{this}} group by period) dc_stats
        where dc_stats.period = {{this}}.period
        and {{this}}.updated_at < dc_stats.max_updated_at',
      'delete from {{this}} where id = \'fake_row_allowing_deletion\'',
    ]
  )
}}


select distinct on (
        collection_ticker_actual_weights.symbol
    ) 'ttf_ticker_missing_in_drivewealth_instruments_' || collection_ticker_actual_weights.symbol as id,
       collection_ticker_actual_weights.symbol,
       'ttf_ticker_missing_in_drivewealth_instruments'                                             as code,
       'daily'                                                                                     as period,
       'TTF ticker ' || collection_ticker_actual_weights.symbol ||
           ' is missing in app.drivewealth_instruments'                                            as message,
       now()                                                                                       as updated_at
from {{ ref('collection_ticker_actual_weights') }}
         left join {{ source('app', 'drivewealth_instruments') }}
                   on collection_ticker_actual_weights.symbol = normalize_drivewealth_symbol(drivewealth_instruments.symbol)
where drivewealth_instruments.symbol is null
   or drivewealth_instruments.status != 'ACTIVE'

union all

-- add one fake record to allow post_hook above to clean other rows
select 'fake_row_allowing_deletion' as id,
       null                         as symbol,
       null                         as code,
       period,
       null                         as message,
       now()                        as updated_at
from (values('daily', 'realtime')) t(period)
