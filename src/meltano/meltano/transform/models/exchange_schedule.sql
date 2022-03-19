{{
  config(
    materialized = "incremental",
    unique_key = "id",
    post_hook=[
      index(this, 'id', true),
      'create unique index if not exists "date__exchange_name" ON {{ this }} (date, exchange_name)',
    ]
  )
}}

with schedule as
         (
             SELECT CONCAT(dd::date, '_', exchanges.name)::varchar as id,
                    dd::date                                       as date,
                    exchanges.name::varchar                        as exchange_name,
                    exchanges.country_name::varchar                as country_name,
                    coalesce(
                            polygon_marketstatus_upcoming.open::timestamptz,
                            (dd::date + exchanges.open_at::time) at time zone exchanges.timezone
                        )                                          as open_at,
                    coalesce(
                            polygon_marketstatus_upcoming.close::timestamptz,
                            (dd::date + exchanges.close_at::time) at time zone exchanges.timezone
                        )                                          as close_at
             FROM generate_series(now() - interval '1 week', now() + interval '1 week', interval '1 day') dd
                      join {{ source('gainy', 'exchanges') }}
             on true
                 left join {{ source('polygon', 'polygon_marketstatus_upcoming') }}
                 ON polygon_marketstatus_upcoming.exchange = exchanges.name
                 and polygon_marketstatus_upcoming.date::date = dd::date
             where extract (isodow from dd)
                 < 6
               and (polygon_marketstatus_upcoming.status is null
                or polygon_marketstatus_upcoming.status != 'closed')
         )

select (date || '_' || exchange_name)::varchar as id,
       date,
       exchange_name,
       null::varchar                          as country_name,
       open_at,
       close_at
from schedule

union all

select distinct on (
    country_name
    ) (date || '_' || country_name)::varchar as id,
      date,
      null::varchar                          as exchange_name,
      country_name,
      open_at,
      close_at
from schedule