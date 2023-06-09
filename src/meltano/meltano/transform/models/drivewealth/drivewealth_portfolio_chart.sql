{{
  config(
    materialized = "view",
  )
}}


with latest_price_row as materialized
      (
             select distinct on (
                 holding_id_v2
                 ) *
             from {{ ref('drivewealth_portfolio_historical_holdings') }}
             where updated_at is not null
             order by holding_id_v2, date desc
      )

(
    select distinct on (
        holding_id_v2, datetime
        ) profile_id,
          holding_id_v2,
          date,
          datetime,
          period,
          value,
          relative_gain,
          updated_at
    from (
             select profile_id,
                    holding_id_v2,
                    date,
                    datetime,
                    value,
                    relative_gain,
                    updated_at,
                    0 as priority
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             where drivewealth_portfolio_historical_prices_aggregated.period = '3min'

             union all

             select profile_id,
                    holding_id_v2,
                    date::date                                    as date,
                    date_trunc('minute', updated_at) -
                    interval '1 minute' *
                    mod(extract(minutes from updated_at)::int, 3) as datetime,
                    value,
                    null                                          as relative_gain,
                    updated_at,
                    1                                             as priority
             from latest_price_row
         ) t
                 join (
                          with holdings as
                                   (
                                       select holding_id_v2
                                       from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                       where period = '3min'
                                       group by holding_id_v2
                                   )
                          select holding_id_v2, '1d'::varchar as period
                          from holdings

                          union all

                          select holding_id_v2, period::varchar
                          from (
                                   select holding_id_v2
                                   from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   where period = '15min'
                                   group by holding_id_v2
                                   having count(*) < 3
                               ) t
                                   join holdings using (holding_id_v2)
                                   join (
                                            values ('all'), ('5y'), ('1y'), ('3m'), ('1m'), ('1w')
                                        ) periods(period) on true
                      ) periods using (holding_id_v2)
    order by holding_id_v2, datetime, priority
)

union all

(
    select distinct on (
        holding_id_v2, datetime
        ) profile_id,
          holding_id_v2,
          date,
          datetime,
          period,
          value,
          relative_gain,
          updated_at
    from (
             select profile_id,
                    holding_id_v2,
                    date,
                    datetime,
                    value,
                    relative_gain,
                    updated_at,
                    0 as priority
             from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             where drivewealth_portfolio_historical_prices_aggregated.period = '15min'

             union all

             select profile_id,
                    holding_id_v2,
                    date::date                                     as date,
                    date_trunc('minute', updated_at) -
                    interval '1 minute' *
                    mod(extract(minutes from updated_at)::int, 15) as datetime,
                    value,
                    null                                           as relative_gain,
                    updated_at,
                    1                                              as priority
             from latest_price_row
         ) t
             join (
                      with holdings as
                               (
                                   select holding_id_v2
                                   from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                                   where period = '15min'
                                   group by holding_id_v2
                                   having count(*) >= 3
                               )
                      select holding_id_v2, '1w'::varchar as period
                      from holdings

                      union all

                      select holding_id_v2, period::varchar
                      from (
                               select holding_id_v2
                               from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                               where period = '1d'
                               group by holding_id_v2
                               having count(*) < 3
                           ) t
                               join holdings using (holding_id_v2)
                               join (
                                        values ('all'), ('5y'), ('1y'), ('3m'), ('1m')
                                    ) periods(period) on true
                  ) periods using (holding_id_v2)
    order by holding_id_v2, datetime, priority
)

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '1m'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.value,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now()::date - interval '1 month + 1 week'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       '3m'::varchar as period,
       drivewealth_portfolio_historical_prices_aggregated.value,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'
  and drivewealth_portfolio_historical_prices_aggregated.datetime >= now()::date - interval '3 month + 1 week'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       periods.period,
       drivewealth_portfolio_historical_prices_aggregated.value,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         join (
                  with holdings as
                           (
                               select holding_id_v2
                               from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                               where period = '1d'
                               group by holding_id_v2
                               having count(*) >= 3
                           )
                  select holding_id_v2, '1y'::varchar as period
                  from holdings

                  union all

                  select holding_id_v2, period::varchar
                  from (
                           select holding_id_v2
                           from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                           where period = '1w'
                           group by holding_id_v2
                           having count(*) < 3
                       ) t
                           join holdings using (holding_id_v2)
                           join (
                                    values ('all'), ('5y')
                                ) periods(period) on true
              ) periods using (holding_id_v2)
where drivewealth_portfolio_historical_prices_aggregated.period = '1d'

union all

select drivewealth_portfolio_historical_prices_aggregated.profile_id,
       drivewealth_portfolio_historical_prices_aggregated.holding_id_v2,
       drivewealth_portfolio_historical_prices_aggregated.datetime::date as date,
       drivewealth_portfolio_historical_prices_aggregated.datetime,
       periods.period,
       drivewealth_portfolio_historical_prices_aggregated.value,
       drivewealth_portfolio_historical_prices_aggregated.relative_gain,
       drivewealth_portfolio_historical_prices_aggregated.updated_at
from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
         join (
                  with holdings as
                           (
                               select holding_id_v2
                               from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                               where period = '1w'
                               group by holding_id_v2
                               having count(*) >= 3
                           )
                  select holding_id_v2, '5y'::varchar as period
                  from holdings

                  union all

                  select holding_id_v2, 'all'::varchar as period
                  from (
                           select holding_id_v2
                           from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                           where period = '1m'
                           group by holding_id_v2
                           having count(*) < 3
                       ) t
                           join holdings using (holding_id_v2)
              ) periods using (holding_id_v2)
where drivewealth_portfolio_historical_prices_aggregated.period = '1w'

union all

(
    with holdings as
             (
                 select holding_id_v2
                 from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
                 where period = '1m'
                 group by holding_id_v2
                 having count(*) >= 3
             )
    select profile_id,
           holding_id_v2,
           datetime::date                as date,
           datetime,
           'all'::varchar                as period,
           value,
           relative_gain,
           drivewealth_portfolio_historical_prices_aggregated.updated_at
    from {{ ref('drivewealth_portfolio_historical_prices_aggregated') }}
             join holdings using (holding_id_v2)
    where drivewealth_portfolio_historical_prices_aggregated.period = '1m'
)