{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      'delete from {{this}}
        using {{this}} AS t
        LEFT OUTER JOIN {{ ref(\'profile_holdings_normalized\') }} using (holding_id_v2)
        WHERE portfolio_holding_gains.holding_id_v2 = t.holding_id_v2
          AND profile_holdings_normalized.holding_id_v2 is null',
    ]
  )
}}


with long_term_tax_holdings as
         (
             select distinct on (
                 holding_id_v2
                 ) holding_id_v2,
                   portfolio_holding_chart_1w.quantity
             from {{ ref('portfolio_holding_chart_1w') }}
             where date < now() - interval '1 year'
             order by holding_id_v2 desc, date desc
         ),
    actual_value as
        (
             select distinct on (
                 holding_id_v2
                 ) holding_id_v2,
                   adjusted_close as actual_value
             from {{ ref('portfolio_holding_chart') }}
                      join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
                      join {{ ref('week_trading_sessions_static') }} using (symbol, date)
             where period = '1d'
               and week_trading_sessions_static.index = 0
               and datetime between open_at and close_at - interval '1 second'
             order by holding_id_v2, datetime desc
     ),
    combined_gains as
        (
             with raw_data_0d as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1d
                          from {{ ref('portfolio_holding_chart') }}
                                   join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
                                   join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                          where period = '1d'
                            and week_trading_sessions_static.index = 0
                            and datetime between open_at and close_at - interval '1 second'
                          group by holding_id_v2
                      ),
                  raw_data_1w as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1w
                          from {{ ref('portfolio_holding_chart') }}
                                   join {{ ref('profile_holdings_normalized') }} using (holding_id_v2)
                                   join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                          where period = '1w'
                            and datetime > now()::date - interval '1 week'
                            and datetime between open_at and close_at - interval '1 second'
                          group by holding_id_v2
                  ),
                  raw_data_1m as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1m
                          from {{ ref('portfolio_holding_chart') }}
                          where period = '1m'
                            and date >= now()::date - interval '1 month'
                          group by holding_id_v2
                  ),
                  raw_data_3m as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_3m
                          from {{ ref('portfolio_holding_chart') }}
                          where period = '3m'
                            and date >= now()::date - interval '3 month'
                          group by holding_id_v2
                  ),
                  raw_data_1y as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1y
                          from {{ ref('portfolio_holding_chart') }}
                          where period = '1y'
                            and date >= now()::date - interval '1 year'
                          group by holding_id_v2
                  ),
                  raw_data_5y as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_5y
                          from {{ ref('portfolio_holding_chart') }}
                          where period = '5y'
                            and date >= now()::date - interval '5 year'
                          group by holding_id_v2
                  ),
                  raw_data_all as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_total
                          from {{ ref('portfolio_holding_chart') }}
                          where period = 'all'
                          group by holding_id_v2
                  )
             select holding_id_v2,
                    absolute_gain_1d,
                    absolute_gain_1w,
                    absolute_gain_1m,
                    absolute_gain_3m,
                    absolute_gain_1y,
                    absolute_gain_5y,
                    absolute_gain_total
             from {{ ref('profile_holdings_normalized') }}
                      left join raw_data_0d using (holding_id_v2)
                      left join raw_data_1w using (holding_id_v2)
                      left join raw_data_1m using (holding_id_v2)
                      left join raw_data_3m using (holding_id_v2)
                      left join raw_data_1y using (holding_id_v2)
                      left join raw_data_5y using (holding_id_v2)
                      left join raw_data_all using (holding_id_v2)
    )
select holding_group_id,
       holding_id_v2,
       holding_id,
       profile_id,
       updated_at,
       actual_value,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value,
       case
           when abs(actual_value - absolute_gain_1d) > 1e-9
               then absolute_gain_1d / (actual_value - absolute_gain_1d)
           end::double precision       as relative_gain_1d,
       case
           when abs(actual_value - absolute_gain_1w) > 1e-9
               then absolute_gain_1w / (actual_value - absolute_gain_1w)
           end::double precision       as relative_gain_1w,
       case
           when abs(actual_value - absolute_gain_1m) > 1e-9
               then absolute_gain_1m / (actual_value - absolute_gain_1m)
           end::double precision       as relative_gain_1m,
       case
           when abs(actual_value - absolute_gain_3m) > 1e-9
               then absolute_gain_3m / (actual_value - absolute_gain_3m)
           end::double precision       as relative_gain_3m,
       case
           when abs(actual_value - absolute_gain_1y) > 1e-9
               then absolute_gain_1y / (actual_value - absolute_gain_1y)
           end::double precision       as relative_gain_1y,
       case
           when abs(actual_value - absolute_gain_5y) > 1e-9
               then absolute_gain_5y / (actual_value - absolute_gain_5y)
           end::double precision       as relative_gain_5y,
       case
           when abs(actual_value - absolute_gain_total) > 1e-9
               then absolute_gain_total / (actual_value - absolute_gain_total)
           end::double precision as relative_gain_total,
       absolute_gain_1d,
       absolute_gain_1w,
       absolute_gain_1m,
       absolute_gain_3m,
       absolute_gain_1y,
       absolute_gain_5y,
       absolute_gain_total,
       coalesce(long_term_tax_holdings.quantity, 0) as ltt_quantity_total
from {{ ref('profile_holdings_normalized') }}
         left join combined_gains using (holding_id_v2)
         left join long_term_tax_holdings using (holding_id_v2)
         left join actual_value using (holding_id_v2)
