{{
  config(
    materialized = "incremental",
    unique_key = "holding_id_v2",
    tags = ["realtime"],
    post_hook=[
      pk('holding_id_v2'),
      'delete from {{this}}
        using {{this}} AS t
        LEFT OUTER JOIN {{ ref(\'profile_holdings_normalized_all\') }} using (holding_id_v2)
        WHERE portfolio_holding_gains.holding_id_v2 = t.holding_id_v2
          AND profile_holdings_normalized_all.holding_id_v2 is null',
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
             (
                 select distinct on (
                     holding_id_v2
                     ) holding_id_v2,
                       adjusted_close as actual_value
                 from {{ ref('portfolio_holding_chart') }}
                           join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                 where period = '1d'
                   and not profile_holdings_normalized_all.is_hidden
                   and not profile_holdings_normalized_all.is_app_trading
                 order by holding_id_v2, datetime desc
             )
{% if var('portfolio_cash_enabled') %}
             union all

             select holding_id_v2, quantity as actual_value
             from {{ ref('profile_holdings_normalized_all') }}
             where symbol = 'CUR:USD'
               and not profile_holdings_normalized_all.is_hidden
               and not profile_holdings_normalized_all.is_app_trading
{% endif %}
     ),
    last_day_value as
        (
             select distinct on (
                 holding_id_v2
                 ) holding_id_v2,
                   adjusted_close as last_day_value
             from {{ ref('portfolio_holding_chart') }}
                      join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
             where period = '1m'
               and not profile_holdings_normalized_all.is_hidden
               and not profile_holdings_normalized_all.is_app_trading
             order by holding_id_v2, datetime desc
     ),
    plaid_gains0 as
        (
             with raw_data_0d as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1d
                          from {{ ref('portfolio_holding_chart') }}
                                   join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                                   join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                          where period = '1d'
                            and week_trading_sessions_static.index = 0
                            and datetime between open_at and close_at - interval '1 second'
                            and not profile_holdings_normalized_all.is_hidden
                          group by holding_id_v2
                      ),
                  raw_data_1w as
                      (
                          select holding_id_v2,
                                 sum(adjusted_close * relative_gain / (1 + relative_gain)) as absolute_gain_1w
                          from {{ ref('portfolio_holding_chart') }}
                                   join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
                                   join {{ ref('week_trading_sessions_static') }} using (symbol, date)
                          where period = '1w'
                            and datetime > now()::date - interval '1 week'
                            and datetime between open_at and close_at - interval '1 second'
                            and not profile_holdings_normalized_all.is_hidden
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
             from {{ ref('profile_holdings_normalized_all') }}
                      left join raw_data_0d using (holding_id_v2)
                      left join raw_data_1w using (holding_id_v2)
                      left join raw_data_1m using (holding_id_v2)
                      left join raw_data_3m using (holding_id_v2)
                      left join raw_data_1y using (holding_id_v2)
                      left join raw_data_5y using (holding_id_v2)
                      left join raw_data_all using (holding_id_v2)
             where not profile_holdings_normalized_all.is_hidden
               and not profile_holdings_normalized_all.is_app_trading
    ),
    plaid_gains as
         (
             select holding_id_v2,
                    updated_at,
                    actual_value,
                    case
                        when abs(actual_value - absolute_gain_1d) > 1e-9
                            then absolute_gain_1d / (actual_value - absolute_gain_1d)
                        end::double precision       as relative_gain_1d,
                    case
                        when abs(actual_value - absolute_gain_1w) > 1e-9
                            then absolute_gain_1w / (actual_value - absolute_gain_1w)
                        end::double precision       as relative_gain_1w,
                    case
                        when abs(last_day_value - absolute_gain_1m) > 1e-9
                            then absolute_gain_1m / (last_day_value - absolute_gain_1m)
                        end::double precision       as relative_gain_1m,
                    case
                        when abs(last_day_value - absolute_gain_3m) > 1e-9
                            then absolute_gain_3m / (last_day_value - absolute_gain_3m)
                        end::double precision       as relative_gain_3m,
                    case
                        when abs(last_day_value - absolute_gain_1y) > 1e-9
                            then absolute_gain_1y / (last_day_value - absolute_gain_1y)
                        end::double precision       as relative_gain_1y,
                    case
                        when abs(last_day_value - absolute_gain_5y) > 1e-9
                            then absolute_gain_5y / (last_day_value - absolute_gain_5y)
                        end::double precision       as relative_gain_5y,
                    case
                        when abs(last_day_value - absolute_gain_total) > 1e-9
                            then absolute_gain_total / (last_day_value - absolute_gain_total)
                        end::double precision as relative_gain_total,
                    absolute_gain_1d,
                    absolute_gain_1w,
                    absolute_gain_1m,
                    absolute_gain_3m,
                    absolute_gain_1y,
                    absolute_gain_5y,
                    absolute_gain_total
             from {{ ref('profile_holdings_normalized_all') }}
                      left join plaid_gains0 using (holding_id_v2)
                      left join actual_value using (holding_id_v2)
                      left join last_day_value using (holding_id_v2) -- todo: get rid of this and fix tests
             where not profile_holdings_normalized_all.is_hidden
               and not profile_holdings_normalized_all.is_app_trading
         )
select t.*,
       holding_group_id,
       holding_id,
       profile_id,
       (actual_value / (1e-9 + sum(actual_value) over (partition by profile_id)))::double precision as value_to_portfolio_value,
       coalesce(long_term_tax_holdings.quantity, 0) as ltt_quantity_total
from (
         select holding_id_v2,
                actual_value,
                absolute_gain_1d,
                absolute_gain_1w,
                absolute_gain_1m,
                absolute_gain_3m,
                absolute_gain_1y,
                absolute_gain_5y,
                absolute_gain_total,
                relative_gain_1d,
                relative_gain_1w,
                relative_gain_1m,
                relative_gain_3m,
                relative_gain_1y,
                relative_gain_5y,
                relative_gain_total,
                updated_at
         from plaid_gains

         union all

         select holding_id_v2,
                actual_value,
                absolute_gain_1d,
                absolute_gain_1w,
                absolute_gain_1m,
                absolute_gain_3m,
                absolute_gain_1y,
                absolute_gain_5y,
                absolute_gain_total,
                relative_gain_1d,
                relative_gain_1w,
                relative_gain_1m,
                relative_gain_3m,
                relative_gain_1y,
                relative_gain_5y,
                relative_gain_total,
                updated_at
         from {{ ref('drivewealth_portfolio_holding_gains') }}
     ) t
         join {{ ref('profile_holdings_normalized_all') }} using (holding_id_v2)
         left join long_term_tax_holdings using (holding_id_v2)
