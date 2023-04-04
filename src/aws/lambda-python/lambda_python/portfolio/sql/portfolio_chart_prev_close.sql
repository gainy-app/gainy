with filtered_holdings as
         (
             select distinct holding_id_v2
             from profile_holdings_normalized_all
                      {join_clause}
             where profile_holdings_normalized_all.profile_id = %(profile_id)s
                   {holding_where_clause}
         ),
     ticker_chart as
         (
             select profile_id,
                    portfolio_holding_chart.quantity,
                    period,
                    portfolio_holding_chart.date,
                    portfolio_holding_chart.datetime,
                    transaction_count,
                    cash_adjustment,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close
             from portfolio_holding_chart
                      left join filtered_holdings using (holding_id_v2)
             where profile_id = %(profile_id)s
               and filtered_holdings.holding_id_v2 is not null
                   {chart_where_clause}
     ),
     static_values as
         (
             select case
                        when %(include_cash)s
                            then sum(value)
                        end as cash_value
             from (
                      select distinct on (
                          profile_holdings_normalized_all.holding_id_v2
                          ) profile_id,
                            case
                                when type = 'cash' and ticker_symbol = 'CUR:USD'
                                    then quantity
                                else 0
                                end as value
                      from profile_holdings_normalized_all
                      where profile_id = %(profile_id)s
                        and type = 'cash'
                        and ticker_symbol = 'CUR:USD'
                        and not is_app_trading

                      union all

                      select profile_id,
                             coalesce(pending_cash, 0) as value
                      from trading_profile_status
                      where profile_id = %(profile_id)s
                  ) t
     ),
     raw_data as
         (
             select shown_period                      as period,
                    date,
                    sum(ticker_chart.adjusted_close)  as adjusted_close,
                    sum(ticker_chart.cash_adjustment) as cash_adjustment
             from (
                      select period        as shown_period,
                             case
                                 when period in ('1d', '1m', '1w')
                                     then '1y'
                                 else period
                                 end       as period,
                             max(date)     as date,
                             max(datetime) as datetime
                      from (
                               select ticker_chart.*,
                                      rank() over (partition by profile_id, period order by ticker_chart.date desc) = 1 as is_latest_day
                               from ticker_chart
                                   join portfolio_chart_skeleton using (profile_id, period, datetime)
                           ) t
                      where (period != '1d' or not is_latest_day)
                        and (period != '1w' or date < now()::date - interval '1 week')
                        and (period != '1m' or datetime < now()::date - interval '1 month')
                        and (period != '3m' or datetime < now()::date - interval '3 month')
                        and (period != '1y' or datetime < now()::date - interval '1 year')
                        and (period != '5y' or datetime < now()::date - interval '5 year')
                      group by period
                  ) t
                      left join ticker_chart using (period, date)
             group by shown_period, date
     )
select raw_data_1d.adjusted_close +
       greatest(0, coalesce(raw_data_1d.cash_adjustment, 0) + coalesce(cash_value, 0))                              as prev_close_1d,
       raw_data_1w.adjusted_close + greatest(0, coalesce(raw_data_1w.cash_adjustment, 0) +
                                                coalesce(cash_value, 0))                                            as prev_close_1w,
       raw_data_1m.adjusted_close + greatest(0, coalesce(raw_data_1m.cash_adjustment, 0) +
                                                coalesce(cash_value, 0))                                            as prev_close_1m,
       raw_data_3m.adjusted_close + greatest(0, coalesce(raw_data_3m.cash_adjustment, 0) +
                                                coalesce(cash_value, 0))                                            as prev_close_3m,
       raw_data_1y.adjusted_close + greatest(0, coalesce(raw_data_1y.cash_adjustment, 0) +
                                                coalesce(cash_value, 0))                                            as prev_close_1y,
       raw_data_5y.adjusted_close + greatest(0, coalesce(raw_data_5y.cash_adjustment, 0) +
                                                coalesce(cash_value, 0))                                            as prev_close_5y
from static_values
         left join raw_data raw_data_1d on raw_data_1d.period = '1d'
         left join raw_data raw_data_1w on raw_data_1w.period = '1w'
         left join raw_data raw_data_1m on raw_data_1m.period = '1m'
         left join raw_data raw_data_3m on raw_data_3m.period = '3m'
         left join raw_data raw_data_1y on raw_data_1y.period = '1y'
         left join raw_data raw_data_5y on raw_data_5y.period = '5y'
