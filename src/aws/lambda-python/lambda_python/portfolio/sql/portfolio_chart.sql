with week_trading_sessions as
         (
             select min(open_at)                           as open_at,
                    min(close_at)                          as close_at,
                    min(date)                              as date,
                    row_number() over (order by date desc) as idx
             from exchange_schedule
             where open_at between now() - interval '1 week' and now()
             group by date
         ),
     latest_open_trading_session as
         (
             select *
             from week_trading_sessions
             where idx = 1
         ),
     raw_chart_data as
         (
             select distinct on (uniq_id, period, portfolio_transaction_chart.datetime)
                 period,
                 portfolio_transaction_chart.datetime,
                 uniq_id,
                 open::numeric,
                 high::numeric,
                 low::numeric,
                 close::numeric,
                 adjusted_close::numeric
             from portfolio_transaction_chart
                      join portfolio_expanded_transactions
                           on portfolio_expanded_transactions.uniq_id = portfolio_transaction_chart.transactions_uniq_id
                      left join week_trading_sessions
                                on portfolio_transaction_chart.datetime between week_trading_sessions.open_at and week_trading_sessions.close_at
                      left join latest_open_trading_session on true
                      {join_clause}
             where ((period = '1d' and week_trading_sessions.idx = 1)
                 or (period = '1w' and week_trading_sessions.idx is not null)
                 or (period = '1m' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 month')
                 or (period = '3m' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '3 months')
                 or (period = '1y' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '1 year')
                 or (period = '5y' and portfolio_transaction_chart.datetime >= latest_open_trading_session.date - interval '5 years')
                 or (period = 'all'))
                 {where_clause}
         )
select period,
       datetime,
       count(uniq_id)::double precision      as transaction_count,
       sum(open)::double precision           as open,
       sum(high)::double precision           as high,
       sum(low)::double precision            as low,
       sum(close)::double precision          as close,
       sum(adjusted_close)::double precision as adjusted_close
from raw_chart_data
group by period, datetime
