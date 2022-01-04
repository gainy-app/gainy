{{
  config(
    materialized = "table",
    post_hook=[
      index(this, 'transaction_id', true),
    ]
  )
}}

with actual_prices as
     (
         select distinct on (symbol) symbol, adjusted_close, '0d'::varchar as period
         from historical_prices_aggregated
         where ((period = '15min' and datetime > now() - interval '2 hour') or
                (period = '1d' and datetime > now() - interval '1 week'))
           and adjusted_close is not null
         order by symbol, datetime desc
     ),
     relative_data as
         (
             select distinct on (
                 portfolio_expanded_transactions.uniq_id
                 ) portfolio_expanded_transactions.id                                                                                                       as transaction_id,
                   portfolio_expanded_transactions.uniq_id                                                                                                  as transaction_uniq_id,
                   coalesce(historical_prices_aggregated.datetime,
                            ticker_options.last_trade_datetime)::timestamp                                                                                  as updated_at,
                   coalesce(actual_prices.adjusted_close,
                            ticker_options.last_price,
                            historical_prices_aggregated.adjusted_close)::numeric                                                                           as actual_price,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   actual_prices.adjusted_close::numeric /
                                   first_value(
                                   historical_prices_aggregated.adjusted_close::numeric)
                                   over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '0 day' PRECEDING) -
                                   1)
                       else 0 end                                                                                                                           as relative_gain_1d,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   case
                                       when portfolio_expanded_transactions.datetime >= now() - interval '1 week'
                                           then portfolio_expanded_transactions.price
                                       else
                                                   first_value(
                                                   historical_prices_aggregated.adjusted_close::numeric)
                                                   over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 week' PRECEDING)
                                       end - 1)
                       else 0 end                                                                                                                           as relative_gain_1w,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   case
                                       when portfolio_expanded_transactions.datetime >= now() - interval '1 month'
                                           then portfolio_expanded_transactions.price
                                       else
                                                   first_value(
                                                   historical_prices_aggregated.adjusted_close::numeric)
                                                   over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 month' PRECEDING)
                                       end - 1)
                       else 0 end                                                                                                                           as relative_gain_1m,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   case
                                       when portfolio_expanded_transactions.datetime >= now() - interval '3 months'
                                           then portfolio_expanded_transactions.price
                                       else
                                                   first_value(
                                                   historical_prices_aggregated.adjusted_close::numeric)
                                                   over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '3 months' PRECEDING)
                                       end - 1)
                       else 0 end                                                                                                                           as relative_gain_3m,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   case
                                       when portfolio_expanded_transactions.datetime >= now() - interval '1 year'
                                           then portfolio_expanded_transactions.price
                                       else
                                                   first_value(
                                                   historical_prices_aggregated.adjusted_close::numeric)
                                                   over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '1 year' PRECEDING)
                                       end - 1)
                       else 0 end                                                                                                                           as relative_gain_1y,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   case
                                       when portfolio_expanded_transactions.datetime >= now() - interval '5 years'
                                           then portfolio_expanded_transactions.price
                                       else first_value(
                                            historical_prices_aggregated.adjusted_close::numeric)
                                            over (partition by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.symbol ORDER BY historical_prices_aggregated.datetime RANGE INTERVAL '5 years' PRECEDING)
                                       end - 1)
                       else 0 end                                                                                                                           as relative_gain_5y,
                   sign(portfolio_expanded_transactions.quantity_norm)::numeric *
                   case
                       when ticker_options.contract_name is null
                           then (
                                   historical_prices_aggregated.adjusted_close::numeric /
                                   portfolio_expanded_transactions.price -
                                   1)
                       else 0 end                                                                                                                           as relative_gain_total,
                   portfolio_expanded_transactions.quantity_norm::numeric
             from {{ ref('portfolio_expanded_transactions') }}
                      join {{ ref('portfolio_securities_normalized') }}
                           on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id
                      left join {{ ref('historical_prices_aggregated') }}
                                on (historical_prices_aggregated.datetime >= portfolio_expanded_transactions.datetime or
                                    portfolio_expanded_transactions.datetime is null)
                                    and (
                                           (historical_prices_aggregated.period = '1d' and
                                            historical_prices_aggregated.datetime >=
                                            now() - interval '3 month' - interval '1 week') or
                                           (historical_prices_aggregated.period = '1w' and
                                            historical_prices_aggregated.datetime >=
                                            now() - interval '1 year' - interval '1 week') or
                                           (historical_prices_aggregated.period = '1m' and
                                            historical_prices_aggregated.datetime >=
                                            now() - interval '5 year' - interval '1 week')
                                       )
                                   and historical_prices_aggregated.symbol = portfolio_securities_normalized.ticker_symbol
                      left join actual_prices
                                on actual_prices.symbol = portfolio_securities_normalized.original_ticker_symbol
                      left join {{ ref('ticker_options') }}
                                on ticker_options.contract_name = portfolio_securities_normalized.original_ticker_symbol
             where portfolio_expanded_transactions.type in ('buy', 'sell')
               and (historical_prices_aggregated.symbol is not null or ticker_options.contract_name is not null)
               and historical_prices_aggregated.datetime < now()::date
             order by portfolio_expanded_transactions.uniq_id, historical_prices_aggregated.datetime desc
         )
select transaction_id,
       transaction_uniq_id,
       updated_at,
       relative_gain_1d::double precision,
       relative_gain_1w::double precision,
       relative_gain_1m::double precision,
       relative_gain_3m::double precision,
       relative_gain_1y::double precision,
       relative_gain_5y::double precision,
       relative_gain_total::double precision,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_1d)) *
                              abs(quantity_norm))::double precision as absolute_gain_1d,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_1w)) *
                              abs(quantity_norm))::double precision as absolute_gain_1w,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_1m)) *
                              abs(quantity_norm))::double precision as absolute_gain_1m,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_3m)) *
                              abs(quantity_norm))::double precision as absolute_gain_3m,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_1y)) *
                              abs(quantity_norm))::double precision as absolute_gain_1y,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_5y)) *
                              abs(quantity_norm))::double precision as absolute_gain_5y,
       sign(quantity_norm) * (actual_price * (1 - 1 / (1 + sign(quantity_norm) * relative_gain_total)) *
                              abs(quantity_norm))::double precision as absolute_gain_total
from relative_data