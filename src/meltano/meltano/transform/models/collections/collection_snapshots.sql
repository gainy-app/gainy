{{
  config(
    materialized = "view",
    tags = ["view"],
  )
}}


with recursive
    collection_tickers_weighted as materialized
        (
            select *
            from {{ source('gainy_history', 'collection_tickers_weighted') }}
            where profile_id is null
        ),
    -- Execution Time: 1118.536 ms
    collection_daily_weights as materialized
        (
            select collection_uniq_id,
                   date,
                   sum(weight)                                                       as weight_sum,
                   row_number() over (partition by collection_uniq_id order by date) as idx
            from collection_tickers_weighted
                     join {{ ref('historical_prices') }} using (symbol, date)
            where profile_id is null
            group by collection_uniq_id, date
        ), --  select * from collection_daily_weights where collection_uniq_id = '0_107';
    -- Execution Time: 11632.348 ms
    collection_ticker_daily_price as materialized
        (
            select distinct on (
                collection_uniq_id, symbol, date
                ) collection_uniq_id,
                  date,
                  symbol,
                  weight,
                  lag(open)
                  over (partition by collection_uniq_id, symbol order by date desc) as price -- we trade with open price on the next day,
            from collection_tickers_weighted
                     join collection_daily_weights using (collection_uniq_id, date)
                     join {{ ref('historical_prices') }} using (symbol, date)
        ),
    -- Execution Time: 35711.127 ms
    collection_daily_prices as materialized
        (
            select collection_uniq_id,
                   date,
                   sum(weight / weight_sum * price) as weighted_price_sum
            from collection_ticker_daily_price
                     join collection_daily_weights using (collection_uniq_id, date)
            group by collection_uniq_id, date
        ), -- select * from collection_daily_prices;
    -- Execution Time: 20945.303 ms
    collection_daily_amounts as
        (
            select collection_uniq_id,
                   date,
                   symbol,
                   idx,
                   weight / weight_sum                             as weight,
                   weight / weight_sum / weighted_price_sum * 1000 as amount,
                   price
            from collection_ticker_daily_price
                     join collection_daily_weights using (collection_uniq_id, date)
                     join collection_daily_prices using (collection_uniq_id, date)
        ), -- select * from collection_daily_amounts where collection_uniq_id = '0_107';
    -- Execution Time: 22925.672 ms
    initial_data as
        (
            select *,
                   holdings_desired / price as amount_desired
            from (
                     select collection_uniq_id,
                            date,
                            symbol,
                            idx,
                            weight,
                            price,
                            price * weight / sum(price * weight) over (partition by collection_uniq_id, date) *
                            1000 as holdings_desired
                     from collection_daily_amounts
                 ) t
        ),
    rec_data as
        (
            select collection_uniq_id,
                   idx,
                   date,
                   symbol,
                   weight,
                   price,
                   holdings_desired,
                   amount_desired,
                   0::double precision as amount_diff,
                   0::double precision as holding_diff,
                   0::double precision as holding_diff_sum,
                   holdings_desired    as holdings_adjusted,
                   amount_desired      as amount_adjusted
            from initial_data
            where idx = 1

            union all

            select initial_data.collection_uniq_id,
                   initial_data.idx,
                   initial_data.date,
                   initial_data.symbol,
                   initial_data.weight,
                   initial_data.price,
                   initial_data.holdings_desired,
                   initial_data.amount_desired,
                   initial_data.amount_desired - rec_data.amount_adjusted                        as amount_diff,
                   initial_data.price * (initial_data.amount_desired - rec_data.amount_adjusted) as holding_diff,
                   sum(initial_data.price * (initial_data.amount_desired - rec_data.amount_adjusted))
                   over (partition by initial_data.collection_uniq_id, initial_data.date)        as holding_diff_sum,
                   initial_data.holdings_desired -
                   initial_data.weight *
                   sum(initial_data.price * (initial_data.amount_desired - rec_data.amount_adjusted))
                   over (partition by initial_data.collection_uniq_id, initial_data.date)        as holdings_adjusted,
                   (initial_data.holdings_desired -
                    initial_data.weight *
                    sum(initial_data.price * (initial_data.amount_desired - rec_data.amount_adjusted))
                    over (partition by initial_data.collection_uniq_id, initial_data.date)) /
                   initial_data.price                                                            as amount_adjusted
            from initial_data
                     join rec_data
                          on rec_data.collection_uniq_id = initial_data.collection_uniq_id
                              and rec_data.symbol = initial_data.symbol
                              and rec_data.idx = initial_data.idx - 1
            where initial_data.idx > 1
        )
select *,
       sum(holdings_adjusted) over (partition by collection_uniq_id, date) as holding_sum
from rec_data
