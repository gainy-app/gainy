create table raw_data.coingecko_market_realtime
(
    ath                              double precision,
    ath_change_percentage            double precision,
    ath_date                         varchar,
    atl                              double precision,
    atl_change_percentage            double precision,
    atl_date                         varchar,
    circulating_supply               double precision,
    current_price                    double precision,
    fully_diluted_valuation          double precision,
    high_24h                         double precision,
    id                               varchar not null
        primary key,
    image                            varchar,
    last_updated                     varchar,
    low_24h                          double precision,
    market_cap                       double precision,
    market_cap_change_24h            double precision,
    market_cap_change_percentage_24h double precision,
    market_cap_rank                  numeric,
    max_supply                       double precision,
    name                             varchar,
    price_change_24h                 double precision,
    price_change_percentage_24h      double precision,
    roi                              varchar,
    symbol                           varchar,
    total_supply                     double precision,
    total_volume                     double precision
);

INSERT INTO raw_data.coingecko_market_realtime (ath, ath_change_percentage, ath_date, atl, atl_change_percentage, atl_date, circulating_supply, current_price, fully_diluted_valuation, high_24h, id, image, last_updated, low_24h, market_cap, market_cap_change_24h, market_cap_change_percentage_24h, market_cap_rank, max_supply, name, price_change_24h, price_change_percentage_24h, roi, symbol, total_supply, total_volume)
VALUES (69045, -36.98488, '2021-11-10T14:24:11.849Z', 67.81, 64063.52189, '2013-07-06T00:00:00.000Z', 19005187, 43472, 912909372572, 45438, 'bitcoin', 'https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1547033579', '2022-04-07T10:24:16.013Z', 42867, 826191111418, -34226565313.01062, -3.9779, 1, 21000000, 'Bitcoin', -1803.138690544336, -3.98264, null, 'btc', 21000000, 30460049725)
on conflict do nothing;
