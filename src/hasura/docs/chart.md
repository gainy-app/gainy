### Chart

periods: 15min, 1d, 1w, 1m

```GraphQL
{
    historical_prices_aggregated(where: {symbol: {_eq: "AAPL"}, period: {_eq: "15min"}, datetime:{_gt: "2021-12-06 07:00:00+0000"}}, order_by: {time: asc}) {
        symbol
        datetime
        period
        open
        high
        low
        close
        adjusted_close
        volume
    }
}
```

### Intra-day Chart

Firstly we'll need to know exchange's schedule for the past couple of days:
```GraphQL
{
    exchange_schedule(order_by: {date: desc}, where: {date: {_lte: "2022-01-11", _gte: "2022-01-04"}}) {
        date
        exchange_name
        open_at
        close_at
    }
}
```

Then having the ticker exchange, we can extract the schedule for a particular day:
```GraphQL
{
    tickers{
        exchange
    }
}
```

General rules:
- if a date is not present in `exchange_schedule`, then the market is closed on that date;
- if a date is present - then the market is open from `open_at` to `close_at`
- when a user opens the stock screen, we need to find the latest record in `exchange_schedule` with appropriate `exchange` and `open_at` less then `now()`
- having this record we need to make a request to `historical_prices_aggregated`, which will return either the latest open trading session of current one (incomplete)
- I would suggest requesting exchange_schedule on the app start and caching it, then implement the logic above to work with cached values

### Realtime prices

```graphql
{
    tickers{
        realtime_metrics{
            actual_price
            relative_daily_change
        }
    }
}
```

## Queries below are deprecated
### Daily prices and growth rate with industry median

```graphql
query MyQuery {
    tickers(where: {symbol: {_eq: "AAPL"}}) {
        historical_prices(where: {date: {_gte: "2021-09-04"}}) {
            close
            date
            open
            adjusted_close
        }
        historical_growth_rates(where: {date: {_gte: "2021-09-04"}}) {
            date
            growth_rate_1d
        }
        ticker_industries {
            gainy_industry {
                industry_stats_dailies(where: {date: {_gte: "2021-09-04"}}) {
                    median_growth_rate_1d
                    median_price
                    date
                }
            }
        }
    }
}
```

### Weekly prices and growth rate with industry median

```graphql
query {
    tickers(where: {symbol: {_eq: "AAPL"}}) {
        ticker_historical_prices_1w(where: {date: {_gte: "2021-08-23"}}) {
            close
            date
            open
            adjusted_close
        }
        ticker_growth_rate_1w(where: {date: {_gte: "2021-08-23"}}) {
            date
            growth_rate
        }
        ticker_industries {
            gainy_industry {
                ticker_industry_median_1w(where: {date: {_gte: "2021-08-23"}}) {
                    median_growth_rate
                    median_price
                    date
                }
            }
        }
    }
}
```

### Monthly prices and growth rate with industry median

```graphql
query {
    tickers(where: {symbol: {_eq: "AAPL"}}) {
        ticker_historical_prices_1m(where: {date: {_gte: "2021-08-01"}}) {
            close
            date
            open
            adjusted_close
        }
        ticker_growth_rate_1m(where: {date: {_gte: "2021-08-01"}}) {
            date
            growth_rate
        }
        ticker_industries {
            gainy_industry {
                ticker_industry_median_1m(where: {date: {_gte: "2021-08-01"}}) {
                    median_growth_rate
                    median_price
                    date
                }
            }
        }
    }
}
```

### Quarterly Net Income and Revenue with industry median

```graphql
query {
    tickers(where: {symbol: {_eq: "AAPL"}}) {
        financials_income_statement_quarterlies(where: {date: {_gte: "2021-01-01"}}) {
            date
            net_income
            total_revenue
        }
        ticker_industries {
            gainy_industry {
                industry_stats_quarterlies (where: {date: {_gte: "2021-01-01"}}) {
                    date
                    median_net_income
                    median_revenue
                }
            }
        }
    }
}
```

