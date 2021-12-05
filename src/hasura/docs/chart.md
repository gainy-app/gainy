### Chart

periods: 15min, 1d, 1w, 1m

```GraphQL
{
    historical_prices_aggregated(where: {symbol: {_eq: "AAPL"}, period: {_eq: "15min"}}, order_by: {time: asc}) {
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

