### Chart

periods: 1d, 1w, 1m, 3m, 1y, 5y, all

```GraphQL
{
    chart(where: {symbol: {_eq: "AAPL"}, period: {_eq: "1d"}}) {
        symbol
        volume
        period
        open
        low
        high
        datetime
        close
        adjusted_close
    }
}
```

### Chart (raw)

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

### Industry Median

```graphql
{
    industry_median_chart(where: {industry_id: {_eq: 111}, period: {_eq: "1d"}}){
        period
        datetime
        median_price
    }
}
```

## Queries below are deprecated
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

