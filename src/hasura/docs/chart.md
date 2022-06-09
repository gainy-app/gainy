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
