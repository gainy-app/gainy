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

### Industry Median

```graphql
{
    industry_median_chart(where: {industry_id: {_eq: 111}, period: {_eq: "1d"}}, order_by: {datetime: asc}){
        period
        datetime
        median_price

    }
}
```

### Collection Median Chart

```graphql
{
    collection_median_chart(where: {collection_id: {_eq: 111}, period: {_eq: "1d"}}, order_by: {datetime: asc}){
        period
        datetime
        median_price

    }
}
```
