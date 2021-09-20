### Alternative stocks

```graphql
query {
  tickers {
    symbol
    ticker_interests {
      symbol
      interest_id
      interest {
        icon_url
        id
        name
        ticker_interests{ # here are our alternative stocks
          ticker{
            symbol
          }
        }
      }
    }
  }
}
```