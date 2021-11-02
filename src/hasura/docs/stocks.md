### Alternative stocks

```graphql
{
    tickers {
        symbol
        ticker_industries {
            gainy_industry {
                name
                ticker_industries {
                    ticker { # here are our alternative stocks
                        symbol
                    }
                }
            }
        }
        ticker_interests {
            symbol
            interest_id
            interest {
                icon_url
                id
                name
            }
        }
        ticker_analyst_ratings {
            strong_buy
            buy
            hold
            sell
            strong_sell
            rating
        }
    }
}
```