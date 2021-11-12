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

### Ticker metrics

```graphql
{
    tickers(order_by: {ticker_metrics: {month_price_change: asc}}) {
        symbol
        name
        description
        ticker_metrics {
            market_capitalization
            avg_volume_10d
            avg_volume_90d
            beta
            enterprise_value_to_sales
            month_price_change
            quarterly_revenue_growth_yoy
            profit_margin
            short_ratio
            short_percent_outstanding
            shares_outstanding
            shares_float
        }
        ticker_industries {
            gainy_industry {
                id
                name
            }
        }
    }
}
```