# GraphQL API server

This repo contains Gainy's GraphQL API server built with [Hasura](https://hasura.io/)

## Supported Queries

- [Articles](docs/articles.md)
- [Chart](docs/chart.md)
- [Crypto](docs/crypto.md)
- [Collections](docs/collections.md)
- [ETF](docs/etf.md)
- [Match Score](docs/match_score.md)
- [News](docs/news.md)
- [Portfolio](docs/portfolio.md)
- [Profile](docs/profile.md)
- [Purchases](docs/purchases.md)
- [Stocks](docs/stocks.md)
- [Stripe](docs/stripe.md)
- [Trading](docs/trading.md)

### 
```graphql
mutation SendAppLink($phone_number: String!, $query_string: String) {
  send_app_link(
    phone_number: $phone_number
    query_string: $query_string
  ){
    ok
  }
}
```
Example: `{"phone_number": "+...", "query_string": "af_js_web=true&af_ss_ver=2_2_0&pid=website_google_organictest&c=campaign_name"}`

## Development
Note: do not use foreign key relationships - all relationship should be of manual type (otherwise hasura fails during pipeline run).
