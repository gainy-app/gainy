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

## Development
Note: do not use foreign key relationships - all relationship should be of manual type (otherwise hasura fails during pipeline run).
