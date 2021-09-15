### Search

```graphql
query @cached(ttl: 120) {
  fetchNewsData(symbol: "AAPL", limit: 5) {
    datetime
    description
    imageUrl
    title
    url
    sourceName
    sourceUrl
  }
}
```