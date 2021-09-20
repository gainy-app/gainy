### List

```graphql
query {
  collections {
    name
    image_url
    id
    description
    ticker_collections_aggregate {
      aggregate {
        count
      }
    }
  }
}
```

### Collection details
```graphql
query {
  collections {
    id
    image_url
    name
    description
    ticker_collections_aggregate {
      aggregate {
        count
      }
    }
    ticker_collections {
      ticker {
        symbol
        name
        description
        ticker_financials {
          dividend_growth
          pe_ratio
          market_capitalization
          highlight
        }
        ticker_industries {
          gainy_industry {
            id
            name
          }
        }
      }
    }
  }
}
```

### Add to favorites

```graphql
mutation {
  insert_app_profile_favorite_collections_one(object: {collection_id: 1, profile_id: 1}) {
    collection_id
    profile_id
  }
  delete_app_profile_favorite_collections_by_pk(collection_id: 1, profile_id: 1) {
    profile_id
    collection_id
  }
}
```