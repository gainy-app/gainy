### List

```graphql
query {
    collections {
        name
        image_url
        id
        description
        size(args: {profile_id: 1})
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
        size(args: {profile_id: 1})
        ticker_collections(args: {profile_id: 1}) {
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

### Recommended Collections

```graphql
query {
    get_recommended_collections(profile_id: 1) {
        id
        collection {
            name
        }
    }
}
```