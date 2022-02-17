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
        enabled
        size
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
        enabled
        size
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

### Recommended Collections

```graphql
query {
    get_recommended_collections(profile_id: 1) {
        id
        collection {
            name
            ticker_collections {
                ticker {
                    symbol
                }
            }
        }
    }
}
```

### Collection search

```graphql
query  {
    search_collections(query: "green energy") {
        id
        collection {
            name
            image_url
            enabled
            description
            ticker_collections_aggregate {
                aggregate {
                    count
                }
            }
        }
    }
}
```

### Home tab

```graphql
query home_tab($profileId: Int, $rankedCount: Int) {
    profile_collection_tickers_performance_ranked(where: {profile_id: {_eq: $profileId}, gainer_rank: {_lte: $rankedCount}, loser_rank: {_lte: $rankedCount}}) {
        gainer_rank
        loser_rank
        profile_id
        relative_daily_change
        symbol
        updated_at
    }
    app_profile_favorite_collections(where: {profile_id: {_eq: $profileId}}, order_by: {collection: {metrics: {relative_daily_change: desc}}}) {
        collection{
            metrics {
                profile_id
                relative_daily_change
                updated_at
            }
        }
    }
}
```