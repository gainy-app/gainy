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
        name
        image_url
        enabled
        description
        size
        metrics {
            absolute_daily_change
            relative_daily_change
            updated_at
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
    profile_collection_tickers_performance_ranked(where: {profile_id: {_eq: $profileId}, _or: [{gainer_rank: {_lte: $rankedCount}}, {loser_rank: {_lte: $rankedCount}}]}) {
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
                actual_price
                absolute_daily_change
                relative_daily_change
                updated_at
            }
        }
    }
}
```

### Charts

```graphql
{
  collection_chart(where: {collection_id: {_eq: 231}, period: {_eq: "1d"}}, limit: 1) {
    datetime
    period
    adjusted_close
  }
#  collection_piechart(where: {collection_id: {_eq: 231}, entity_type: {_eq: "ticker"}}) {
#  collection_piechart(where: {collection_id: {_eq: 231}, entity_type: {_eq: "category"}}) {
  collection_piechart(where: {collection_id: {_eq: 231}, entity_type: {_eq: "interest"}}) {
    weight
    entity_type
    relative_daily_change
    entity_name
    entity_id
    absolute_value
    absolute_daily_change
  }
}
```
