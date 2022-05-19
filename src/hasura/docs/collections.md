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
            market_capitalization_sum
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
    collections(where: {id: {_eq: 231}}) {
        id
        uniq_id
    }
    collection_chart(where: {collection_uniq_id: {_eq: "1_231"}, period: {_eq: "1w"}}) {
        datetime
        period
        adjusted_close
    }
    # collection_piechart(where: {collection_uniq_id: {_eq: "1_231"}, entity_type: {_eq: "ticker"}}) {
    # collection_piechart(where: {collection_uniq_id: {_eq: "1_231"}, entity_type: {_eq: "interest"}}) {
    collection_piechart(where: {collection_uniq_id: {_eq: "1_231"}, entity_type: {_eq: "category"}}) {
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

### Match Score

```graphql
query GetCollectionMatchScore($collectionUniqId: String!, $profileId: Int!) {
    collection_match_score(where: {profile_id: {_eq: $profileId}, collection_uniq_id: {_eq: $collectionUniqId}}) {
        match_score
        risk_level
        risk_similarity
        interest_level
        interest_similarity
        category_level
        category_similarity
    }
    collection_match_score_explanation(where: {profile_id: {_eq: $profileId}, collection_uniq_id: {_eq: $collectionUniqId}}) {
        interest {
            id
            name
        }
        category {
            id
            name
        }
    }
}
```
