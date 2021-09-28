### Get match score by ticker

```graphql
query {
    get_match_score_by_ticker(profile_id: 1, symbol: "GOOGL") {
        is_match
        match_score
        symbol
        explanation
        ticker {
            name
        }
    }

}
```

### Get match scores by collections

```graphql
query {
    get_match_scores_by_collections(profile_id: 1, collection_ids: [45, 83]) {
        is_match
        match_score
        symbol
        collection_id
        explanation
        ticker {
            name
        }
    }
}
```