### Get match score by ticker

```graphql
query {
    get_match_score_by_ticker(profile_id: 1, symbol: "GOOGL") {
        symbol
        is_match
        match_score
        risk_level_explanation
        category_level_explanation
        interest_level_explanation
        ticker {
            name
        }
    }
}
```

### Get match scores by collection id

```graphql
query {
    get_match_scores_by_collection(profile_id: 1, collection_id: 45) {
        is_match
        match_score
        symbol
        risk_level_explanation
        category_level_explanation
        interest_level_explanation
        ticker {
            name
        }
    }
}
```