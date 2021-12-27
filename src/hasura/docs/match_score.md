### Get match score by ticker

```graphql
query {
    get_match_score_by_ticker(profile_id: 1, symbol: "GOOGL") {
        symbol
        match_score
        fits_risk
        fits_categories
        fits_interests
        ticker {
            name
        }
    }
}
```

### Get match scores by ticker list

```graphql
query {
    get_match_scores_by_ticker_list(profile_id: 1, symbols: ["GOOGL", "AAPL"]) {
        symbol
        match_score
        fits_risk
        fits_categories
        fits_interests
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
        match_score
        symbol
        fits_risk
        fits_categories
        fits_interests
        ticker {
            name
        }
    }
}
```