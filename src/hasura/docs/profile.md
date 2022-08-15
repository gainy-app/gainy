### Create profile

```graphql
mutation {
  insert_app_profiles(objects: {
    avatar_url: "", 
    email: "test3@example.com", 
    first_name: "fn", 
    gender: 0, 
    last_name: "ln",
    legal_address: "legal_address",
    user_id: "AO0OQyz0jyL5lNUpvKbpVdAPvlI3",
    profile_interests: {
      data: [
        {
          interest_id: 3
        }
      ]
    },
    profile_scoring_setting: {
      data: {
        average_market_return: 6, # 6, 15, 25, 50
        damage_of_failure: 0.5, # 0 - ok, 1 - disaster
        if_market_drops_20_i_will_buy: 0.5, # 0 - sell, 1 - buy
        if_market_drops_40_i_will_buy: 0.5, # 0 - sell, 1 - buy
        investment_horizon: 0.5, # 0 - short, 1 - long
        risk_level: 0.5, # 0 - less risky, 1 - more reward
        stock_market_risk_level: "very_risky", # one of ['very_risky', 'somewhat_risky', 'neutral', 'somewhat_safe', 'very_safe']
        trading_experience: "never_tried", # one of ['never_tried', 'very_little', 'companies_i_believe_in', 'etfs_and_safe_stocks', 'advanced', 'daily_trader', 'investment_funds', 'professional', 'dont_trade_after_bad_experience']
        unexpected_purchases_source: "checking_savings" # one of ['checking_savings', 'stock_investments', 'credit_card', 'other_loans']
      }
    }
  }) {
    returning {
      id
    }
  }
}
```

### Update onboarding info

```graphql
mutation {
    update_app_profile_scoring_settings_by_pk(
        pk_columns: {profile_id: 11},
        _set: {
            average_market_return: 6
        }
    ) {
        average_market_return
    }
}
```

### Update interests and categories

```graphql
mutation set_recommendation_settings($profileId: Int!, $interests: [Int]!, $categories: [Int]!, $recommended_collections_count: Int) {
    set_recommendation_settings(profile_id: $profileId, interests: $interests, categories: $categories, recommended_collections_count: $recommended_collections_count) {
        recommended_collections {
            id
            collection {
                id
                name
                image_url
                enabled
                description
            }
        }
    }
}
```

### Update favorite collections

```graphql
mutation {
    insert_app_profile_favorite_collections(
        objects: [
            {profile_id: 11, collection_id: 3},
            {profile_id: 11, collection_id: 4},
        ],
        on_conflict: {
            constraint: profile_favorite_collections_pkey,
            update_columns: []
        }
    ) {
        returning {
            collection_id
        }
    }
    delete_app_profile_favorite_collections(
        where: {
            collection_id: {_in: [1,2]},
            profile_id: {_eq: 11}
        }
    ) {
        returning {
            collection_id
        }
    }
}
```

### Update watchlisted tickers

```graphql
mutation {
    insert_app_profile_watchlist_tickers(
        objects: [
            {profile_id: 1, symbol: "AAPL"},
        ],
        on_conflict: {
            constraint: profile_watchlist_tickers_pkey,
            update_columns: []
        }
    ) {
        returning {
            symbol
        }
    }
    delete_app_profile_watchlist_tickers(
        where: {
            symbol: {_in: ["AAPL"]},
            profile_id: {_eq: 1}
        }
    ) {
        returning {
            symbol
        }
    }
}
```

### Metrics settings

```graphql
mutation {
    delete_app_profile_ticker_metrics_settings(
        where: {
            id: {_in: [20]},
            profile_id: {_eq: 1}
        }
    ) {
        returning {
            field_name
        }
    }
    insert_app_profile_ticker_metrics_settings(
        objects: [
            {profile_id: 1, field_name: "market_capitalization", order: 0},
            {id: 21, profile_id: 1, field_name: "avg_volume_10d", order: 1},
            {id: 22, profile_id: 1, field_name: "avg_volume_90d", order: 2},
        ],
        on_conflict: {
            constraint: profile_ticker_metrics_settings_pkey,
            update_columns: [field_name order]
        }
    ) {
        returning {
            id
            field_name
            order
        }
    }
}

query{
  app_profile_ticker_metrics_settings(where: {profile_id: {_eq: 1}}, order_by: {order: asc}){
    id
    field_name
    order
  }
}
```

### Metrics settings for a collection

```graphql
mutation {
    delete_app_profile_ticker_metrics_settings(
        where: {
            id: {_in: [26]},
            profile_id: {_eq: 1}
        }
    ) {
        returning {
            field_name
        }
    }
    insert_app_profile_ticker_metrics_settings(
        objects: [
            {profile_id: 1, collection_id: 1, field_name: "market_capitalization", order: 0},
            {id: 27, profile_id: 1, collection_id: 1, field_name: "avg_volume_10d", order: 1},
            {id: 28, profile_id: 1, collection_id: 1, field_name: "avg_volume_90d", order: 2},
        ],
        on_conflict: {
            constraint: profile_ticker_metrics_settings_pkey,
            update_columns: [collection_id field_name order]
        }
    ) {
        returning {
            id
            collection_id 
            field_name
            order
        }
    }
}

query{
  app_profile_ticker_metrics_settings(where: {profile_id: {_eq: 1}, collection_id: {_eq: 1}}, order_by: {order: asc}){
    id
    collection_id
    field_name
    order
  }
}
```

### Delete user

```graphql
mutation {
  delete_app_profiles_by_pk(id: 1) {
    id
  }
}
```