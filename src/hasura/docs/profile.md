1. [Create profile](#create-profile)
2. [Update onboarding info](#update-onboarding-info)
3. [Update interests and categories](#update-interests-and-categories)
4. [Update favorite collections](#update-favorite-collections)
5. [Update watchlisted tickers](#update-watchlisted-tickers)
6. [Metrics settings](#metrics-settings)
7. [Metrics settings for a collection](#metrics-settings-for-a-collection)
8. [Delete user](#delete-user)
9. [Profile flags](#profile-flags)

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
  }) {
    returning {
      id
    }
  }
}
```
Minimalistic:
```graphql
mutation CreateAppProfile($email: String!, $firstName: String!, $lastName: String!, $userID: String!){
  insert_app_profiles(objects: {
    email: $email, 
    first_name: $firstName, 
    last_name: $lastName,
    user_id: $userID,
  }) {
    returning {
      id
    }
  }
}
```

### Update onboarding info

```graphql
mutation UpdateAppProfileScoringSettings(
  $profileID: Int!, 
  $averageMarketReturn: Int!, 
  $damageOfFailure: Float!, 
  $marketLoss20: Float!, 
  $marketLoss40: Float!, 
  $investemtHorizon: Float!, 
  $riskLevel: Float!, 
  $stockMarketRiskLevel: String!, 
  $tradingExperience: String!, 
  $unexpectedPurchaseSource: String!
) {
  insert_app_profile_scoring_settings_one(
    object: {
      profile_id: $profileID, 
      average_market_return: $averageMarketReturn, 
      damage_of_failure: $damageOfFailure, 
      if_market_drops_20_i_will_buy: $marketLoss20, 
      if_market_drops_40_i_will_buy: $marketLoss40, 
      investment_horizon: $investemtHorizon, 
      risk_level: $riskLevel, 
      stock_market_risk_level: $stockMarketRiskLevel, 
      trading_experience: $tradingExperience, 
      unexpected_purchases_source: $unexpectedPurchaseSource
    }, 
    on_conflict: {
      constraint: profile_scoring_settings_pkey, 
      update_columns: [
        average_market_return
        damage_of_failure
        if_market_drops_20_i_will_buy
        if_market_drops_40_i_will_buy
        investment_horizon
        risk_level
        stock_market_risk_level 
        trading_experience
        unexpected_purchases_source
      ]
    }
  ) {
    __typename
    profile_id
  }
}

```

### Update interests and categories

```graphql
mutation set_recommendation_settings($profileId: Int!, $interests: [Int], $categories: [Int], $recommended_collections_count: Int) {
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

### Profile flags

```graphql
{
  app_profiles {
    flags{
      is_trading_enabled
      is_region_changing_allowed
      is_personalization_enabled
    }
  }
}
```
Update
```graphql
mutation UpdateProfileFlags($profile_id: Int!, $is_trading_enabled: Boolean) {
  insert_app_profile_flags_one(
      object: {profile_id: $profile_id, is_trading_enabled: $is_trading_enabled}, 
      on_conflict: {constraint: profile_flags_pkey, update_columns: is_trading_enabled}
  ) {
    profile_id
  }
}
```
