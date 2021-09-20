### Create profile

```graphql
mutation {
  insert_app_profiles(objects: {
    avatar_url: "", 
    email: "test3@example.com", 
    first_name: "fn", 
    gender: 0, 
    last_name: "ln", 
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

### Update interests

```graphql
mutation {
    insert_app_profile_interests(
        objects: [
            {profile_id: 11, interest_id: 3},
            {profile_id: 11, interest_id: 4},
        ],
        on_conflict: {
            constraint: profile_interests_pkey,
            update_columns: []
        }
    ) {
        returning {
            interest_id
        }
    }
    delete_app_profile_interests(
        where: {
            interest_id: {_in: [1,2]},
            profile_id: {_eq: 11}
        }
    ) {
        returning {
            interest_id
        }
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