### Invitations

Invitations store information about which users invited which users and affect subscription promotions. 
As invitations are not personalized, invitation should be created by the profile accepting the invitation (to_profile_id) after it completed onboarding.
It's possible to create one invitation per to_profile_id, and you can't update or delete them after

```graphql
mutation {
  insert_app_invitations_one(object: {from_profile_id: 1, to_profile_id: 2}) {
    id
  }
}
```

After the invitation is created, the `profile.subscription_end_date` is updated.

### Purchases

To update profile's purchases from RevenueCat, the app must use the following query:
```graphql
mutation {
  update_purchases(profile_id: 1) {
    subscription_end_date
  }
}
```
Returned `subscription_end_date` field is the newly calculated end date for the profile after purchases are updated.

### Promo Codes

To get promo code metadata the app must use the following query:
```graphql
query GetPromocode($code: String!) { 
    get_promocode(code: $code) { 
        id 
        name 
        description
        config
    }
}
```
If the promo code is not found or inactive, `null` will be returned. 

Config field is JSON string with the following format: 
```json
{"tariff_mapping": {"gainy_80_r_y1": "gainy_56_r_y1"}}
```
