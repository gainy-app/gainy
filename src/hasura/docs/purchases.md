## Deprecated

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
