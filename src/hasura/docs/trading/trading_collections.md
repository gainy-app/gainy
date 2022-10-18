# Trading collections
1. [Get recommended TTF weights](#get-recommended-ttf-weights)
2. [Reconfigure TTF holdings](#reconfigure-ttf-holdings)
3. [Get actual TTF holding weights and amount](#get-actual-ttf-holding-weights-and-amount)
4. [Get actual TTF amount and history](#get-actual-ttf-amount-and-history)

### Get recommended TTF weights
```graphql
query GetCollectionTickerActualWeights($collection_id: Int!){
  collection_ticker_actual_weights(where: {collection_id: {_eq: $collection_id}}){
    symbol
    weight
  }
}
```

### Reconfigure TTF holdings
Generate the trades to make user's TTF holdings look like input params.
```graphql
mutation TradingWithdrawFunds(
    $profile_id: Int!
    $collection_id: Int!
    $weights: [TickerWeight]
    $target_amount_delta: Float!
) {

  trading_reconfigure_collection_holdings(
    profile_id: $profile_id
    collection_id: $collection_id
    weights: $weights
    target_amount_delta: $target_amount_delta
  ){
    ok
  }
}
```
Types:
```graphql
input TickerWeight {
    symbol: String!
    weight: Float!    
}
```

### Get actual TTF holding weights and amount
```graphql
query TradingGetActualCollectionHoldings(
  $profile_id: Int!
  $collection_id: Int!
){
  trading_get_actual_collection_holdings(
    profile_id: $profile_id
    collection_id: $collection_id
  ){
    symbol
    target_weight
    actual_weight
    value
  }
}
```


### Get actual TTF amount and history
```graphql
query TradingGetCollectionData($profile_id: Int!, $collection_id: Int!) {
  trading_get_collection_data(profile_id: $profile_id, collection_id: $collection_id) {
    history {
      pending {
        created_at
        executed_at
        target_amount_delta
      }
      successful {
        created_at
        executed_at
        target_amount_delta
      }
    }
    actual_value
  }
}
```
