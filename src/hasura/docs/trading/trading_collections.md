# Trading collections
1. [Get recommended TTF weights](#get-recommended-ttf-weights)
2. [Reconfigure TTF holdings](#reconfigure-ttf-holdings)
3. [Cancel pending TTF order](#cancel-pending-ttf-order)
4. [Get actual TTF holding weights and amount](#get-actual-ttf-holding-weights-and-amount)
5. [Get actual TTF amount and history](#get-actual-ttf-amount-and-history)


### Trading mechanism
- All the requests are in PENDING state until 9.30am EST
- At 9.30 am they're sent to DW, status changes to PENDING_EXECUTION
- At 10.00 they're executed by DW
- From 10 to 11 we're getting events from DW _and updating TCS statuses_
- At 17 we download all accounts _and portfolios information_

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
mutation TradingReconfigureCollectionHoldings(
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
    trading_collection_version_id
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

### Cancel pending TTF order
```graphql
mutation TradingCancelPendingOrder(
    $profile_id: Int!
    $trading_collection_version_id: Int!
) {
  trading_cancel_pending_order(
    profile_id: $profile_id
    trading_collection_version_id: $trading_collection_version_id
  ){
    trading_collection_version_id
  }
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
query TradingGetProfileCollectionStatus($profile_id: Int!, $collection_id: Int!) {
  trading_profile_collection_status(where: {profile_id: {_eq: $profile_id}, collection_id: {_eq: $collection_id}}) {
    absolute_gain_1d
    absolute_gain_total
    actual_value
    relative_gain_1d
    relative_gain_total
    value_to_portfolio_value
  }
  app_trading_collection_versions(where: {profile_id: {_eq: $profile_id}, collection_id: {_eq: $collection_id}}, limit: 3, order_by: {created_at: desc}) {
    id
    created_at
    target_amount_delta
    history{
      tags
    }
  }
}
```
