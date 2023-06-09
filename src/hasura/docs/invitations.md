## Deprecated

### Invitations

Invitations store information about which users invited which users. 
As invitations are not personalized, invitation should be created by the profile accepting the invitation (to_profile_id) after it completed onboarding.
It's possible to create one invitation per to_profile_id, and you can't update or delete them after.

```graphql
mutation RedeemInvitation {
  insert_app_invitations_one(object: {from_profile_id: 1, to_profile_id: 2}) {
    id
  }
}
```

### Invitation History

```graphql
query InvitationHistory($profile_id: Int!) {
  invitation_history(where: {profile_id: {_eq: $profile_id}}, order_by: {invited_at: desc}) {
    name
    step1_signed_up
    step2_brokerate_account_open
    step3_deposited_enough
    is_complete
  }
}
```