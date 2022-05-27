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