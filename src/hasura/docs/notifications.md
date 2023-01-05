# Notifications

### List
```graphql
query ListNotifications($profile_id: Int!) {
  notifications(where: {profile_id: {_eq: $profile_id}}, order_by: [{created_at: desc}]) {
    created_at
    data
    is_viewed
    notification_uuid
    profile_id
    text
    title
  }
}
```

### View
```graphql
mutation MarkNotificationsViewed($notifications: [app_profile_notification_viewed_insert_input!]!) {
  insert_app_profile_notification_viewed(objects: $notifications, on_conflict: {constraint: profile_notification_viewed_pkey}) {
    affected_rows
  }
}
```

with params like `{"notifications": [{"notification_uuid": "d2bd565c-e35f-4227-a990-5f8bea7dc597", "profile_id": 1}]}`

### Unread count
```graphql
query GetNotViewedNotificationsCount($profile_id: Int!) {
  profile_flags(where: {profile_id: {_eq: $profile_id}}) {
    not_viewed_notifications_count
  }
}
```
