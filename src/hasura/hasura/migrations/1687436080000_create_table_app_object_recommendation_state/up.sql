create table app.object_recommendation_state
(
    object_id   text,
    object_type text,
    state_hash  text,
    updated_at  timestamp default now(),
    primary key (object_id, object_type)
);
