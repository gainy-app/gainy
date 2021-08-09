create table app.profiles
(
    id                    serial
        constraint profile_pkey
        primary key,
    email                 varchar                                not null
        constraint profile_email_key
        unique
        constraint email
        check ((email)::text ~* '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'::text),
    first_name            varchar                                not null,
    last_name             varchar                                not null,
    risk_level            real                                   not null
        constraint risk_level
        check (risk_level >= (0)::double precision),
    gender                smallint                               not null
        constraint gender
        check ((gender IS NULL) OR (gender = ANY (ARRAY [0, 1]))),
    investment_horizon    real                                   not null
        constraint investment_horizon
        check (investment_horizon > (0)::double precision),
    investment_experience varchar                                not null
        constraint investment_experience
        check ((investment_experience)::text = ANY
        ((ARRAY ['none'::character varying, 'low'::character varying, 'medium'::character varying, 'expert'::character varying, 'unknown'::character varying])::text[])),
    created_at            timestamp with time zone default now() not null,
    investment_goals      jsonb                                  not null
        constraint investment_goals
        check (investment_goals <@ '["growth", "preserve", "income", "speculation"]'::jsonb)
);

