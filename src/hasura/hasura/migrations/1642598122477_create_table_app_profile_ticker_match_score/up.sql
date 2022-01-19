-- Drop deprecated tables and views on TEST and LOCAL envs
drop view if exists app.profile_ticker_match_score;
drop table if exists app.profile_ticker_match_score_json;

create table app.profile_ticker_match_score
(
    profile_id       int4 NOT NULL,
    symbol           text NOT NULL,
    match_score      int4 NOT NULL,
    fits_risk        int4 NOT NULL,
    risk_similarity  float8 NOT NULL,
    fits_categories  int4 NOT NULL,
    fits_interests   int4 NOT NULL,
    category_matches text NOT NULL,
    interest_matches text NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (profile_id, symbol)
);