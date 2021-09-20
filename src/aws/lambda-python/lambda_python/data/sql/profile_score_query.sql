with profile_industries as (
    select distinct
        ap.profile_id,
        p.user_id as hasura_user_id,
        tid.industry_id
    from
        app.profile_interests ap
            join
        app.profiles p
        on
                ap.profile_id = p.id
            join
        ticker_interests tin
        on
                ap.interest_id = tin.interest_id
            join
        ticker_industries tid
        on
                tid.symbol = tin.symbol
)
select
    "pi".profile_id,
    json_object_agg("pi".industry_id, 1.0) as industry_score_json
from
    profile_industries "pi"
where
        "pi".profile_id = '{0}'
group by
    "pi".profile_id;