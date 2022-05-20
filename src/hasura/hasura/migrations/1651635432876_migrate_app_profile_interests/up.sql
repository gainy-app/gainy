insert into "app"."profile_interests"
select profile_id, 1 as interest_id from "app"."profile_interests" where interest_id in (56)
union all
select profile_id, 2 as interest_id from "app"."profile_interests" where interest_id in (3, 17, 39)
union all
select profile_id, 9 as interest_id from "app"."profile_interests" where interest_id in (35)
union all
select profile_id, 10 as interest_id from "app"."profile_interests" where interest_id in (19, 23, 47)
union all
select profile_id, 12 as interest_id from "app"."profile_interests" where interest_id in (31)
union all
select profile_id, 13 as interest_id from "app"."profile_interests" where interest_id in (48)
union all
select profile_id, 14 as interest_id from "app"."profile_interests" where interest_id in (15)
union all
select profile_id, 18 as interest_id from "app"."profile_interests" where interest_id in (44, 50, 51, 55)
union all
select profile_id, 20 as interest_id from "app"."profile_interests" where interest_id in (21, 53)
union all
select profile_id, 25 as interest_id from "app"."profile_interests" where interest_id in (52)
union all
select profile_id, 26 as interest_id from "app"."profile_interests" where interest_id in (40)
union all
select profile_id, 36 as interest_id from "app"."profile_interests" where interest_id in (37)
union all
select profile_id, 41 as interest_id from "app"."profile_interests" where interest_id in (54)
on conflict do nothing;

delete from "app"."profile_interests" where interest_id in (56, 3, 17, 39, 35, 19, 23, 47, 31, 48, 15, 44, 50, 51, 55, 21, 53, 52, 40, 37, 54);
