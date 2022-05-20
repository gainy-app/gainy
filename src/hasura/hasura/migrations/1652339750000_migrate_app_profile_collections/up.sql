insert into "app"."profile_favorite_collections"
select profile_id, 16 as collection_id from "app"."profile_favorite_collections" where collection_id in (20003)
union all
select profile_id, 19 as collection_id from "app"."profile_favorite_collections" where collection_id in (20006)
union all
select profile_id, 73 as collection_id from "app"."profile_favorite_collections" where collection_id in (20030)
union all
select profile_id, 42 as collection_id from "app"."profile_favorite_collections" where collection_id in (20007)
union all
select profile_id, 65 as collection_id from "app"."profile_favorite_collections" where collection_id in (20028)
union all
select profile_id, 244 as collection_id from "app"."profile_favorite_collections" where collection_id in (20017, 20019)
union all
select profile_id, 107 as collection_id from "app"."profile_favorite_collections" where collection_id in (20013)
union all
select profile_id, 225 as collection_id from "app"."profile_favorite_collections" where collection_id in (20018, 20029)
union all
select profile_id, 134 as collection_id from "app"."profile_favorite_collections" where collection_id in (20034)
union all
select profile_id, 92 as collection_id from "app"."profile_favorite_collections" where collection_id in (138, 209, 20039, 20040)
union all
select profile_id, 86 as collection_id from "app"."profile_favorite_collections" where collection_id in (148)
union all
select profile_id, 154 as collection_id from "app"."profile_favorite_collections" where collection_id in (203)
union all
select profile_id, 162 as collection_id from "app"."profile_favorite_collections" where collection_id in (20032)
union all
select profile_id, 170 as collection_id from "app"."profile_favorite_collections" where collection_id in (199)
union all
select profile_id, 172 as collection_id from "app"."profile_favorite_collections" where collection_id in (175, 20035)
union all
select profile_id, 140 as collection_id from "app"."profile_favorite_collections" where collection_id in (235)
union all
select profile_id, 240 as collection_id from "app"."profile_favorite_collections" where collection_id in (20001)
union all
select profile_id, 241 as collection_id from "app"."profile_favorite_collections" where collection_id in (20002)
union all
select profile_id, 242 as collection_id from "app"."profile_favorite_collections" where collection_id in (20010)
union all
select profile_id, 243 as collection_id from "app"."profile_favorite_collections" where collection_id in (20012)
on conflict do nothing;

delete from "app"."profile_favorite_collections" where collection_id in (20003, 20006, 20030, 20007, 20028, 20017, 20019, 20013, 20018, 20029, 20034, 138, 209, 20039, 20040, 148, 203, 20032, 199, 175, 20035, 235, 20001, 20002, 20010, 20012);
