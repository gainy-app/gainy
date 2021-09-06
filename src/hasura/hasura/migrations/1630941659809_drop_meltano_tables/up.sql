alter table app.profile_categories drop constraint profile_categories_category_id_fkey;
DROP table app.categories;
DROP table app.collection_symbols;
alter table app.profile_favorite_collections drop constraint profile_favorite_collections_collection_id_fkey;
DROP table app.collections;