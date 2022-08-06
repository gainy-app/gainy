create table if not exists raw_data.blogs
(
    alt_text_for_main_image varchar,
    author_id               varchar,
    category_link           varchar,
    category_name           varchar,
    created_by              varchar,
    created_on              varchar,
    cta_second_text         varchar,
    cta_text_inside_block   varchar,
    date_added              varchar,
    faq_id                  varchar,
    first_paragraph         varchar,
    id                      varchar not null
        primary key,
    main_image              varchar,
    meta_description        varchar,
    name                    varchar,
    post_body               varchar,
    post_summary            varchar,
    published_by            varchar,
    published_on            varchar,
    rate_rating             double precision,
    rate_votes              double precision,
    related_articles        jsonb,
    slug                    varchar,
    sta_text                varchar,
    title_tag               varchar,
    updated_by              varchar,
    updated_on              varchar,
    updated_at              timestamp,
    _sdc_batched_at         timestamp,
    _sdc_deleted_at         varchar,
    _sdc_extracted_at       timestamp
);

alter table raw_data.blogs add if not exists updated_at timestamp;
