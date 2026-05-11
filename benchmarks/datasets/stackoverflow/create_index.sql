CREATE INDEX stackoverflow_posts_idx ON stackoverflow_posts
USING bm25 (
    id,
    title,
    body,
    tags,
    post_type_id,
    score,
    creation_date,
    view_count,
    answer_count,
    comment_count,
    owner_display_name,
    owner_user_id
) WITH (
    key_field = 'id',
    text_fields = '{
        "title": {"fast": true},
        "body": {"fast": true},
        "tags": {"tokenizer": {"type": "keyword"}},
        "owner_display_name": {"fast": true}
    }'
);

CREATE INDEX badges_idx ON badges
USING bm25 (
    id,
    name,
    date,
    user_id,
    class,
    tag_based
) WITH (
    key_field = 'id',
    text_fields = '{
        "name": {"fast": true}
    }'
 );

CREATE INDEX comments_idx ON comments
USING bm25 (
    id,
    post_id,
    score,
    text,
    creation_date,
    user_display_name
) WITH (
    key_field = 'id',
    text_fields = '{
        "text": {"fast": true},
        "user_display_name: {"tokenizer": {"type": "keyword"}},
    }'
);

CREATE INDEX users_idx ON users
USING bm25 (
    id,
    about_me,
    display_name,
    reputation
) WITH (
    key_field = 'id',
    text_fields = '{
        "about_me": {"fast": true},
        "display_name": {"fast": true}
    }'
);
