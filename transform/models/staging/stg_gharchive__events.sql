-- Staging (silver): one row per event, deduplicated and typed.
--
-- Dedup rationale: GH Archive hours shouldn't contain duplicate event ids, and
-- our hour-level idempotency prevents cross-run duplicates — but "shouldn't"
-- is not a guarantee we let the marts depend on. If a duplicate ever appears,
-- we keep the most recently ingested copy. The uniqueness test on event_id
-- then proves the guarantee downstream instead of assuming it upstream.

with source as (

    select * from {{ source('gharchive', 'events') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as _row_num
    from source

)

select
    -- identifiers
    event_id,
    event_type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_login,

    -- timestamps
    created_at,
    cast(created_at as date) as event_date,
    date_trunc('hour', created_at) as event_hour_ts,

    -- event attributes
    payload_action,
    pr_merged,
    pr_additions,
    pr_deletions,
    pr_changed_files,
    repo_language,
    repo_stars,
    push_distinct_size,
    create_ref_type,

    -- lineage
    ingested_at,
    source_file

from deduplicated
where _row_num = 1
