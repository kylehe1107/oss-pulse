-- Fact table (gold): grain = one row per GitHub event.
--
-- Incremental materialization: each run only processes rows ingested since the
-- last build (filter on ingested_at, not created_at — so a re-ingested or
-- backfilled OLD hour still enters the fact table). unique_key=event_id means
-- a re-processed event replaces itself instead of duplicating: idempotency at
-- the warehouse layer, mirroring the idempotency at the lake layer.

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='fail'
    )
}}

select
    -- primary key
    event_id,

    -- foreign keys (star schema)
    actor_id,
    repo_id,
    event_date,

    -- degenerate dimensions (event-level attributes with no natural dim table)
    event_type,
    payload_action,
    create_ref_type,

    -- timestamps
    created_at,
    event_hour_ts,

    -- measures
    pr_merged,
    pr_additions,
    pr_deletions,
    pr_changed_files,
    push_distinct_size,

    -- lineage
    ingested_at

from {{ ref('stg_gharchive__events') }}

{% if is_incremental() %}
where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
