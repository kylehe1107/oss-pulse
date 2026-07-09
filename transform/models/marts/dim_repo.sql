-- Repository dimension: one row per repo seen in the event window.
-- Names can change over time (renames/transfers); we keep the latest observed
-- name and enrich with language/stars from PR payloads where available.

with events as (

    select * from {{ ref('stg_gharchive__events') }}

),

latest_identity as (

    select
        repo_id,
        repo_name,
        org_login
    from events
    qualify row_number() over (
        partition by repo_id
        order by created_at desc
    ) = 1

),

activity as (

    select
        repo_id,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        count(*) as n_events
    from events
    group by repo_id

)

select
    activity.repo_id,
    latest_identity.repo_name,
    latest_identity.org_login,
    attrs.language,
    attrs.stars,
    activity.first_seen_at,
    activity.last_seen_at,
    activity.n_events
from activity
inner join latest_identity
    on activity.repo_id = latest_identity.repo_id
left join {{ ref('int_repo_attributes') }} as attrs
    on activity.repo_id = attrs.repo_id
