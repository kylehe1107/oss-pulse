-- Actor dimension: one row per user/bot seen in the event window.
-- is_bot is a heuristic (see macros/bot_detection.sql) computed on the latest
-- observed login; the dashboard excludes bots from "contributor momentum"
-- because CI bots would otherwise dominate every activity metric.

with events as (

    select * from {{ ref('stg_gharchive__events') }}

),

latest_identity as (

    select
        actor_id,
        actor_login
    from events
    qualify row_number() over (
        partition by actor_id
        order by created_at desc
    ) = 1

),

activity as (

    select
        actor_id,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        count(*) as n_events,
        count(distinct repo_id) as n_repos
    from events
    group by actor_id

)

select
    activity.actor_id,
    latest_identity.actor_login,
    {{ is_bot_login('latest_identity.actor_login') }} as is_bot,
    activity.first_seen_at,
    activity.last_seen_at,
    activity.n_events,
    activity.n_repos
from activity
inner join latest_identity
    on activity.actor_id = latest_identity.actor_id
