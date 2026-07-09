-- Intermediate: repository enrichment attributes.
--
-- The events firehose only exposes a repo's language and star count inside
-- PullRequestEvent payloads (the full base-repo object rides along). This
-- model condenses that to one row per repo: the latest known language and the
-- latest known star count. Repos with no PR events in the window stay
-- unenriched (language null) — a documented limitation of event-derived data,
-- not a bug.

with pr_events as (

    select
        repo_id,
        repo_language,
        repo_stars,
        created_at
    from {{ ref('stg_gharchive__events') }}
    where event_type = 'PullRequestEvent'

),

latest_language as (

    select
        repo_id,
        repo_language as language
    from pr_events
    where repo_language is not null
    qualify row_number() over (
        partition by repo_id
        order by created_at desc
    ) = 1

),

latest_stars as (

    select
        repo_id,
        repo_stars as stars
    from pr_events
    where repo_stars is not null
    qualify row_number() over (
        partition by repo_id
        order by created_at desc
    ) = 1

),

repo_ids as (

    select repo_id from latest_language
    union
    select repo_id from latest_stars

)

select
    repo_ids.repo_id,
    latest_language.language,
    latest_stars.stars
from repo_ids
left join latest_language on repo_ids.repo_id = latest_language.repo_id
left join latest_stars on repo_ids.repo_id = latest_stars.repo_id
