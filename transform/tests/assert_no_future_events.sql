-- Singular test: no event may claim to have happened in the future.
-- A future created_at means a clock/timezone bug somewhere in the chain
-- (GitHub, the archive, or our parsing) and would silently distort every
-- "momentum" metric, so the build fails loudly instead.
-- 1h grace absorbs clock skew at the publication boundary.

select
    event_id,
    created_at
from {{ ref('fct_events') }}
where created_at > current_timestamp + interval 1 hour
