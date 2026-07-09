-- Date dimension: continuous day spine covering the event window.
-- Built with generate_series instead of a dbt package to stay dependency-free;
-- being continuous (no gaps even on days with zero data) is what makes
-- week-over-week momentum queries safe to write with joins instead of window
-- tricks.

with bounds as (

    select
        min(event_date) as start_date,
        max(event_date) as end_date
    from {{ ref('stg_gharchive__events') }}

),

spine as (

    select
        cast(unnest(generate_series(
            cast(start_date as timestamp),
            cast(end_date as timestamp),
            interval 1 day
        )) as date) as date_day
    from bounds

)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    cast(date_trunc('week', date_day) as date) as week_start_date,
    extract(isodow from date_day) in (6, 7) as is_weekend
from spine
