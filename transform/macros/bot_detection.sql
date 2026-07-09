{% macro is_bot_login(login_col) %}
    -- Heuristic bot detection. GitHub Apps get an explicit "[bot]" suffix;
    -- the rest are naming conventions plus a known-offenders list. This is a
    -- deliberate 95% solution: perfect bot detection would need the users API,
    -- which the events firehose doesn't include. Kept in one macro so staging
    -- and dims can never disagree about who's a bot.
    (
        {{ login_col }} is not null
        and (
            {{ login_col }} ilike '%[bot]%'
            or {{ login_col }} ilike '%-bot'
            or {{ login_col }} ilike '%_bot'
            or lower({{ login_col }}) in (
                'github-actions', 'dependabot', 'renovate', 'greenkeeper',
                'snyk-bot', 'codecov', 'coveralls', 'travis-ci', 'circleci',
                'azure-pipelines', 'netlify', 'vercel', 'imgbot', 'allcontributors'
            )
        )
    )
{% endmacro %}
