{% test non_negative(model, column_name) %}

    -- Custom generic test: fails if any non-null value is negative.
    -- Applied to count-like measures (additions, deletions, sizes) where a
    -- negative value can only mean upstream corruption.

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} < 0

{% endtest %}
