{% macro revenue_calc(booking_amount, cleaning_fee, service_fee) %}
    coalesce({{ booking_amount }}, 0)
    + coalesce({{ cleaning_fee }}, 0)
    + coalesce({{ service_fee }}, 0)
{% endmacro %}
