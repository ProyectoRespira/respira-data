{% macro aqi_linear(value, c_lo, c_hi, i_lo, i_hi) -%}
  (
    (({{ i_hi }} - {{ i_lo }}) / ({{ c_hi }} - {{ c_lo }})) * ({{ value }} - {{ c_lo }}) + {{ i_lo }}
  )::integer
{%- endmacro %}

{% macro aqi_pm25(value) -%}
  case
    when {{ value }} is null then null
    when {{ value }} < 0 then null
    when {{ value }} <= 12.0 then {{ aqi_linear(value, 0.0, 12.0, 0, 50) }}
    when {{ value }} <= 35.4 then {{ aqi_linear(value, 12.1, 35.4, 51, 100) }}
    when {{ value }} <= 55.4 then {{ aqi_linear(value, 35.5, 55.4, 101, 150) }}
    when {{ value }} <= 150.4 then {{ aqi_linear(value, 55.5, 150.4, 151, 200) }}
    when {{ value }} <= 250.4 then {{ aqi_linear(value, 150.5, 250.4, 201, 300) }}
    when {{ value }} <= 350.4 then {{ aqi_linear(value, 250.5, 350.4, 301, 400) }}
    when {{ value }} <= 500.4 then {{ aqi_linear(value, 350.5, 500.4, 401, 500) }}
    else 500
  end
{%- endmacro %}

{% macro aqi_pm10(value) -%}
  case
    when {{ value }} is null then null
    when {{ value }} < 0 then null
    when {{ value }} <= 54.0 then {{ aqi_linear(value, 0.0, 54.0, 0, 50) }}
    when {{ value }} <= 154.0 then {{ aqi_linear(value, 55.0, 154.0, 51, 100) }}
    when {{ value }} <= 254.0 then {{ aqi_linear(value, 155.0, 254.0, 101, 150) }}
    when {{ value }} <= 354.0 then {{ aqi_linear(value, 255.0, 354.0, 151, 200) }}
    when {{ value }} <= 424.0 then {{ aqi_linear(value, 355.0, 424.0, 201, 300) }}
    when {{ value }} <= 504.0 then {{ aqi_linear(value, 425.0, 504.0, 301, 400) }}
    when {{ value }} <= 604.0 then {{ aqi_linear(value, 505.0, 604.0, 401, 500) }}
    else 500
  end
{%- endmacro %}

{% macro aqi_level(aqi_value) -%}
  case
    when {{ aqi_value }} is null then null
    when {{ aqi_value }} <= 50 then 1
    when {{ aqi_value }} <= 100 then 2
    when {{ aqi_value }} <= 150 then 3
    when {{ aqi_value }} <= 200 then 4
    when {{ aqi_value }} <= 300 then 5
    when {{ aqi_value }} <= 500 then 6
    else 6
  end
{%- endmacro %}
