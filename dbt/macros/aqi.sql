{% macro aqi_linear(value, c_lo, c_hi, i_lo, i_hi) -%}
  round(
    (({{ i_hi }} - {{ i_lo }}) / ({{ c_hi }} - {{ c_lo }})) * ({{ value }} - {{ c_lo }}) + {{ i_lo }}
  )::integer
{%- endmacro %}

{% macro aqi_pm25(value) -%}
  {% set concentration = "trunc((" ~ value ~ ")::numeric, 1)" %}
  case
    when {{ concentration }} is null then null
    when {{ concentration }} < 0 then null
    when {{ concentration }} <= 12.0 then {{ aqi_linear(concentration, 0.0, 12.0, 0, 50) }}
    when {{ concentration }} <= 35.4 then {{ aqi_linear(concentration, 12.1, 35.4, 51, 100) }}
    when {{ concentration }} <= 55.4 then {{ aqi_linear(concentration, 35.5, 55.4, 101, 150) }}
    when {{ concentration }} <= 150.4 then {{ aqi_linear(concentration, 55.5, 150.4, 151, 200) }}
    when {{ concentration }} <= 250.4 then {{ aqi_linear(concentration, 150.5, 250.4, 201, 300) }}
    when {{ concentration }} <= 350.4 then {{ aqi_linear(concentration, 250.5, 350.4, 301, 400) }}
    when {{ concentration }} <= 500.4 then {{ aqi_linear(concentration, 350.5, 500.4, 401, 500) }}
    else 500
  end
{%- endmacro %}

{% macro aqi_pm10(value) -%}
  {% set concentration = "trunc((" ~ value ~ ")::numeric, 0)" %}
  case
    when {{ concentration }} is null then null
    when {{ concentration }} < 0 then null
    when {{ concentration }} <= 54.0 then {{ aqi_linear(concentration, 0.0, 54.0, 0, 50) }}
    when {{ concentration }} <= 154.0 then {{ aqi_linear(concentration, 55.0, 154.0, 51, 100) }}
    when {{ concentration }} <= 254.0 then {{ aqi_linear(concentration, 155.0, 254.0, 101, 150) }}
    when {{ concentration }} <= 354.0 then {{ aqi_linear(concentration, 255.0, 354.0, 151, 200) }}
    when {{ concentration }} <= 424.0 then {{ aqi_linear(concentration, 355.0, 424.0, 201, 300) }}
    when {{ concentration }} <= 504.0 then {{ aqi_linear(concentration, 425.0, 504.0, 301, 400) }}
    when {{ concentration }} <= 604.0 then {{ aqi_linear(concentration, 505.0, 604.0, 401, 500) }}
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
