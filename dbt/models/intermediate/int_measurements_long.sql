{%- set sources_cfg = var('measurements_sources') -%}

with long as (

  {%- for source_name, cfg in sources_cfg.items() %}

    {{ unpivot_measurements_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

)

select * from long
