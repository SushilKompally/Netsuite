
{# ---------------------------------------------------------
# Strings: trim + NULL if empty
# --------------------------------------------------------- #}
{% macro clean_string(col, tool_name='snowflake') -%}
nullif(trim(to_varchar({{ col }})), '')
{%- endmacro %}

{% macro clean_string_lower(col, tool_name='snowflake') -%}
lower(nullif(trim(to_varchar({{ col }})), ''))
{%- endmacro %}


{# ---------------------------------------------------------
# Numerics: safe integer / decimal
# --------------------------------------------------------- #}
{% macro safe_integer(col, tool_name='snowflake') -%}
-- Returns NUMBER(38,0) for integer-like fields (IDs, counts)
cast(try_to_number({{ col }}) as number(38,0))
{%- endmacro %}

{% macro safe_decimal(col, precision=18, scale=2, tool_name='snowflake') -%}
-- Returns DECIMAL(precision, scale); robust for strings and numeric inputs
try_to_decimal({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}

{% macro safe_float(col, tool_name='snowflake') -%}
-- Floating-point when exact precision isn’t required
try_to_double({{ col }})
{%- endmacro %}


{# ---------------------------------------------------------
# Boolean: normalize common representations (1/0, Y/N, T/F)
# --------------------------------------------------------- #}
{% macro safe_boolean(col, tool_name='snowflake') -%}
case
  when {{ col }} is null then null
  when upper(trim(to_varchar({{ col }}))) in ('TRUE','T','YES','Y','1') then true
  when upper(trim(to_varchar({{ col }}))) in ('FALSE','F','NO','N','0') then false
  else null
end
{%- endmacro %}


{# ---------------------------------------------------------
# Date: guard blanks/invalid and cutoff
# --------------------------------------------------------- #}
{% macro safe_date(col, cutoff_date="1900-01-01", tool_name='snowflake') -%}
case
  when nullif(trim(to_varchar({{ col }})), '') is null then null
  when try_to_date({{ col }}) is null then null
  when try_to_date({{ col }}) <= to_date('{{ cutoff_date }}') then null
  else try_to_date({{ col }})
end
{%- endmacro %}


{# ---------------------------------------------------------
# Timestamp (TIMESTAMP_NTZ): guard blanks/invalid
# --------------------------------------------------------- #}
{% macro safe_timestamp_ntz(col, tool_name='snowflake') -%}
case
  when nullif(trim(to_varchar({{ col }})), '') is null then null
  when try_to_timestamp_ntz({{ col }}) is null then null
  else try_to_timestamp_ntz({{ col }})
end
{%- endmacro %}
