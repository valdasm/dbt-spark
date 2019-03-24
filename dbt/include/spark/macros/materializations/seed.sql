{% macro spark__reset_csv_table(model, full_refresh, old_relation) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}


{% macro spark_load_csv_rows(model, batch_size) %}
    {% set agate_table = model['agate_table'] %}
    {% set cols_sql = ", ".join(agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert overwrite table {{ this.render(False) }} values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}


{% macro spark__load_csv_rows(model) %}
  {{ return(spark_load_csv_rows(model, 10000) )}}
{% endmacro %}
