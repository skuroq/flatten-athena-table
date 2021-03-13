create TABLE "{{tmp_table}}"
            with (
                external_location = '{{location}}',
                format = 'PARQUET',
                parquet_compression = 'SNAPPY'             
            )
            AS
            select
{% for source_column, target_column in columns %}
"{{source_column}}" as "{{target_column}}"{% if not loop.last -%},{% endif %}{% endfor %}
from "{{source_tb_name}}"
