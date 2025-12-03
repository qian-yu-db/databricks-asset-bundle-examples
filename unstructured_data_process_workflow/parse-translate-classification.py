# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion, Translation, Segmentation, Classification
# MAGIC
# MAGIC - `ai_parse_document` for parsing
# MAGIC - `ai_transate` or `ai_query` for translation
# MAGIC - `ai_query` for classification and segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Env

# COMMAND ----------

dbutils.widgets.text("catalog", "fins_genai")
dbutils.widgets.text("schema", "genai_hackathon")
dbutils.widgets.text("volume", "docs_for_redaction")
dbutils.widgets.text("table_prefix", "redaction_workflow")
dbutils.widgets.dropdown("reset_data", "true", ["true", "false"])

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
table_prefix = dbutils.widgets.get("table_prefix")
reset_data = dbutils.widgets.get("reset_data") == "true"

print(f"Use Unit Catalog: {catalog}")
print(f"Use Schema: {schema}")
print(f"Use Volume: {volume}")
print(f"Use Table Prefix: {table_prefix}")
print(f"Reset Data: {reset_data}")

# COMMAND ----------

pipeline_config = {
    "source_path": f"/Volumes/{catalog}/{schema}/{volume}/original",
    "raw_parsed_files_table_name": f"{catalog}.{schema}.{table_prefix}_raw_parsed_files",
    "parsed_records_table_name": f"{catalog}.{schema}.{table_prefix}_parsed_records",
    "parsed_content_table_name": f"{catalog}.{schema}.{table_prefix}_parsed_content",
    "image_path": f"/Volumes/{catalog}/{schema}/{volume}/images"
}
print(pipeline_config)

# COMMAND ----------

if reset_data:
    print("Delete tables ...")
    spark.sql(f"DROP TABLE IF EXISTS {pipeline_config['raw_parsed_files_table_name']}")
    spark.sql(f"DROP TABLE IF EXISTS {pipeline_config['parsed_content_table_name']}")
    spark.sql(f"DROP TABLE IF EXISTS {pipeline_config['parsed_records_table_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsed Document in Variant Data Type

# COMMAND ----------

partitionCount = '5' # to adjust based on file volume

bronze_sql_script = f"""
CREATE OR REPLACE TABLE {pipeline_config['raw_parsed_files_table_name']} AS
WITH files as (
  SELECT
    path,
    content
  FROM
    READ_FILES('{pipeline_config['source_path']}/*.{{pdf,pptx,docx}}', format => 'binaryFile')
  ORDER BY path ASC
),
repartitioned_files AS (
  SELECT *
  FROM files
  -- Force Spark to split into partitions
  DISTRIBUTE BY crc32(path) % INT({partitionCount})
)
SELECT
    path,
    regexp_extract(path, '\\.([a-zA-Z0-9]+)$', 1) as file_type,
    ai_parse_document(
        content, 
        map('version', '2.0', 
            'imageOutputPath', '{pipeline_config['image_path']}',
            'descriptionElementTypes', '*')
    ) as parsed
FROM repartitioned_files
"""

spark.sql(bronze_sql_script)
display(spark.table(pipeline_config['raw_parsed_files_table_name']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Translate, Segment, Classification with ai_query on raw parsed column

# COMMAND ----------

TRANSLATE_PROMPT = """
translate to english. Retain the original formatting, only translate. 
Do not add any extra words before or after the original content - e.g. do not summarize what you are doing. and do not remove spacing/white spaces unneccessarily
"""

SEGMENT_PROMPT = """
Analyze this document and identify if it contains multiple CVs (resumes/curricula vitae) or customer success stories/credentials. Use the bounding boxes to help as this info often comes from slides, with CVs or creds grouped together in multiple text boxes.

For each distinct CV or credential found, return a JSON array with the following structure:
[
    {
        "item_number": 1, 
        "type": "CV" or "credential", 
        "person_name": "name if CV", 
        "email": "email if CV",
        "title": "brief title", 
        "text": "all of the associated text for this entity"
    }
]

- If the document contains only one CV/credential, return an array with one item.
- If no CVs/credentials are found, return an empty array [].

Document content: 
"""

# COMMAND ----------

analysis_query = f"""
CREATE OR REPLACE TABLE {pipeline_config['parsed_content_table_name']} AS
with translated as
(
  SELECT 
    path,
    file_type,
    parsed,
    ai_query(
      'databricks-meta-llama-3-3-70b-instruct',
      '{TRANSLATE_PROMPT}' || parsed
    ) as translated_content
  FROM {pipeline_config['raw_parsed_files_table_name']}
)
select
  path,
  file_type,
  parsed,
  translated_content,
    from_json(
      regexp_replace(
          ai_query(
            'databricks-claude-sonnet-4-5',
            CONCAT(
              '{SEGMENT_PROMPT}', translated_content
            ),
            modelParameters => named_struct('max_tokens', 8000)
          ), '```json|```', ''),
          'ARRAY<STRUCT<item_number: INT, type: STRING, person_name: STRING, email: STRING, title: STRING, text: STRING>>'
  ) as split_analysis
from translated
"""

spark.sql(analysis_query)
display(spark.table(pipeline_config['parsed_content_table_name']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformer by split multiple CV and Case study into multiple rows

# COMMAND ----------

transform_query = f"""
CREATE OR REPLACE TABLE {pipeline_config['parsed_records_table_name']} AS
select
  path,
  file_type,
  parsed,
  translated_content,
  exploded_item.item_number as item_id,
  exploded_item.type as document_type,
  exploded_item.person_name as person_name,
  CASE WHEN exploded_item.email = '' THEN NULL ELSE exploded_item.email END as email,
  exploded_item.title as sub_title,
  exploded_item.text as text
FROM {pipeline_config['parsed_content_table_name']}
LATERAL VIEW explode(split_analysis) AS exploded_item
"""

spark.sql(transform_query)
display(spark.table(pipeline_config['parsed_records_table_name']))