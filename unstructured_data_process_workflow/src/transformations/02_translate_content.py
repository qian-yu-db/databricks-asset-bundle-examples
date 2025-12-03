# Databricks notebook source
# MAGIC %md
# MAGIC # Translate Document Content using ai_query
# MAGIC
# MAGIC This notebook uses Structured Streaming to translate parsed document content to English using ai_query.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Reads parsed documents from the raw_parsed_files table
# MAGIC - Translates the content to English using ai_query with Llama model
# MAGIC - Preserves original formatting and spacing
# MAGIC - Writes translated content to a Delta table

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "genai_hackathon", "Schema name")
dbutils.widgets.text("table_prefix", "redaction_workflow", "Table prefix")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/genai_hackathon/checkpoints/parse_translate_classify_workflow/02_translate_content",
    "Checkpoint location",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_prefix = dbutils.widgets.get("table_prefix")
checkpoint_location = dbutils.widgets.get("checkpoint_location")

# Load configuration from YAML
import yaml
import os


# Read and parse the config file
with open('./config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

llm_model = config['TRANSLATION_MODEL']
TRANSLATE_PROMPT = config['TRANSLATE_PROMPT']

# Derived table names
source_table_name = f"{table_prefix}_raw_parsed_files"
table_name = f"{table_prefix}_translated_content"

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("🌍 Starting Content Translation Pipeline...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")
print(f"🤖 LLM Model: {llm_model}")

from pyspark.sql.functions import col, concat, current_timestamp, expr, lit

# Read from source table using Structured Streaming
print("🔄 Reading from source table using Structured Streaming...")
parsed_stream = spark.readStream.format("delta").table(source_table_name)

# Translate content using ai_query
print("🔍 Translating content to English using ai_query...")
translated_df = (
    parsed_stream.withColumn(
        "translated_content",
        expr(f"""
            ai_query(
                '{llm_model}',
                concat('{TRANSLATE_PROMPT}', cast(parsed as string))
            )
        """),
    )
    .withColumn("translation_timestamp", current_timestamp())
    .select(
        "path",
        "file_type",
        "parsed",
        "translated_content",
        "parsed_at",
        "translation_timestamp",
    )
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    translated_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Content translation completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
