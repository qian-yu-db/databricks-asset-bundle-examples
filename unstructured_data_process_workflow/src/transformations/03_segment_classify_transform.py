# Databricks notebook source
# MAGIC %md
# MAGIC # Segment, Classify and Transform Documents
# MAGIC
# MAGIC This notebook uses Structured Streaming to:
# MAGIC 1. Segment and classify documents (CVs/credentials) using ai_query with Claude
# MAGIC 2. Parse the JSON response to extract structured data
# MAGIC 3. Transform the data by exploding multiple items into individual records
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Reads translated content from the translated_content table
# MAGIC - Uses ai_query with Claude to identify and segment CVs/credentials
# MAGIC - Parses the JSON response to extract structured data
# MAGIC - Creates two tables:
# MAGIC   - parsed_content: Contains the segmented analysis
# MAGIC   - parsed_records: Individual records for each CV/credential

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "genai_hackathon", "Schema name")
dbutils.widgets.text("table_prefix", "redaction_workflow", "Table prefix")
dbutils.widgets.text(
    "checkpoint_location_segment",
    "/Volumes/fins_genai/genai_hackathon/checkpoints/parse_translate_classify_workflow/03_segment_classify",
    "Checkpoint location for segmentation",
)
dbutils.widgets.text(
    "checkpoint_location_transform",
    "/Volumes/fins_genai/genai_hackathon/checkpoints/parse_translate_classify_workflow/04_transform_records",
    "Checkpoint location for transformation",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_prefix = dbutils.widgets.get("table_prefix")
checkpoint_location_segment = dbutils.widgets.get("checkpoint_location_segment")
checkpoint_location_transform = dbutils.widgets.get("checkpoint_location_transform")

# Load configuration from YAML
import yaml
import os

# Read and parse the config file
with open('./config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

llm_model = config['CLASSIFICATION_MODEL']
SEGMENT_PROMPT = config['SEGMENT_PROMPT']

# Derived table names
source_table_name = f"{table_prefix}_translated_content"
parsed_content_table = f"{table_prefix}_parsed_content"
parsed_records_table = f"{table_prefix}_parsed_records"

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Segment and Classify Documents

# COMMAND ----------

print("🔍 Starting Document Segmentation and Classification Pipeline...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{parsed_content_table}")
print(f"🗂️  Checkpoint: {checkpoint_location_segment}")
print(f"🤖 LLM Model: {llm_model}")

from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    expr,
    from_json,
    regexp_replace,
)

# Read from source table using Structured Streaming
print("🔄 Reading from source table using Structured Streaming...")
translated_stream = (
    spark.readStream.format("delta")
    .table(source_table_name)
    .filter(col("translated_content").isNotNull())
)

# Segment and classify using ai_query
print("🔍 Segmenting and classifying documents using ai_query...")
segmented_df = (
    translated_stream.withColumn(
        "split_analysis",
        from_json(
            regexp_replace(
                expr(f"""
                    ai_query(
                        '{llm_model}',
                        concat('{SEGMENT_PROMPT}', translated_content),
                        modelParameters => named_struct('max_tokens', 8000)
                    )
                """),
                "```json|```",
                "",
            ),
            "ARRAY<STRUCT<item_number: INT, type: STRING, person_name: STRING, email: STRING, title: STRING, text: STRING>>",
        ),
    )
    .withColumn("segmentation_timestamp", current_timestamp())
    .select(
        "path",
        "file_type",
        "parsed",
        "translated_content",
        "split_analysis",
        "parsed_at",
        "translation_timestamp",
        "segmentation_timestamp",
    )
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query_segment = (
    segmented_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location_segment)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(parsed_content_table)
)

# Wait for the streaming query to complete
print("⏳ Waiting for streaming write to complete...")
query_segment.awaitTermination()

print("✅ Document segmentation and classification completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{parsed_content_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform by Exploding Multiple Items into Individual Records

# COMMAND ----------

print("\n🔄 Starting Transformation Pipeline...")
print(f"📖 Source Table: {catalog}.{schema}.{parsed_content_table}")
print(f"📄 Output Table: {catalog}.{schema}.{parsed_records_table}")
print(f"🗂️  Checkpoint: {checkpoint_location_transform}")

from pyspark.sql.functions import explode, when

# Read from parsed_content table using Structured Streaming
print("🔄 Reading from parsed_content table using Structured Streaming...")
parsed_content_stream = (
    spark.readStream.format("delta")
    .table(parsed_content_table)
    .filter(col("split_analysis").isNotNull())
)

# Explode the split_analysis array to create individual records
print("🔍 Exploding split_analysis array to create individual records...")
exploded_df = (
    parsed_content_stream.withColumn("exploded_item", explode(col("split_analysis")))
    .select(
        col("path"),
        col("file_type"),
        col("parsed"),
        col("translated_content"),
        col("exploded_item.item_number").alias("item_id"),
        col("exploded_item.type").alias("document_type"),
        col("exploded_item.person_name").alias("person_name"),
        when(col("exploded_item.email") == "", None)
        .otherwise(col("exploded_item.email"))
        .alias("email"),
        col("exploded_item.title").alias("sub_title"),
        col("exploded_item.text").alias("text"),
        col("parsed_at"),
        col("translation_timestamp"),
        col("segmentation_timestamp"),
    )
    .withColumn("record_timestamp", current_timestamp())
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query_transform = (
    exploded_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location_transform)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(parsed_records_table)
)

# Wait for the streaming query to complete
print("⏳ Waiting for streaming write to complete...")
query_transform.awaitTermination()

print("✅ Transformation completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{parsed_records_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook has successfully:
# MAGIC 1. ✅ Segmented and classified documents into CVs/credentials
# MAGIC 2. ✅ Parsed the JSON response to extract structured data
# MAGIC 3. ✅ Transformed the data into individual records
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `{parsed_content_table}`: Contains the segmented analysis for each document
# MAGIC - `{parsed_records_table}`: Contains individual records for each CV/credential found
