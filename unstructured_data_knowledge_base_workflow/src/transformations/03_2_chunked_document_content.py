# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Structured Data using AI Query
# MAGIC
# MAGIC This notebook uses Structured Streaming to extract structured JSON from document text using ai_query.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/03_2_chunked_document_content",
    "Checkpoint location",
)
dbutils.widgets.text(
    "source_table_name", "parsed_documents_elements", "Source table name"
)
dbutils.widgets.text(
    "table_name", "parsed_documents_chunked_content", "Output table name"
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
source_table_name = dbutils.widgets.get("source_table_name")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("📄 Starting Chunked Document Content Pipeline...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import (
    col,
    concat_ws,
    expr,
    collect_list,
)

# Read from source table using Structured Streaming  
print("🔄 Reading from elements table using Structured Streaming...")
elements_stream = (
    spark.readStream.format("delta")
    .table(source_table_name)
    .filter((col("content").isNotNull()))
)

# Aggregate content by path and page_id
print("🔧 Setting up content aggregation by path and page_id...")
print("📊 Collecting and concatenating all content for each page...")
chunked_content_df = (
    elements_stream.groupBy("path", "page_id")
    .agg(
        concat_ws("\n\n", collect_list("content")).alias("chunked_content"),
        expr("max(parsed_at)").alias("latest_parsed_at"),
    )
    .select(
        "path",
        col("page_id").cast("string").alias("page_id"),
        "chunked_content",
        "latest_parsed_at",
    )
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: complete (aggregated content per page)")
print("⚡ Trigger: availableNow (batch processing)")
query = (
    chunked_content_df.writeStream.format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Chunked document content pipeline completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
