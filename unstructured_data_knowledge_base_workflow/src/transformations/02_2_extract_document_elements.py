# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Text from Parsed Documents
# MAGIC
# MAGIC This notebook uses Structured Streaming to extract clean text from parsed documents.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/02_2_extract_document_elements",
    "Checkpoint location",
)
dbutils.widgets.text("source_table_name", "parsed_documents_raw", "Source table name")
dbutils.widgets.text("table_name", "parsed_documents_elements", "Output table name")

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

print("🧩 Starting Document Elements Extraction...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import col, concat_ws, expr, lit, when

# Read from source table using Structured Streaming
print("🔄 Reading from source table using Structured Streaming...")
parsed_stream = spark.readStream.format("delta").table(source_table_name)

# Extract document elements from parsed documents
print("🔍 Extracting individual document elements with metadata...")
print("📋 Extracting: element_id, type, bbox, page_id, content, description")
elements_df = (
    parsed_stream.filter(
        expr(
            "parsed:document:elements IS NOT NULL AND CAST(parsed:error_status AS STRING) IS NULL"
        )
    )
    .select(
        col("path"),
        col("parsed_at"),
        expr("posexplode(try_cast(parsed:document:elements AS ARRAY<VARIANT>))").alias(
            "idx", "items"
        ),
    )
    .select(
        col("path"),
        col("parsed_at"),
        expr("cast(items:id as int)").alias("element_id"),
        expr("cast(items:type as string)").alias("type"),
        expr("cast(items:bbox[0]:coord as ARRAY<INT>)").alias("bbox"),
        expr("cast(items:bbox[0]:page_id as int)").alias("page_id"),
        when(
            expr("cast(items:type as string)") == "figure",
            expr("cast(items:description as string)"),
        )
        .otherwise(expr("cast(items:content as string)"))
        .alias("content"),
        expr("cast(items:description as string)").alias("description"),
    )
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    elements_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Document elements extraction completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
