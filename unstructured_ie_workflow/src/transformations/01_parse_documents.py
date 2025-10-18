# Databricks notebook source
# MAGIC %md
# MAGIC # Parse Documents using ai_parse_document
# MAGIC
# MAGIC This notebook uses Structured Streaming to incrementally parse PDFs and images using the ai_parse_document function.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "source_volume_path",
    "/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/inputs/",
    "Source volume path",
)
dbutils.widgets.text(
    "output_volume_path",
    "/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/outputs/",
    "Output volume path",
)
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/01_parse_documents",
    "Checkpoint location",
)
dbutils.widgets.text("table_name", "parsed_documents_raw", "Output table name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_volume_path = dbutils.widgets.get("source_volume_path")
output_volume_path = dbutils.widgets.get("output_volume_path")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("🚀 Starting Document Parsing Pipeline...")
print(f"📂 Source Path: {source_volume_path}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BinaryType,
    TimestampType,
    LongType,
)

# Define schema for binary files (must match exact schema expected by binaryFile format)
binary_file_schema = StructType(
    [
        StructField("path", StringType(), False),
        StructField("modificationTime", TimestampType(), False),
        StructField("length", LongType(), False),
        StructField("content", BinaryType(), True),
    ]
)

# Read files using Structured Streaming
print("📖 Reading files with pattern: *.{pdf,jpg,jpeg,png}")
files_df = (
    spark.readStream.format("binaryFile")
    .schema(binary_file_schema)
    .option("pathGlobFilter", "*.{pdf,jpg,jpeg,png}")
    .load(source_volume_path)
)

# Parse documents with ai_parse_document
print("🔍 Parsing documents with ai_parse_document (version 2.0)...")
print(f"📁 Images will be saved to: {output_volume_path}")
parsed_df = (
    files_df.repartition(8, expr("crc32(path) % 8"))
    .withColumn(
        "parsed",
        expr(f"""
            ai_parse_document(
                content,
                map(
                    'version', '2.0',
                    'imageOutputPath', '{output_volume_path}',
                    'descriptionElementTypes', '*'
                )
            )
        """),
    )
    .withColumn("parsed_at", current_timestamp())
    .select("path", "parsed", "parsed_at")
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    parsed_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("delta.feature.variantType-preview", "supported")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Document parsing pipeline completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
