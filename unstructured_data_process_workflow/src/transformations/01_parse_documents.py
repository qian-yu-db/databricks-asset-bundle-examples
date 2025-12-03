# Databricks notebook source
# MAGIC %md
# MAGIC # Parse Documents using ai_parse_document
# MAGIC
# MAGIC This notebook uses Structured Streaming to incrementally parse PDFs, PPTX, and DOCX files using the ai_parse_document function.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Reads binary files (PDF, PPTX, DOCX) from the source volume using structured streaming
# MAGIC - Parses documents using ai_parse_document with version 2.0
# MAGIC - Extracts images and saves descriptions for all element types
# MAGIC - Writes parsed results to a Delta table in VARIANT format

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "genai_hackathon", "Schema name")
dbutils.widgets.text("volume", "docs_for_redaction", "Volume name")
dbutils.widgets.text("table_prefix", "redaction_workflow", "Table prefix")
dbutils.widgets.text(
    "partition_count", "5", "Number of partitions for processing"
)
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/genai_hackathon/checkpoints/parse_translate_classify_workflow/01_parse_documents",
    "Checkpoint location",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
table_prefix = dbutils.widgets.get("table_prefix")
partition_count = int(dbutils.widgets.get("partition_count"))
checkpoint_location = dbutils.widgets.get("checkpoint_location")

# Derived paths
source_path = f"/Volumes/{catalog}/{schema}/{volume}/original"
image_path = f"/Volumes/{catalog}/{schema}/{volume}/images"
table_name = f"{table_prefix}_raw_parsed_files"

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("🚀 Starting Document Parsing Pipeline...")
print(f"📂 Source Path: {source_path}")
print(f"🖼️  Image Path: {image_path}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")
print(f"⚙️  Partition Count: {partition_count}")

from pyspark.sql.functions import col, current_timestamp, expr, regexp_extract
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
print("📖 Reading files with pattern: *.{pdf,pptx,docx}")
files_df = (
    spark.readStream.format("binaryFile")
    .schema(binary_file_schema)
    .option("pathGlobFilter", "*.{pdf,pptx,docx}")
    .load(source_path)
)

# Parse documents with ai_parse_document
print("🔍 Parsing documents with ai_parse_document (version 2.0)...")
print(f"📁 Images will be saved to: {image_path}")
parsed_df = (
    files_df
    .repartition(partition_count, expr(f"crc32(path) % {partition_count}"))
    .withColumn("file_type", regexp_extract(col("path"), r'\.([a-zA-Z0-9]+)$', 1))
    .withColumn(
        "parsed",
        expr(f"""
            ai_parse_document(
                content,
                map(
                    'version', '2.0',
                    'imageOutputPath', '{image_path}',
                    'descriptionElementTypes', '*'
                )
            )
        """),
    )
    .withColumn("parsed_at", current_timestamp())
    .select("path", "file_type", "parsed", "parsed_at")
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
