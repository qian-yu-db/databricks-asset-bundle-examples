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
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/02_extract_document_content",
    "Checkpoint location",
)
dbutils.widgets.text("source_table_name", "parsed_documents_raw", "Source table name")
dbutils.widgets.text("table_name", "parsed_documents_content", "Output table name")

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

print("📝 Starting Document Content Extraction...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import col, concat_ws, expr, lit, when

# Read from source table using Structured Streaming
print("🔄 Reading from source table using Structured Streaming...")
parsed_stream = spark.readStream.format("delta").table(source_table_name)

# Extract text from parsed documents
print("🔍 Extracting and concatenating text content from document elements...")
text_df = (
    parsed_stream.withColumn(
        "content",
        when(
            expr("try_cast(parsed:error_status AS STRING)").isNotNull(), lit(None)
        ).otherwise(
            concat_ws(
                "\n\n",
                expr("""
                transform(
                    try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                    elements -> (CASE WHEN try_cast(elements:type as STRING) = 'figure' 
                                THEN try_cast(elements:description as STRING) 
                                ELSE try_cast(elements:content AS STRING) END)
                )
                """),
            )
        ),
    )
    .withColumn("error_status", expr("try_cast(parsed:error_status AS STRING)"))
    .select("path", "content", "error_status", "parsed_at")
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    text_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Document content extraction completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
