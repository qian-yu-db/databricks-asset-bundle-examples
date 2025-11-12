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
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/03_extract_structured_data",
    "Checkpoint location",
)
dbutils.widgets.dropdown(
    name="ai_agent",
    choices=["agent_bricks", "ai_query"],
    defaultValue="ai_query",
    label="Agent name",
)
dbutils.widgets.text(
    "source_table_name", "parsed_documents_content", "Source table name"
)
dbutils.widgets.text("table_name", "parsed_documents_structured", "Output table name")
dbutils.widgets.text(
    "output_volume_path",
    "/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/outputs/",
    "Output volume path for JSONL files",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
agent_choice = dbutils.widgets.get("ai_agent")
source_table_name = dbutils.widgets.get("source_table_name")
table_name = dbutils.widgets.get("table_name")
output_volume_path = dbutils.widgets.get("output_volume_path")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("🔍 Starting Structured Data Extraction...")
print(f"📖 Source Table: {catalog}.{schema}.{source_table_name}")
print(f"📄 Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")
print(f"🤖 AI Agent: {agent_choice}")

from pyspark.sql.functions import col, concat, current_timestamp, expr, length, lit
import yaml

# Read from source table using Structured Streaming
print("🔄 Reading from source table using Structured Streaming...")
text_stream = (
    spark.readStream.format("delta")
    .table(source_table_name)
    .filter((col("content").isNotNull()) & (col("error_status").isNull()))
)

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Extract structured data using ai_query
if agent_choice == "ai_query":
    print("📋 Using ai_query for structured data extraction...")
    print(f"🤖 LLM Model: {config['LLM_MODEL']}")
    structured_df = (
        text_stream.withColumn(
            "extracted_entities",
            expr(f"""
                ai_query(
                    '{config["LLM_MODEL"]}',
                    concat(
                        '{config["PROMPT"]}',
                        content
                    ),
                    responseFormat => '{config["RESPONSE_FORMAT"]}',
                    modelParameters => named_struct(
                        'max_tokens', 5000
                    )
                )
            """),
        )
        .withColumn("extraction_timestamp", current_timestamp())
        .select("path", "extracted_entities", "parsed_at", "extraction_timestamp")
    )
elif agent_choice == "agent_bricks":
    print("🧱 Using agent_bricks for structured data extraction...")
    print(f"🤖 AGENT_BRICKS_ENDPOINT: {config['AGENT_BRICKS_ENDPOINT']}")

    structured_df = (
        text_stream.withColumn(
            "extracted_entities",
            expr(f"""
                ai_query(
                    '{config["AGENT_BRICKS_ENDPOINT"]}',
                    content,
                    failOnError => false
                )
            """),
        )
        .withColumn("extraction_timestamp", current_timestamp())
        .select("path", "extracted_entities", "parsed_at", "extraction_timestamp")
    )

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    structured_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

# Wait for the streaming query to complete
print("⏳ Waiting for streaming write to complete...")
query.awaitTermination()

print("✅ Structured data extraction completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")

# COMMAND ----------

# Export extracted_entities to JSONL files
print("📄 Starting JSONL export of extracted entities...")
print(f"📂 JSONL Output Path: {output_volume_path}")

# First, let's check if there's data in the table
table_count = spark.table(table_name).count()
print(f"🔢 Records in {table_name}: {table_count}")

if table_count > 0:
    # Use batch processing to write JSONL files (more reliable than streaming for file output)
    print("📊 Reading data from Delta table...")
    from pyspark.sql.functions import regexp_extract, to_json, struct

    df_batch = (
        spark.table(table_name)
        .select("path", "extracted_entities")
        .filter(col("extracted_entities").isNotNull())
        .withColumn(
            "file_name",
            regexp_extract(col("path"), r"([^/]+)$", 1)
        )
        .withColumn(
            "output_json",
            to_json(struct(
                col("file_name").alias("file_name"),
                col("extracted_entities").alias("extracted_entities")
            ))
        )
    )

    # Write as text files with .jsonl in the path name
    jsonl_output_path = f"{output_volume_path}/extracted_entities"
    print(f"💾 Writing JSONL to: {jsonl_output_path}")

    # Write each JSON string as a separate line (coalesce to single file)
    df_batch.select("output_json").coalesce(1).write.mode("overwrite").text(jsonl_output_path)

    print("✅ JSONL export completed successfully!")
    print(f"📄 JSONL files saved to: {jsonl_output_path}")
else:
    print("⚠️  No data found in the table to export")
