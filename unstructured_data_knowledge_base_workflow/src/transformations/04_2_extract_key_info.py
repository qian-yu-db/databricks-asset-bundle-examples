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
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/04_2_extract_key_info",
    "Checkpoint location",
)
dbutils.widgets.dropdown(
    name="ai_agent",
    choices=["agent_bricks", "ai_query"],
    defaultValue="ai_query",
    label="Agent name",
)
dbutils.widgets.text(
    "source_table_name", "parsed_documents_content_enriched", "Source table name"
)
dbutils.widgets.text("table_name", "parsed_documents_structured", "Output table name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
agent_choice = dbutils.widgets.get("ai_agent")
source_table_name = dbutils.widgets.get("source_table_name")
table_name = dbutils.widgets.get("table_name")

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
    .filter(col("content").isNotNull())
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
                        'max_tokens', 5000,
                        'temperature', 0.0
                    )
                )
            """),
        )
        .withColumn("extraction_timestamp", current_timestamp())
        .select("path", "content", "extracted_entities", "extraction_timestamp")
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

print("✅ Structured data extraction completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
