# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Information from Cropped Diagrams and Enrich Document Content
# MAGIC
# MAGIC This notebook processes cropped diagram images using AI to extract pin/connector information,
# MAGIC then enriches the parsed document elements with this information to create enhanced document content.

# COMMAND ----------
%pip install openai -q
%restart_python
# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "checkpoint_location",
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/04_1_extract_info_from_cropped_diagram",
    "Checkpoint location",
)
dbutils.widgets.text(
    "source_table_name1", "parsed_documents_cropped_images", "Source table name"
)
dbutils.widgets.text(
    "source_table_name2", "parsed_documents_elements", "Source table name"
)
dbutils.widgets.text(
    "table_name", "parsed_documents_content_enriched", "Output table name"
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
source_table_name1 = dbutils.widgets.get("source_table_name1")
source_table_name2 = dbutils.widgets.get("source_table_name2")
table_name = dbutils.widgets.get("table_name")
cropped_images_path = f"/Volumes/{catalog}/{schema}/ai_parse_document_workflow/cropped_images/"

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
print("=" * 80)
print("🖼️  Starting Diagram Information Extraction and Content Enrichment Pipeline")
print("=" * 80)
print(f"\n📊 Configuration:")
print(f"  • Catalog: {catalog}")
print(f"  • Schema: {schema}")
print(f"\n📥 Input Sources:")
print(f"  • Cropped Images Metadata: {catalog}.{schema}.{source_table_name1}")
print(f"  • Parsed Elements: {catalog}.{schema}.{source_table_name2}")
print(f"  • Cropped Images Directory: {cropped_images_path}")
print(f"\n📤 Output:")
print(f"  • Enriched Content Table: {catalog}.{schema}.{table_name}")
print(f"  • Checkpoint Location: {checkpoint_location}")
print("=" * 80)

print("\n🔄 Step 1: Reading cropped diagram images from volume...")

# Import required types for binaryFile schema
from pyspark.sql.types import StructType, StructField, StringType as StrType, TimestampType, LongType, BinaryType

# Define schema for binaryFile format
binary_file_schema = StructType([
    StructField("path", StrType(), False),
    StructField("modificationTime", TimestampType(), False),
    StructField("length", LongType(), False),
    StructField("content", BinaryType(), False)
])

cropped_images_stream = (
    spark.readStream.format("binaryFile")
    .schema(binary_file_schema)
    .load(cropped_images_path)
)
print("✓ Cropped images stream initialized")

# COMMAND ----------
print("\n🤖 Step 2: Setting up AI extraction for diagram pin information...")
from pyspark.sql.functions import pandas_udf, col, trim
from pyspark.sql.types import StringType
from typing import Iterator
import base64
from openai import OpenAI
import pandas as pd
import yaml

print("  • Loading configuration from config.yaml...")
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

print("  • Configuring Databricks Foundation Model endpoint...")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
HOST = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}/serving-endpoints'

FIGURE_PROMPT = config["FIGURE_PROMPT"]
print(f"  • Using prompt: {FIGURE_PROMPT[:100]}...")
print(f"  • Model: databricks-claude-sonnet-4-5")

@pandas_udf(StringType())
def extract_pin_mapping_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Extract pin/connector information from diagram images using Claude Sonnet."""
    # Initialize client once per executor partition
    client = OpenAI(api_key=TOKEN, base_url=HOST)

    for content_series in iterator:
        results = []
        for idx, content in enumerate(content_series):
            try:
                image_data = base64.b64encode(content).decode("utf-8")
                completion = client.chat.completions.create(
                    model="databricks-claude-sonnet-4-5",
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": FIGURE_PROMPT},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}},
                        ],
                    }],
                    timeout=60.0
                )
                results.append(completion.choices[0].message.content)
            except Exception as e:
                error_msg = f"Error processing image {idx}: {type(e).__name__}: {str(e)}"
                results.append(error_msg)

        yield pd.Series(results)

print("✓ UDF defined for pin information extraction")

print("\n🔄 Step 3: Applying AI extraction to cropped images...")
cropped_images_output_stream = cropped_images_stream.withColumn("pin_information", extract_pin_mapping_udf(col("content")))
print("  • Filtering images with valid pin information...")
cropped_images_output_filtered_stream = (
    cropped_images_output_stream.filter(trim(col("pin_information")) != "NO PIN INFORMATION").drop("content")
    .withColumnRenamed("path", "pin_image_path")
)
print("✓ Filtered stream created (excluding images without pin information)")

# COMMAND ----------

print("\n🔄 Step 4: Reading source tables...")
from pyspark.sql.functions import concat, lit, when, collect_list, concat_ws

print("  • Reading cropped image metadata from Delta table...")
cropped_image_meta_data_stream = (
    spark.readStream.format("delta")
    .table(source_table_name1)
    .withColumn("cropped_figure_path", concat(lit("dbfs:"), col("cropped_figure_path")))
)
print(f"    ✓ Stream from {source_table_name1}")

print("  • Reading parsed document elements from Delta table...")
parsed_elements_stream = (
    spark.readStream.format("delta")
    .table(source_table_name2)
)
print(f"    ✓ Stream from {source_table_name2}")

print("\n  • Joining extracted pin information with image metadata...")
cropped_images_pins_joined_stream = (
    cropped_images_output_filtered_stream
    .select("pin_image_path", "pin_information")
    .join(
        cropped_image_meta_data_stream,
        cropped_images_output_filtered_stream["pin_image_path"] == cropped_image_meta_data_stream["cropped_figure_path"],
        how="inner"
    ).drop("pin_image_path")
)
print("    ✓ Pin information matched with metadata (path, element_id, page_id)")

print("\n🔄 Step 5-6: Processing joins and aggregations using foreachBatch...")
print("  • This approach processes each micro-batch as regular DataFrames")
print("  • Allows complex joins and aggregations without streaming constraints")

def process_batch(batch_df, batch_id):
    """Process each micro-batch with batch operations"""
    if batch_df.isEmpty():
        print(f"  Batch {batch_id}: No data to process")
        return

    print(f"\n  Batch {batch_id}: Processing...")

    # Read parsed elements as batch DataFrame
    parsed_elements_batch = spark.read.table(f"{catalog}.{schema}.{source_table_name2}")

    # Perform left join with parsed elements (batch operation)
    elements_enriched_batch = (
        parsed_elements_batch
        .join(
            batch_df,
            (parsed_elements_batch["path"] == batch_df["path"]) &
            (parsed_elements_batch["element_id"] == batch_df["element_id"]) &
            (parsed_elements_batch["page_id"] == batch_df["page_id"]),
            how="left"
        )
        .select(
            parsed_elements_batch["path"],
            parsed_elements_batch["element_id"],
            parsed_elements_batch["type"],
            parsed_elements_batch["content"],
            batch_df["pin_information"]
        )
    )

    # Enrich content with pin information and aggregate
    content_enriched_batch = (
        elements_enriched_batch
        .withColumn(
            "enriched_content",
            when(
                (col("type") == "figure") & col("pin_information").isNotNull(),
                concat(col("content"), lit("\n\n## Pin Information:\n"), col("pin_information"))
            ).otherwise(col("content"))
        )
        .select("path", "element_id", "enriched_content")
        .orderBy("element_id")
        .groupBy("path")
        .agg(concat_ws("\n\n", collect_list("enriched_content")).alias("content"))
    )

    # Write to Delta table
    content_enriched_batch.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.{table_name}")

    print(f"  Batch {batch_id}: ✓ Processed and saved {content_enriched_batch.count()} documents")

print("  ✓ Batch processing function defined")

# COMMAND ----------

print("\n💾 Step 7: Starting foreachBatch stream processing...")
print(f"  • Output table: {catalog}.{schema}.{table_name}")
print(f"  • Processing mode: foreachBatch (handles each micro-batch)")
print(f"  • Trigger: availableNow (batch processing)")
print(f"  • Checkpoint: {checkpoint_location}")

query = (
    cropped_images_pins_joined_stream
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .start()
)

print("  • Waiting for stream processing to complete...")
query.awaitTermination()

print("\n" + "=" * 80)
print("✅ Diagram information extraction and content enrichment completed successfully!")
print("=" * 80)
print(f"\n📊 Results saved to: {catalog}.{schema}.{table_name}")
print("\n📝 Summary:")
print("  1. ✓ Cropped diagram images read from volume (binaryFile stream)")
print("  2. ✓ AI extraction of pin/connector information (Claude Sonnet 4.5)")
print("  3. ✓ Pin information joined with image metadata (inner join)")
print("  4. ✓ Batch processing with foreachBatch for complex operations")
print("  5. ✓ Document elements enriched with pin details (left join)")
print("  6. ✓ Content aggregated by document path")
print("  7. ✓ Results written to Delta table")
print("=" * 80)