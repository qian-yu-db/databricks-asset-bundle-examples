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
    "/Volumes/fins_genai/unstructured_documents/checkpoints/ai_parse_document_workflow/03_1_crop_diagrams",
    "Checkpoint location",
)
dbutils.widgets.text(
    "source_table_name1", "parsed_documents_elements", "Source table name"
)
dbutils.widgets.text(
    "source_table_name2", "parsed_documents_pages", "Source table name"
)
dbutils.widgets.text(
    "table_name", "parsed_documents_cropped_images", "Output table name"
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
source_table_name1 = dbutils.widgets.get("source_table_name1")
source_table_name2 = dbutils.widgets.get("source_table_name2")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

print("🖼️  Starting Diagram Cropping Pipeline...")
print(f"📖 Elements Table: {catalog}.{schema}.{source_table_name1}")
print(f"📄 Pages Table: {catalog}.{schema}.{source_table_name2}")
print(f"🖼️  Output Table: {catalog}.{schema}.{table_name}")
print(f"🗂️  Checkpoint: {checkpoint_location}")

from pyspark.sql.functions import col, concat, current_timestamp, expr, length, lit
import yaml

# Read from source table using Structured Streaming
print("🔄 Reading elements and pages streams...")
elements_stream = (
    spark.readStream.format("delta")
    .table(source_table_name1)
    .filter((col("description").isNotNull()))
)

pages_stream = (
    spark.readStream.format("delta")
    .table(source_table_name2)
    .filter((col("image_uri").isNotNull()))
)

# Extract structured data using ai_query
print("🔍 Loading configuration and selecting diagrams...")
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

print(f"🤖 LLM Model: {config['LLM_MODEL']}")
print("🎯 Identifying diagrams worth cropping using AI classification...")
selected_diagrams_df = (
    elements_stream.select(
        "path",
        "parsed_at",
        "element_id",
        "type",
        "bbox",
        "page_id",
        "content",
        "description",
    )
    .alias("e")
    .join(
        pages_stream.alias("p"),
        (col("e.page_id") == col("p.page_id")) & (col("e.path") == col("p.path")),
        "inner",
    )
    .withColumn(
        "selected_diagrams",
        expr(f"""
            ai_query(
                '{config["LLM_MODEL"]}',
                concat(
                    '{config["FIGURE_CLASSIFICATION_PROMPT"]}',
                    e.description
                )
            )
        """),
    )
    .filter(col("selected_diagrams").isin(["package_circuits_diagram", "circuit_schematic"]))
    .select(
        col("e.path"),
        col("e.parsed_at"),
        col("e.element_id"),
        col("e.type"),
        col("e.bbox"),
        col("e.page_id"),
        col("e.content"),
        col("e.description"),
        col("selected_diagrams"),
        col("p.image_uri"),
    )
)

# COMMAND ----------
print("✂️  Setting up image cropping functionality...")
# Crop selected diagrams using Pandas UDF
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd
from PIL import Image
import os


@pandas_udf(returnType=StringType())
def crop_figure_udf(
    paths: pd.Series, element_ids: pd.Series, bboxes: pd.Series, image_uris: pd.Series
) -> pd.Series:
    def crop_single_figure(path, element_id, bbox, image_uri):
        try:
            file_name = os.path.basename(path).split(".")[0]

            # Replace 'images' folder with 'cropped_images' in the path
            cropped_image_path = image_uri.replace("/outputs/", "/cropped_images/")
            cropped_image_path = (
                "/".join(cropped_image_path.split("/")[:-1])
                + f"/{file_name}_{element_id}.jpg"
            )

            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(cropped_image_path), exist_ok=True)

            # Open and crop image
            image = Image.open(image_uri)
            cropped_image = image.crop(bbox)
            cropped_image.save(cropped_image_path)

            return cropped_image_path
        except Exception as e:
            return f"Error: {str(e)}"

    # Apply the function to each row using vectorized operations
    results = []
    for i in range(len(paths)):
        result = crop_single_figure(
            paths.iloc[i], element_ids.iloc[i], bboxes.iloc[i], image_uris.iloc[i]
        )
        results.append(result)

    return pd.Series(results)


print("🖼️  Applying cropping UDF to selected diagrams...")
cropped_images_df = (
    selected_diagrams_df.withColumn(
        "cropped_figure_path",
        crop_figure_udf(col("path"), col("element_id"), col("bbox"), col("image_uri")),
    )
    .withColumn("cropping_timestamp", current_timestamp())
    .select(
        "path",
        "parsed_at",
        "element_id",
        "type",
        "bbox",
        "page_id",
        "content",
        "description",
        "selected_diagrams",
        "cropped_figure_path",
        "cropping_timestamp",
    )
)

# Write to Delta table with streaming
print("💾 Starting streaming write to Delta table...")
print("⚡ Processing mode: availableNow (batch processing)")
query = (
    cropped_images_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("✅ Diagram cropping pipeline completed successfully!")
print(f"📊 Results saved to: {catalog}.{schema}.{table_name}")
