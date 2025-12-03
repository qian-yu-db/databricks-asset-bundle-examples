# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Pipeline Tables and Checkpoints
# MAGIC
# MAGIC This notebook provides a utility to clean up all tables and checkpoints created by the document processing pipeline.
# MAGIC
# MAGIC ## What Gets Cleaned
# MAGIC
# MAGIC ### Tables (8 total):
# MAGIC 1. **parsed_documents_raw** - Raw parsed documents from ai_parse_document
# MAGIC 2. **parsed_documents_content** - Extracted text content
# MAGIC 3. **parsed_documents_elements** - Extracted document elements with metadata
# MAGIC 4. **parsed_documents_pages** - Extracted page information
# MAGIC 5. **parsed_documents_cropped_images** - Cropped diagram images metadata
# MAGIC 6. **parsed_documents_chunked_content** - Chunked content for embeddings
# MAGIC 7. **parsed_documents_content_enriched** - Content enriched with diagram pin information
# MAGIC 8. **parsed_documents_structured** - Structured data extracted using AI
# MAGIC
# MAGIC ### Checkpoint Directories (8 total):
# MAGIC - 01_parse_documents
# MAGIC - 02_1_extract_document_content
# MAGIC - 02_2_extract_document_elements
# MAGIC - 02_3_extract_document_pages
# MAGIC - 03_1_crop_diagrams
# MAGIC - 03_2_chunked_document_content
# MAGIC - 04_1_extract_info_from_cropped_diagrams
# MAGIC - 04_2_extract_key_info
# MAGIC
# MAGIC ### Output Directories:
# MAGIC - Parsed document outputs
# MAGIC - Cropped diagram images
# MAGIC
# MAGIC ## Usage
# MAGIC Set the widget **"Clean all pipeline tables and checkpoints?"** to **"Yes"** to execute cleanup.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "unstructured_documents", "Schema name")
dbutils.widgets.text(
    "output_volume_path",
    "/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/outputs/",
    "Output volume path",
)
dbutils.widgets.dropdown(
    name="clean_pipeline_tables",
    choices=["No", "Yes"],
    defaultValue="No",
    label="Clean all pipeline tables and checkpoints?",
)
dbutils.widgets.text("raw_table_name", "parsed_documents_raw", "Raw table name")
dbutils.widgets.text(
    "content_table_name", "parsed_documents_content", "Content table name"
)
dbutils.widgets.text(
    "elements_table_name", "parsed_documents_elements", "Elements table name"
)
dbutils.widgets.text("pages_table_name", "parsed_documents_pages", "Pages table name")
dbutils.widgets.text(
    "cropped_images_table_name",
    "parsed_documents_cropped_images",
    "Cropped images table name",
)
dbutils.widgets.text(
    "chunked_content_table_name",
    "parsed_documents_chunked_content",
    "Chunked content table name",
)
dbutils.widgets.text(
    "structured_table_name", "parsed_documents_structured", "Structured table name"
)
dbutils.widgets.text(
    "content_enriched_table_name",
    "parsed_documents_content_enriched",
    "Content enriched table name",
)
dbutils.widgets.text(
    "checkpoint_base_path",
    "checkpoints/ai_parse_document_workflow",
    "Checkpoint base path",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
output_volume_path = dbutils.widgets.get("output_volume_path")
cropped_images_volume_path = (
    "/".join(output_volume_path.split("/")[:-1]) + "/cropped_images/"
)
clean_pipeline = dbutils.widgets.get("clean_pipeline_tables")
raw_table_name = dbutils.widgets.get("raw_table_name")
content_table_name = dbutils.widgets.get("content_table_name")
elements_table_name = dbutils.widgets.get("elements_table_name")
pages_table_name = dbutils.widgets.get("pages_table_name")
cropped_images_table_name = dbutils.widgets.get("cropped_images_table_name")
chunked_content_table_name = dbutils.widgets.get("chunked_content_table_name")
structured_table_name = dbutils.widgets.get("structured_table_name")
content_enriched_table_name = dbutils.widgets.get("content_enriched_table_name")
checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path")

# COMMAND ----------

# Set catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import shutil
import os


def ensure_volume_exists(volume_name):
    """Ensure a Unity Catalog volume exists, create if it doesn't"""
    try:
        # Check if volume exists by trying to describe it
        spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{volume_name}")
        print(f"  ℹ️  Volume already exists: {catalog}.{schema}.{volume_name}")
        return True
    except Exception:
        # Volume doesn't exist, try to create it
        try:
            spark.sql(
                f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}"
            )
            print(f"  ✅ Created volume: {catalog}.{schema}.{volume_name}")
            return True
        except Exception as e:
            print(
                f"  ❌ Failed to create volume {catalog}.{schema}.{volume_name}: {str(e)}"
            )
            return False


def ensure_directories_exist():
    """Ensure required volumes and directories exist, create if they don't"""

    print("📁 Checking and creating required volumes and directories...")

    # Extract volume names from paths
    # e.g., "/Volumes/fins_genai/unstructured_documents/ai_parse_document_workflow/outputs/"
    #       -> "ai_parse_document_workflow"
    # e.g., "checkpoints/ai_parse_document_workflow" -> "checkpoints"
    volumes_to_check = set()

    # Extract volume from output_volume_path
    if output_volume_path.startswith("/Volumes/"):
        path_parts = output_volume_path.split("/")
        if len(path_parts) >= 5:
            output_volume_name = path_parts[4]  # /Volumes/catalog/schema/volume/...
            volumes_to_check.add(output_volume_name)

    # Extract checkpoint volume name from checkpoint_base_path
    checkpoint_volume = checkpoint_base_path.split("/")[0]
    volumes_to_check.add(checkpoint_volume)

    # Ensure all required volumes exist
    print("\n🗄️  Checking Unity Catalog volumes...")
    for vol in volumes_to_check:
        ensure_volume_exists(vol)

    # Define all required directories
    print("\n📂 Creating subdirectories...")
    required_dirs = [
        output_volume_path,
        cropped_images_volume_path,
        f"/Volumes/{catalog}/{schema}/{checkpoint_base_path}",
    ]

    for directory in required_dirs:
        try:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                print(f"  ✅ Created directory: {directory}")
            else:
                print(f"  ℹ️  Directory already exists: {directory}")
        except Exception as e:
            print(f"  ❌ Failed to create directory {directory}: {str(e)}")

    print("\n📁 Volume and directory check completed!\n")


def cleanup_pipeline():
    """Clean up all pipeline tables and checkpoints"""

    print("🧹 Starting pipeline cleanup...")

    # Define all tables created by the pipeline (in order of creation)
    tables_to_drop = [
        raw_table_name,                    # 01_parse_documents
        content_table_name,                # 02_1_extract_document_content
        elements_table_name,               # 02_2_extract_document_elements
        pages_table_name,                  # 02_3_extract_document_pages
        cropped_images_table_name,         # 03_1_crop_diagrams
        chunked_content_table_name,        # 03_2_chunked_document_content
        content_enriched_table_name,       # 04_1_extract_info_from_cropped_diagrams
        structured_table_name,             # 04_2_extract_key_info
    ]

    # Define all checkpoint locations (matching job configuration)
    checkpoint_paths = [
        f"{checkpoint_base_path}/01_parse_documents",
        f"{checkpoint_base_path}/02_1_extract_document_content",
        f"{checkpoint_base_path}/02_2_extract_document_elements",
        f"{checkpoint_base_path}/02_3_extract_document_pages",
        f"{checkpoint_base_path}/03_1_crop_diagrams",
        f"{checkpoint_base_path}/03_2_chunked_document_content",
        f"{checkpoint_base_path}/04_1_extract_info_from_cropped_diagrams",
        f"{checkpoint_base_path}/04_2_extract_key_info",
    ]

    # Drop tables
    print("📋 Dropping pipeline tables...")
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")
            print(f"  ✅ Dropped table: {table}")
        except Exception as e:
            print(f"  ❌ Failed to drop table {table}: {str(e)}")

    # Clean checkpoint directories
    print("\n🗂️  Cleaning checkpoint directories...")
    for checkpoint_path in checkpoint_paths:
        try:
            # Check if path exists before attempting to remove
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                print(f"  ✅ Removed checkpoint: {checkpoint_path}")
            else:
                print(f"  ℹ️  Checkpoint directory not found: {checkpoint_path}")
        except Exception as e:
            print(f"  ❌ Failed to remove checkpoint {checkpoint_path}: {str(e)}")

    # Clean any additional output directories (optional)
    output_paths = [
        f"{output_volume_path}",
        f"{cropped_images_volume_path}",
    ]

    print("\n📁 Cleaning output directories...")
    for output_path in output_paths:
        try:
            if os.path.exists(output_path):
                # Only remove contents, not the directory itself
                for item in os.listdir(output_path):
                    item_path = os.path.join(output_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                print(f"  ✅ Cleaned output directory: {output_path}")
            else:
                print(f"  ℹ️  Output directory not found: {output_path}")
        except Exception as e:
            print(f"  ❌ Failed to clean output directory {output_path}: {str(e)}")

    print("\n🎉 Pipeline cleanup completed!")


# COMMAND ----------

# Ensure directories exist if requested
ensure_directories_exist()

# Clean pipeline if requested
if clean_pipeline == "Yes":
    print("⚠️  CLEANING PIPELINE - This will remove all tables and checkpoints!")
    cleanup_pipeline()
else:
    print(
        "ℹ️  Cleanup skipped. Set 'Clean all pipeline tables and checkpoints?' to 'Yes' to proceed."
    )
    print("\n📋 Tables that would be cleaned:")
    print(f"  1. {raw_table_name} (parsed documents)")
    print(f"  2. {content_table_name} (extracted content)")
    print(f"  3. {elements_table_name} (extracted elements)")
    print(f"  4. {pages_table_name} (extracted pages)")
    print(f"  5. {cropped_images_table_name} (cropped diagrams)")
    print(f"  6. {chunked_content_table_name} (chunked content)")
    print(f"  7. {content_enriched_table_name} (content enriched with diagram info)")
    print(f"  8. {structured_table_name} (structured data)")
    print("\n🗂️  Checkpoint directories that would be cleaned:")
    print(f"  - {checkpoint_base_path}/01_parse_documents")
    print(f"  - {checkpoint_base_path}/02_1_extract_document_content")
    print(f"  - {checkpoint_base_path}/02_2_extract_document_elements")
    print(f"  - {checkpoint_base_path}/02_3_extract_document_pages")
    print(f"  - {checkpoint_base_path}/03_1_crop_diagrams")
    print(f"  - {checkpoint_base_path}/03_2_chunked_document_content")
    print(f"  - {checkpoint_base_path}/04_1_extract_info_from_cropped_diagrams")
    print(f"  - {checkpoint_base_path}/04_2_extract_key_info")
    print("\n📁 Output directories that would be cleaned:")
    print(f"  - {output_volume_path}")
    print(f"  - {cropped_images_volume_path}")
