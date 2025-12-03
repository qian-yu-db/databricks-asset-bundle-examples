# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Pipeline Tables and Checkpoints
# MAGIC
# MAGIC This notebook provides a utility to clean up all tables and checkpoints created by the parse-translate-classification pipeline.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "fins_genai", "Catalog name")
dbutils.widgets.text("schema", "genai_hackathon", "Schema name")
dbutils.widgets.text("volume", "docs_for_redaction", "Volume name")
dbutils.widgets.text("table_prefix", "redaction_workflow", "Table prefix")
dbutils.widgets.dropdown(
    name="clean_pipeline_tables",
    choices=["No", "Yes"],
    defaultValue="No",
    label="Clean all pipeline tables and checkpoints?",
)
dbutils.widgets.text(
    "checkpoint_base_path",
    "checkpoints/parse_translate_classify_workflow",
    "Checkpoint base path",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
table_prefix = dbutils.widgets.get("table_prefix")
clean_pipeline = dbutils.widgets.get("clean_pipeline_tables")
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

    # Extract checkpoint volume name from checkpoint_base_path
    # e.g., "checkpoints/parse_translate_classify_workflow" -> "checkpoints"
    checkpoint_volume = checkpoint_base_path.split("/")[0]

    # Ensure all required volumes exist
    print("\n🗄️  Checking Unity Catalog volumes...")
    volumes_to_check = [volume, checkpoint_volume]

    for vol in set(volumes_to_check):  # Use set to avoid duplicates
        ensure_volume_exists(vol)

    # Define all required directories
    print("\n📂 Creating subdirectories...")
    required_dirs = [
        f"/Volumes/{catalog}/{schema}/{volume}/original",
        f"/Volumes/{catalog}/{schema}/{volume}/images",
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

    # Define all tables created by the pipeline
    tables_to_drop = [
        f"{table_prefix}_raw_parsed_files",
        f"{table_prefix}_translated_content",
        f"{table_prefix}_parsed_content",
        f"{table_prefix}_parsed_records",
    ]

    # Define all checkpoint locations
    checkpoint_paths = [
        f"/Volumes/{catalog}/{schema}/{checkpoint_base_path}/01_parse_documents",
        f"/Volumes/{catalog}/{schema}/{checkpoint_base_path}/02_translate_content",
        f"/Volumes/{catalog}/{schema}/{checkpoint_base_path}/03_segment_classify",
        f"/Volumes/{catalog}/{schema}/{checkpoint_base_path}/04_transform_records",
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

    # Clean image directory
    image_path = f"/Volumes/{catalog}/{schema}/{volume}/images"
    print(f"\n📁 Cleaning image directory: {image_path}")
    try:
        if os.path.exists(image_path):
            # Only remove contents, not the directory itself
            for item in os.listdir(image_path):
                item_path = os.path.join(image_path, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            print(f"  ✅ Cleaned image directory: {image_path}")
        else:
            print(f"  ℹ️  Image directory not found: {image_path}")
    except Exception as e:
        print(f"  ❌ Failed to clean image directory {image_path}: {str(e)}")

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
    print(f"  - {table_prefix}_raw_parsed_files")
    print(f"  - {table_prefix}_translated_content")
    print(f"  - {table_prefix}_parsed_content")
    print(f"  - {table_prefix}_parsed_records")
    print("\n🗂️  Checkpoint directories that would be cleaned:")
    print(f"  - /Volumes/{catalog}/{schema}/{checkpoint_base_path}/*")
    print("\n📁 Image directory that would be cleaned:")
    print(f"  - /Volumes/{catalog}/{schema}/{volume}/images")
