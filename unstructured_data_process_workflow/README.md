# Parse-Translate-Classify Workflow - Databricks Asset Bundle

A Databricks Asset Bundle (DAB) for processing unstructured documents (PDFs, PPTX, DOCX) through a multi-stage pipeline that parses, translates, segments, and classifies content.

## Overview

This workflow identifies and extracts CVs (resumes) and credentials from documents using AI-powered parsing, translation, and classification. It's implemented using **Spark Structured Streaming** for incremental, fault-tolerant processing.

### What This Workflow Does

1. **Parse Documents**: Extracts text and images from PDF, PPTX, and DOCX files
2. **Translate Content**: Converts content to English while preserving formatting
3. **Segment & Classify**: Identifies individual CVs and credentials within documents
4. **Transform**: Creates individual records for each CV/credential found

## Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────┐
│          Parse-Translate-Classify Workflow                    │
└──────────────────────────────────────────────────────────────┘

Stage 0: Cleanup
  └─> 00-clean-pipeline-tables.py

Stage 1: Parse Documents
  └─> 01_parse_documents.py
      ├─ Input: *.{pdf,pptx,docx}
      └─ Output: raw_parsed_files table

Stage 2: Translate
  └─> 02_translate_content.py
      ├─ Input: raw_parsed_files table
      └─ Output: translated_content table

Stage 3: Segment & Transform
  └─> 03_segment_classify_transform.py
      ├─ Input: translated_content table
      └─ Outputs: parsed_content + parsed_records tables
```

## Prerequisites

### Databricks Requirements
- Databricks Runtime with AI Functions support
- Unity Catalog enabled
- Access to Foundation Model APIs:
  - `databricks-meta-llama-3-3-70b-instruct` (for translation)
  - `databricks-claude-sonnet-4-5` (for classification)

### Unity Catalog Setup
Before deploying, ensure you have:
1. **Catalog**: `fins_genai` (or customize via variables)
2. **Schema**: `genai_hackathon` (or customize via variables)
3. **Volume**: `docs_for_redaction` with subdirectories:
   - `original/` - for source documents
   - `images/` - for extracted images
   - `checkpoints/` - for streaming checkpoints

### Databricks CLI
Install and authenticate the Databricks CLI:
```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate (follow prompts)
databricks configure --token
```

## Quick Start

### 1. Clone and Navigate
```bash
cd unstructured_data_process_workflow
```

### 2. Validate the Bundle
```bash
databricks bundle validate
```

### 3. Deploy to Development
```bash
databricks bundle deploy -t dev
```

### 4. Run the Workflow
```bash
# Run the entire workflow
databricks bundle run -t dev parse_translate_classify_workflow

# Or run via Databricks UI
# Navigate to Workflows -> parse_translate_classify_workflow -> Run now
```

### 5. Deploy to Production
```bash
databricks bundle deploy -t prod
```

## Configuration

### LLM Models and Prompts (config.yaml)

LLM models and prompts are configured in `src/transformations/config.yaml`:

```yaml
# Translation Configuration
TRANSLATION_MODEL: "databricks-meta-llama-3-3-70b-instruct"
TRANSLATION_PROMPT: |
  translate to english. Retain the original formatting, only translate...

# Classification and Segmentation Configuration
CLASSIFICATION_MODEL: "databricks-claude-sonnet-4-5"
SEGMENTATION_PROMPT: |
  Analyze this document and identify if it contains multiple CVs...
```

This centralized configuration allows you to:
- Change LLM models without modifying DAB configuration
- Update prompts independently from pipeline parameters
- Version control AI behavior separately from infrastructure

### Variables (databricks.yml)

Customize infrastructure and pipeline settings in `databricks.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `fins_genai` | Unity Catalog name |
| `schema` | `genai_hackathon` | Schema name |
| `volume` | `docs_for_redaction` | Volume name for files |
| `table_prefix` | `redaction_workflow` | Prefix for all tables |
| `clean_pipeline_tables` | `"No"` | Clean tables before run |
| `partition_count` | `"5"` | Processing partitions |
| `checkpoint_base_path` | `checkpoints/parse_translate_classify_workflow` | Checkpoint location |

### Example: Custom Configuration

Create a custom target in `databricks.yml`:

```yaml
targets:
  custom:
    mode: development
    workspace:
      host: https://your-workspace.cloud.databricks.com
    variables:
      catalog: my_catalog
      schema: my_schema
      volume: my_volume
      table_prefix: my_workflow
      clean_pipeline_tables: "Yes"
```

Deploy with custom configuration:
```bash
databricks bundle deploy -t custom
databricks bundle run -t custom parse_translate_classify_workflow
```

## Job Structure

### Tasks

The workflow consists of 4 sequential tasks:

#### 1. `clean_pipeline_tables`
- **Notebook**: `00-clean-pipeline-tables.py`
- **Purpose**: Cleanup utility
- **Actions**:
  - Ensures required directories exist
  - Optionally drops tables and cleans checkpoints

#### 2. `parse_documents`
- **Notebook**: `01_parse_documents.py`
- **Depends on**: `clean_pipeline_tables`
- **Purpose**: Parse documents using `ai_parse_document`
- **Output**: `{table_prefix}_raw_parsed_files`

#### 3. `translate_content`
- **Notebook**: `02_translate_content.py`
- **Depends on**: `parse_documents`
- **Purpose**: Translate to English using `ai_query`
- **Output**: `{table_prefix}_translated_content`

#### 4. `segment_classify_transform`
- **Notebook**: `03_segment_classify_transform.py`
- **Depends on**: `translate_content`
- **Purpose**: Segment, classify, and transform records
- **Outputs**:
  - `{table_prefix}_parsed_content`
  - `{table_prefix}_parsed_records`

## Tables Created

With default `table_prefix: "redaction_workflow"`:

| Table Name | Description | Schema |
|------------|-------------|--------|
| `redaction_workflow_raw_parsed_files` | Raw parsed documents | path, file_type, parsed (VARIANT), parsed_at |
| `redaction_workflow_translated_content` | Translated English text | path, file_type, parsed, translated_content, ... |
| `redaction_workflow_parsed_content` | Segmented analysis | path, file_type, split_analysis (ARRAY), ... |
| `redaction_workflow_parsed_records` | Individual CV/credential records | path, item_id, document_type, person_name, email, ... |

## Directory Structure

```
unstructured_data_process_workflow/
├── databricks.yml                      # Main DAB configuration
├── README.md                           # This file
├── resources/
│   └── parse_translate_classify_workflow.job.yml  # Job definition
├── src/
│   └── transformations/
│       ├── 00-clean-pipeline-tables.py
│       ├── 01_parse_documents.py
│       ├── 02_translate_content.py
│       ├── 03_segment_classify_transform.py
│       ├── config.yaml                 # Prompts and configuration
│       └── README.md                   # Technical documentation
└── parse-translate-classification.py   # Original batch implementation (reference)
```

## Monitoring and Troubleshooting

### View Job Status
```bash
# List all runs
databricks jobs list

# Get specific job details
databricks jobs get --job-id <job-id>

# View run output
databricks runs get-output --run-id <run-id>
```

### Databricks UI
1. Navigate to **Workflows** in Databricks UI
2. Find `parse_translate_classify_workflow`
3. View runs, logs, and results

### Common Issues

#### 1. Checkpoint Conflicts
**Error**: `Checkpoint location already exists`

**Solution**: Set `clean_pipeline_tables: "Yes"` or manually clean:
```sql
-- Drop tables
DROP TABLE IF EXISTS redaction_workflow_raw_parsed_files;
DROP TABLE IF EXISTS redaction_workflow_translated_content;
DROP TABLE IF EXISTS redaction_workflow_parsed_content;
DROP TABLE IF EXISTS redaction_workflow_parsed_records;
```

#### 2. Model Access Issues
**Error**: `Model not found` or `Permission denied`

**Solution**: Verify access to models:
```python
# Test in Databricks notebook
spark.sql("SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', 'test')").show()
spark.sql("SELECT ai_query('databricks-claude-sonnet-4-5', 'test')").show()
```

#### 3. Volume Not Found
**Error**: `Volume does not exist`

**Solution**: Create volume structure:
```sql
CREATE VOLUME IF NOT EXISTS fins_genai.genai_hackathon.docs_for_redaction;
```

Then create subdirectories via notebook or DBFS.

## Streaming Implementation

This workflow uses **Spark Structured Streaming** with:
- **Trigger**: `availableNow=True` (batch semantics)
- **Checkpointing**: Ensures exactly-once processing
- **Incremental**: Only new data is processed
- **Fault-tolerant**: Can resume from failures

### Benefits
- Processes only new files added to volume
- No duplicate processing
- Can run on schedule for continuous ingestion
- Scalable for large datasets

## Customization

### Change Models and Prompts
Edit models and prompts in `src/transformations/config.yaml`:
```yaml
# Change models
TRANSLATION_MODEL: "databricks-gpt-5-mini"
CLASSIFICATION_MODEL: "databricks-claude-sonnet-4-5"

# Customize prompts
TRANSLATE_PROMPT: |
  Your custom translation prompt here

SEGMENT_PROMPT: |
  Your custom segmentation prompt here
```

After updating the config file, redeploy the bundle for changes to take effect:
```bash
databricks bundle deploy -t dev
```

### Add New Tasks
1. Create new notebook in `src/transformations/`
2. Add task to `resources/parse_translate_classify_workflow.job.yml`
3. Define dependencies using `depends_on`

### Schedule Workflow
Uncomment schedule in job definition:
```yaml
schedule:
  quartz_cron_expression: "0 0 * * * ?"  # Daily at midnight
  timezone_id: "UTC"
```

## Cost Optimization

### Model Selection
- **Translation**: Llama 3.3 70B (cost-effective, good quality)
- **Classification**: Claude Sonnet 4.5 (higher accuracy for complex parsing)

### Processing Optimization
- Adjust `partition_count` based on data volume
- Use `availableNow` trigger for batch processing
- Monitor token usage in model metrics

## Support and Documentation

### Resources
- [Databricks Asset Bundles Docs](https://docs.databricks.com/dev-tools/bundles/index.html)
- [AI Functions Documentation](https://docs.databricks.com/sql/language-manual/functions/ai_query.html)
- [Structured Streaming Guide](https://docs.databricks.com/structured-streaming/index.html)

### Technical Documentation
See `src/transformations/README.md` for detailed notebook documentation.

## License

This workflow is provided as-is for use with Databricks Asset Bundles.
