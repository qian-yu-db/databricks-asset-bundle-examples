# Unstructured Knowledge Base Workflow with Structured Streaming

A comprehensive Databricks Asset Bundle demonstrating **advanced multi-modal document processing** for building searchable knowledge bases using `ai_parse_document`, `ai_query`, diagram extraction, content chunking, and visual enrichment.

## Overview

This workflow implements a complete 9-stage pipeline for creating knowledge bases from unstructured documents:
1. **Clean** pipeline tables and checkpoints (optional)
2. **Parse** PDFs and images with [`ai_parse_document`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document)
3. **Extract** text content (02_1)
4. **Extract** document elements with bounding boxes (02_2)
5. **Extract** page information and images (02_3)
6. **Crop** diagrams and charts from pages (03_1)
7. **Chunk** content for embeddings (03_2)
8. **Enrich** text with diagram descriptions using LLMs (04_1)
9. **Analyze** enriched content for structured extraction (04_2)

All stages run as Python notebook tasks in a Databricks Workflow using Structured Streaming with serverless compute.

## Architecture

```
Source Documents (UC Volume)
         ↓
  Task 0: clean_pipeline_tables (optional)
         ↓
  Task 1: ai_parse_document → parsed_documents_raw (variant)
         ↓
    ┌────┴────┬────────────┐
    ↓         ↓            ↓
Task 2_1   Task 2_2    Task 2_3
content    elements     pages
    │         │            │
    └────┬────┴─────┬──────┘
         ↓          ↓
     Task 3_1   Task 3_2
  crop_diagrams  chunk_content
         │          │
         ↓          │
     Task 4_1       │
  enrich_content    │
  (diagram info)    │
         │          │
         └────┬─────┘
              ↓
          Task 4_2
     extract_structured_data
```

### Key Features

- **Multi-modal processing**: Text, tables, diagrams, and images
- **Diagram extraction**: Crops and analyzes diagrams separately using multimodal LLMs
- **Content chunking**: Semantic chunking for vector embeddings and RAG
- **Visual enrichment**: Augments text with diagram descriptions (pins diagram info to relevant text)
- **Incremental processing**: Streaming with checkpoints prevents reprocessing
- **Parallel execution**: Independent tasks run concurrently for efficiency
- **foreachBatch processing**: Complex joins and aggregations using batch operations
- **Serverless compute**: Cost-efficient execution
- **Fully parameterized**: 8 configurable tables, volumes, and paths

## Prerequisites

- Databricks workspace with Unity Catalog
- Databricks CLI v0.218.0+
- Unity Catalog volumes for:
  - Source documents (PDFs/images)
  - Parsed output images
  - Cropped diagrams
  - Streaming checkpoints
- AI functions (`ai_parse_document`, `ai_query`)
- Multimodal LLM support for diagram analysis

## Quick Start

### Option 1: Shell Script with CLI

1. **Install and authenticate**
   ```bash
   databricks auth login --host https://your-workspace.cloud.databricks.com
   ```

2. **Configure** `databricks.yml` with your workspace settings

3. **Run complete workflow**
   ```bash
   ./run_workflow.sh --profile YOUR_PROFILE
   ```

### Option 2: Manual Steps

1. **Validate** the bundle configuration
   ```bash
   databricks bundle validate --profile YOUR_PROFILE
   ```

2. **Deploy**
   ```bash
   databricks bundle deploy --profile YOUR_PROFILE
   ```

3. **Upload documents** to your source volume

4. **Run workflow**
   ```bash
   databricks bundle run unstructured_document_knowledge_base_workflow --profile YOUR_PROFILE
   ```

## Configuration

### Bundle Configuration
Edit `databricks.yml`:

```yaml
variables:
  catalog: fins_genai                                             # Your catalog
  schema: unstructured_documents                                  # Your schema
  source_volume_path: /Volumes/fins_genai/.../inputs/            # Source PDFs
  output_volume_path: /Volumes/fins_genai/.../outputs/           # Parsed images & cropped diagrams
  checkpoint_base_path: /Volumes/fins_genai/.../checkpoints/     # Streaming checkpoints
  clean_pipeline_tables: Yes                                     # Clean tables on run

  # Table names (8 tables)
  raw_table_name: parsed_documents_raw                           # Raw parsed docs
  content_table_name: parsed_documents_content                   # Extracted text
  elements_table_name: parsed_documents_elements                 # Elements with bounding boxes
  pages_table_name: parsed_documents_pages                       # Page info
  cropped_images_table_name: parsed_documents_cropped_images     # Cropped diagrams
  chunked_content_table_name: parsed_documents_chunked_content   # Chunked text
  content_enriched_table_name: parsed_documents_content_enriched # Text + diagram info
  structured_table_name: parsed_documents_structured             # Final structured data

  agent_choice: ai_query                                          # ai_query or agent_bricks
```

### AI Extraction Configuration
Edit `src/transformations/config.yaml` to customize LLM extraction.

## Workflow Tasks

### Task 0: Pipeline Cleanup (Optional)
**File**: `src/transformations/00-clean-pipeline-tables.py`

Conditionally cleans all 8 tables and checkpoints:
- Controlled by `clean_pipeline_tables` variable (Yes/No)
- Removes existing tables, checkpoints, and output files
- Enables fresh processing runs

### Task 1: Document Parsing
**File**: `src/transformations/01_parse_documents.py`

Uses `ai_parse_document` to extract text, tables, metadata, and bounding boxes:
- Reads files from volume using Structured Streaming
- Stores variant output with element coordinates
- Incremental: checkpointed streaming prevents reprocessing

### Task 2_1: Content Extraction
**File**: `src/transformations/02_1_extract_document_content.py`

Extracts concatenated clean text:
- Reads from raw table via streaming
- Handles both parser v1.0 and v2.0 formats
- Concatenates text elements while preserving structure

### Task 2_2: Element Extraction
**File**: `src/transformations/02_2_extract_document_elements.py`

Extracts individual elements with bounding boxes:
- Parses variant data to extract elements array
- Includes element type, text, coordinates, and confidence scores
- Used for diagram detection and chunking

### Task 2_3: Page Extraction
**File**: `src/transformations/02_3_extract_document_pages.py`

Extracts page-level information and images:
- Parses variant data to extract pages array
- Includes page images stored in output volume
- Used for diagram cropping

### Task 3_1: Diagram Cropping
**File**: `src/transformations/03_1_crop_diagrams.py`

Crops diagrams and charts from pages:
- Joins elements and pages tables
- Filters for element types: 'figure', 'chart', 'table', 'logo'
- Crops images based on bounding boxes
- Stores cropped images in output volume

### Task 3_2: Content Chunking
**File**: `src/transformations/03_2_chunked_document_content.py`

Chunks content for vector embeddings:
- Semantic chunking based on element boundaries
- Maintains context for RAG applications
- Optimized for embedding generation

### Task 4_1: Visual Content Enrichment
**File**: `src/transformations/04_1_extract_info_from_cropped_diagrams.py`

Enriches text with diagram descriptions:
- Uses multimodal LLM to analyze cropped diagrams
- Generates natural language descriptions
- **Pins diagram info to relevant text** using foreachBatch with left join
- Aggregates diagram descriptions per document
- Outputs enriched content table

**Technical Note**: Uses `foreachBatch` to perform complex left outer join between streaming and batch DataFrames, avoiding watermark constraints.

### Task 4_2: Structured Data Extraction
**File**: `src/transformations/04_2_extract_key_info.py`

Applies LLM to extract structured insights from enriched content:
- Reads from content_enriched table (text + diagram info)
- Uses `ai_query` with Claude Sonnet 4 or agent bricks endpoint
- Customizable prompt for domain-specific extraction
- Outputs structured JSON to Delta table

## Visual Debugger

Interactive notebook for visualizing parsing results with bounding boxes.

**Open**: `src/explorations/ai_parse_document -- debug output.py`

**Configure widgets**:
- `input_file`: `/Volumes/main/default/source_docs/sample.pdf`
- `image_output_path`: `/Volumes/main/default/parsed_out/`
- `page_selection`: `all` (or `1-3`, `1,5,10`)

**Features**:
- Color-coded bounding boxes by element type
- Hover tooltips showing extracted content
- Automatic image scaling
- Page selection support

## Project Structure

```
.
├── databricks.yml                      # Bundle configuration
├── run_workflow.sh                     # Workflow runner with CLI
├── resources/
│   └── ai_parse_document_workflow.job.yml  # Job definition
├── src/
│   ├── transformations/
│   │   ├── 00-clean-pipeline-tables.py              # Pipeline cleanup
│   │   ├── 01_parse_documents.py                    # PDF/image parsing
│   │   ├── 02_1_extract_document_content.py         # Text extraction
│   │   ├── 02_2_extract_document_elements.py        # Element extraction
│   │   ├── 02_3_extract_document_pages.py           # Page extraction
│   │   ├── 03_1_crop_diagrams.py                    # Diagram cropping
│   │   ├── 03_2_chunked_document_content.py         # Content chunking
│   │   ├── 04_1_extract_info_from_cropped_diagrams.py # Visual enrichment
│   │   ├── 04_2_extract_key_info.py                 # Structured extraction
│   │   └── config.yaml                              # AI extraction config
│   └── explorations/
│       └── ai_parse_document -- debug output.py     # Visual debugger
└── README.md
```

## Output and Results

### Generated Tables (8 total)
1. **parsed_documents_raw**: Raw variant output from ai_parse_document
2. **parsed_documents_content**: Concatenated text content
3. **parsed_documents_elements**: Individual elements with bounding boxes
4. **parsed_documents_pages**: Page-level information
5. **parsed_documents_cropped_images**: Metadata for cropped diagram images
6. **parsed_documents_chunked_content**: Semantically chunked content
7. **parsed_documents_content_enriched**: Text enriched with diagram descriptions
8. **parsed_documents_structured**: Final structured extraction results

### Generated Files
- **Page images**: Parsed page images in `output_volume_path/`
- **Cropped diagrams**: Diagram images in `output_volume_path/` for analysis

## Use Cases

- **RAG Applications**: Chunked content with visual enrichment for retrieval
- **Knowledge Bases**: Searchable, enriched content with multimodal understanding
- **Technical Documentation**: Process diagrams, flowcharts, and technical drawings
- **Financial Reports**: Extract charts, tables, and structured data
- **Research Papers**: Analyze figures, equations, and supplementary materials

## Advanced Features

### foreachBatch Processing
Task 4_1 uses `foreachBatch` to:
- Read cropped diagram analysis results as streaming data
- Join with parsed elements using regular batch DataFrame operations
- Perform complex aggregations without streaming constraints
- Handle left outer joins without watermark requirements

### Parallel Execution
Tasks are organized for maximum parallelism:
- Tasks 2_1, 2_2, 2_3 run in parallel (all depend only on Task 1)
- Tasks 3_1 and 3_2 run in parallel when their dependencies are met

### Incremental Processing
All streaming tasks use checkpoints to track processed data:
- Only new documents trigger pipeline execution
- Failed documents can be reprocessed without affecting successful ones
- Idempotent: safe to re-run the entire workflow

## Resources

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks Workflows](https://docs.databricks.com/workflows/)
- [Structured Streaming](https://docs.databricks.com/structured-streaming/)
- [`ai_parse_document` Function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document)
- [`ai_query` Function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)
- [foreachBatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch)
