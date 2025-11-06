# Databricks Asset Bundle Examples for GenAI Workflows

Production-ready examples demonstrating incremental document processing workflows using Databricks Asset Bundles, Structured Streaming, and AI functions.

## Workflows

### 1. [Unstructured Information Extraction Workflow](./unstructured_ie_workflow)
A streamlined 4-stage pipeline for extracting structured data from documents:
- **Parse** PDFs/images with `ai_parse_document`
- **Extract** clean text content
- **Analyze** content with `ai_query` or agent bricks endpoints
- **Export** structured entities as JSONL

**Best for**: Information extraction, document classification, entity recognition, financial data extraction

**Key features**: JSONL export, CLI workflow runner with file management, serverless compute, optional cleanup stage

### 2. [Unstructured Knowledge Base Workflow](./unstructured_data_knowledge_base_workflow)
A comprehensive 9-stage pipeline for creating searchable knowledge bases:
- **Parse** documents with bounding boxes and metadata
- **Extract** text, elements, and page information
- **Crop** diagrams and charts for specialized analysis
- **Chunk** content for embeddings
- **Enrich** text with visual diagram information
- **Analyze** with LLMs for structured extraction

**Best for**: RAG applications, knowledge bases, multi-modal document understanding, technical documentation processing

**Key features**: Diagram extraction, content chunking, visual enrichment, foreachBatch processing

## Architecture

Both workflows use:
- **Databricks Asset Bundles** for CI/CD and infrastructure-as-code
- **Structured Streaming** with checkpointing for incremental processing
- **Unity Catalog** for data governance
- **Serverless Compute** for cost efficiency
- **Task Dependencies** for orchestration

## Quick Start

```bash
# Authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Choose a workflow
cd unstructured_ie_workflow  # or unstructured_data_knowledge_base_workflow

# Deploy and run (with file management)
./run_workflow.sh --profile YOUR_PROFILE --upload-pdfs --download-jsonl
```

See individual workflow READMEs for detailed configuration and usage.

## Resources

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks Workflows](https://docs.databricks.com/workflows/)
- [Structured Streaming](https://docs.databricks.com/structured-streaming/)
- [`ai_parse_document` Function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document)
- [`ai_query` Function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query)
