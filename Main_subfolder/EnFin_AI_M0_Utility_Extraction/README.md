# Utility Bill Extraction System

Automated utility data extraction from utility bills for VPP (Virtual Power Plant) data tape generation.

## Overview

This system extracts utility-specific customer identifiers from utility bills using a combination of:
- **AWS Bedrock (Llama 4)** - Vision-based extraction with reasoning
- **AWS Textract** - High-confidence OCR for cross-validation
- **AWS Bedrock Prompt Management** - Centralized prompt storage with local fallback
- **AWS DynamoDB** - Extraction result persistence
- **Salesforce** - CRM integration via TokenManager Lambda layer

## Supported Utilities

| Utility | State | Program | Fields Extracted |
|---------|-------|---------|------------------|
| PG&E | CA | ELRP | Meter ID |
| SCE | CA | ELRP | Meter ID |
| SDG&E | CA | ELRP | Meter ID |
| ComED | IL | PJM | Account Number, Electric Choice ID, Meter Number |
| Ameren | IL | MISO | Account Number, Meter Number |

## Architecture

```
PDF → fitz (images) → LLM#1 (page detection) → Textract (reference text)
    → LLM#2 (extraction with cross-validation) → LLM#3 (confidence scoring)
    → [Retry loop if needed] → Salesforce
```

### Processing Pipeline

1. **Document Ingestion**: PDF converted to page images using PyMuPDF (fitz)
2. **Page Detection**: LLM identifies which pages contain required data
3. **Textract Processing**: Creates high-confidence reference text
4. **Data Extraction**: LLM extracts values, cross-validating against reference text
5. **Confidence Scoring**: LLM validates extraction with detailed scoring
6. **Retry Loop**: Re-attempts low-confidence fields (up to 2 retries)
7. **Output Routing**: Auto-accept, flag for review, or manual queue

## Configuration

All settings are configurable via environment variables. See `.env.example` for a complete list.

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | - | S3 bucket containing utility bills |
| `TEXTRACT_MIN_CONFIDENCE` | 90.0 | Minimum Textract confidence (%) |
| `MAX_RETRIES` | 2 | Maximum retry attempts |
| `RETRY_THRESHOLD` | 0.80 | Confidence below which to retry |
| `LOCK_THRESHOLD` | 0.95 | Confidence above which to lock values |
| `AUTO_ACCEPT_THRESHOLD` | 0.95 | Confidence for auto-accept |
| `FLAG_THRESHOLD` | 0.80 | Confidence for flagged accept |
| `EXTRACT_CANDIDATE_VALUES` | true | Enable pattern-based candidate extraction |

### Bedrock Prompt Management

| Variable | Description |
|----------|-------------|
| `PROMPT_ID_PAGE_DETECTION` | Prompt ID for page detection |
| `PROMPT_ID_EXTRACTION` | Prompt ID for field extraction |
| `PROMPT_ID_CONFIDENCE_SCORING` | Prompt ID for confidence scoring |
| `PROMPT_ID_FOCUSED_RETRY` | Prompt ID for focused retry |

### DynamoDB

| Variable | Default | Description |
|----------|---------|-------------|
| `DYNAMODB_TABLE_NAME` | utility-extraction-results | Table for storing results |

### Salesforce Integration

| Variable | Description |
|----------|-------------|
| `SF_TOKEN_TABLE_NAME` | DynamoDB table for SF tokens |
| `SF_HOST` | Salesforce instance URL |
| `SF_AUTH_PATH` | OAuth token endpoint path |
| `SF_USERNAME` | Salesforce integration user |
| `SF_SECRET_NAME` | Secrets Manager secret name |

## Bedrock Prompt Management Setup

Before deployment, create the following prompts in AWS Bedrock Prompt Management:

1. **Page Detection Prompt** - Identifies pages containing utility data
2. **Extraction Prompt** - Extracts field values with cross-validation
3. **Confidence Scoring Prompt** - Validates and scores extractions
4. **Focused Retry Prompt** - Re-extracts low-confidence fields

Reference prompts are available in `src/config/prompts.py`. The system will automatically fall back to these local prompts if Bedrock Prompt Management is unavailable.

## Deployment

### Using SAM CLI

```bash
# Build
sam build

# Deploy
sam deploy --guided

# Or with parameters
sam deploy \
  --parameter-overrides \
    Environment=prod \
    S3BucketName=my-utility-bills-bucket \
    TextractMinConfidence=90 \
    MaxRetries=2
```

### Manual Lambda Deployment

```bash
# Install dependencies
pip install -r requirements.txt -t package/

# Copy source code
cp -r src package/

# Create deployment package
cd package && zip -r ../deployment.zip . && cd ..

# Upload to Lambda
aws lambda update-function-code \
  --function-name utility-bill-extraction \
  --zip-file fileb://deployment.zip
```

## Testing

```bash
# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## Triggering Extraction

Send a message to the SQS queue:

```json
{
  "path": "utility-bills/2024/11/project-123/bill.pdf",
  "project_id": "a]1234567890ABC",
  "timestamp": "2024-11-20T10:30:00Z"
}
```

## Output Format

```json
{
  "classification": {
    "provider": "PG&E",
    "state": "CA",
    "program": "ELRP",
    "confidence": "HIGH"
  },
  "fields": {
    "meter_id": {
      "extracted_value": "1011207547",
      "validated_value": "1011207547",
      "confidence_score": 0.96,
      "validation_passed": true,
      "reference_confirmed": true
    }
  },
  "overall_confidence": 0.96,
  "recommendation": "auto_accept"
}
```

## Data Storage

### DynamoDB Schema

Extraction results are stored in DynamoDB with the following structure:

| Attribute | Type | Description |
|-----------|------|-------------|
| `extraction_id` | String (PK) | Unique extraction identifier |
| `project_id` | String (GSI) | Project identifier for querying |
| `source_file` | String | S3 path to source PDF |
| `utility_provider` | String | Detected utility (PG&E, SCE, etc.) |
| `state` | String | State code (CA, IL) |
| `extracted_fields` | Map | Field values with confidence |
| `overall_confidence` | Number | Overall confidence score |
| `recommendation` | String | auto_accept, flag_for_review, manual_required |
| `requires_review` | Boolean (GSI) | Flag for pending reviews |
| `salesforce_record_id` | String | Linked Salesforce record |
| `salesforce_sync_status` | String | synced, pending_review, manual_review |
| `ttl` | Number | Auto-expiry timestamp (90 days) |

## Confidence Scoring

Scores are calculated based on:

| Factor | Weight |
|--------|--------|
| Reference text match | +0.35 |
| Format validation | +0.25 |
| Label proximity | +0.20 |
| Consistency check | +0.10 |
| LLM confidence adjustment | +0.10 |

### Decision Thresholds

| Confidence | Action |
|------------|--------|
| ≥95% | Auto-accept to Salesforce |
| 80-94% | Accept with verification flag |
| <80% | Route to QSS manual review |

## License

Internal use only - EnFin
