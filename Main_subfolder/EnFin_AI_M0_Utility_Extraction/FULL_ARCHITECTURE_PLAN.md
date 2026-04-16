# Complete Architecture Plan: Utility Bill Data Extraction System

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Processing Pipeline](#processing-pipeline)
4. [Component Details](#component-details)
5. [Confidence Scoring with OCR Verification](#confidence-scoring-with-ocr-verification)
6. [Data Flow](#data-flow)
7. [Error Handling](#error-handling)
8. [Cost Optimization](#cost-optimization)
9. [Configuration Management](#configuration-management)
10. [Monitoring & Alerting](#monitoring--alerting)

---

## 1. System Overview

### Purpose
Automatically extract utility-specific customer identifiers from utility bills received at M0 milestone, store in Salesforce, and make available for VPP data tape generation.

### Key Requirements
- >95% extraction accuracy
- Support for 5 utilities: PG&E, SCE, SDG&E (CA), ComED, Ameren (IL)
- Bilingual support (English/Spanish)
- Configurable and dynamic (not utility-specific prompts)
- Robust confidence scoring with OCR verification

### Technology Stack
| Component | Technology |
|-----------|------------|
| Compute | AWS Lambda |
| Queue | AWS SQS |
| Storage | AWS S3 |
| AI Extraction | AWS Bedrock (Llama 4 Maverick) |
| OCR Verification | AWS Textract |
| CRM | Salesforce |
| Monitoring | CloudWatch |

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           UTILITY BILL EXTRACTION SYSTEM                                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌─────────────┐
                                    │  M0 Upload  │
                                    │   (Source)  │
                                    └──────┬──────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│  ┌─────────────┐      ┌─────────────┐      ┌──────────────────────────────────────────┐  │
│  │             │      │             │      │          EXTRACTION LAMBDA               │  │
│  │   S3        │─────▶│   SQS       │─────▶│  ┌────────────────────────────────────┐  │  │
│  │   Bucket    │      │   Queue     │      │  │     STEP 1: DOCUMENT PREP          │  │  │
│  │             │      │             │      │  │  • Download PDF from S3            │  │  │
│  │ utility-    │      │ {"path":    │      │  │  • Convert to images (page-wise)   │  │  │
│  │ bills/      │      │  "file.pdf"}│      │  │  • Identify utility from logo/text │  │  │
│  │             │      │             │      │  └───────────────┬────────────────────┘  │  │
│  └─────────────┘      └─────────────┘      │                  │                       │  │
│                                            │                  ▼                       │  │
│                                            │  ┌────────────────────────────────────┐  │  │
│                                            │  │    STEP 2: LLM EXTRACTION          │  │  │
│  ┌─────────────────────────────────────┐   │  │  ┌────────────────────────────┐    │  │  │
│  │         AWS BEDROCK                 │◀──┼──│  │   Llama 4 Maverick         │    │  │  │
│  │    (Llama 4 Maverick)               │   │  │  │   • Universal prompt       │    │  │  │
│  │                                     │───┼──│  │   • Reasoning + extraction │    │  │  │
│  │  Input: Page images + prompt        │   │  │  │   • Confidence indicators  │    │  │  │
│  │  Output: JSON with reasoning        │   │  │  └────────────────────────────┘    │  │  │
│  └─────────────────────────────────────┘   │  └───────────────┬────────────────────┘  │  │
│                                            │                  │                       │  │
│                                            │                  ▼                       │  │
│                                            │  ┌────────────────────────────────────┐  │  │
│  ┌─────────────────────────────────────┐   │  │    STEP 3: OCR VERIFICATION        │  │  │
│  │         AWS TEXTRACT                │◀──┼──│  ┌────────────────────────────┐    │  │  │
│  │                                     │   │  │  │   Textract Analysis        │    │  │  │
│  │  Input: Target page(s) only         │───┼──│  │   • Extract all text       │    │  │  │
│  │  Output: High-accuracy text/tables  │   │  │  │   • Find extracted values  │    │  │  │
│  │                                     │   │  │  │   • Compare with LLM       │    │  │  │
│  └─────────────────────────────────────┘   │  │  └────────────────────────────┘    │  │  │
│                                            │  └───────────────┬────────────────────┘  │  │
│                                            │                  │                       │  │
│                                            │                  ▼                       │  │
│                                            │  ┌────────────────────────────────────┐  │  │
│                                            │  │    STEP 4: MISMATCH RESOLUTION     │  │  │
│                                            │  │  (Only if LLM ≠ Textract)          │  │  │
│  ┌─────────────────────────────────────┐   │  │  ┌────────────────────────────┐    │  │  │
│  │    RESOLUTION PROMPT (Bedrock)      │◀──┼──│  │   Second LLM Pass          │    │  │  │
│  │                                     │   │  │  │   • Show both values       │    │  │  │
│  │  "LLM extracted: X                  │───┼──│  │   • Textract OCR context   │    │  │  │
│  │   Textract found: Y                 │   │  │  │   • Make final decision    │    │  │  │
│  │   Which is correct?"                │   │  │  └────────────────────────────┘    │  │  │
│  └─────────────────────────────────────┘   │  └───────────────┬────────────────────┘  │  │
│                                            │                  │                       │  │
│                                            │                  ▼                       │  │
│                                            │  ┌────────────────────────────────────┐  │  │
│                                            │  │    STEP 5: CONFIDENCE SCORING      │  │  │
│                                            │  │  • Format validation (30%)         │  │  │
│                                            │  │  • Context validation (25%)        │  │  │
│                                            │  │  • Reasoning confidence (20%)      │  │  │
│                                            │  │  • OCR match score (15%)           │  │  │
│                                            │  │  • Consistency check (10%)         │  │  │
│                                            │  └───────────────┬────────────────────┘  │  │
│                                            └──────────────────┼────────────────────────┘  │
│                                                               │                          │
└───────────────────────────────────────────────────────────────┼──────────────────────────┘
                                                                │
                            ┌───────────────────────────────────┼───────────────────────────┐
                            │                                   ▼                           │
                            │  ┌────────────────────────────────────────────────────────┐   │
                            │  │              DECISION ENGINE                           │   │
                            │  │  ┌──────────────┬──────────────┬──────────────────┐   │   │
                            │  │  │ ≥95%         │ 80-94%       │ <80%             │   │   │
                            │  │  │ AUTO-ACCEPT  │ FLAG+ACCEPT  │ MANUAL REVIEW    │   │   │
                            │  │  └──────┬───────┴──────┬───────┴────────┬─────────┘   │   │
                            │  └─────────┼──────────────┼────────────────┼─────────────┘   │
                            │            │              │                │                 │
                            │            ▼              ▼                ▼                 │
                            │  ┌─────────────────────────────────────────────────────────┐ │
                            │  │                    SALESFORCE                           │ │
                            │  │  ┌────────────────────────────────────────────────────┐ │ │
                            │  │  │  Custom Fields:                                    │ │ │
                            │  │  │  • Meter_ID__c                                     │ │ │
                            │  │  │  • Account_Number__c                               │ │ │
                            │  │  │  • Electric_Choice_ID__c                           │ │ │
                            │  │  │  • Meter_Number__c                                 │ │ │
                            │  │  │  • Extraction_Confidence__c                        │ │ │
                            │  │  │  • Needs_Verification__c                           │ │ │
                            │  │  │  • Extraction_Timestamp__c                         │ │ │
                            │  │  └────────────────────────────────────────────────────┘ │ │
                            │  └─────────────────────────────────────────────────────────┘ │
                            │                                                              │
                            │  ┌─────────────────────────────────────────────────────────┐ │
                            │  │           QSS REVIEW QUEUE (for <80% confidence)        │ │
                            │  │  • Dashboard for manual verification                    │ │
                            │  │  • Side-by-side: extracted value vs. bill image         │ │
                            │  │  • One-click approve/correct workflow                   │ │
                            │  └─────────────────────────────────────────────────────────┘ │
                            └──────────────────────────────────────────────────────────────┘
```

---

## 3. Processing Pipeline

### Step-by-Step Flow

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                              DETAILED PROCESSING PIPELINE                                   │
└────────────────────────────────────────────────────────────────────────────────────────────┘

Step 1: DOCUMENT PREPARATION
├── 1.1 Receive SQS message with S3 path
├── 1.2 Download PDF from S3
├── 1.3 Convert PDF to page images (150 DPI for balance of quality/cost)
├── 1.4 Quick utility classification:
│       ├── Check first page for utility logo/header
│       ├── Identify: PG&E | SCE | SDG&E | ComED | Ameren
│       └── Determine state (CA/IL) and program (ELRP/PJM/MISO)
└── 1.5 Select relevant pages based on utility type:
        ├── PG&E: Pages 3-4 (meter info in Service Information)
        ├── SCE: Page 3 (usage section)
        ├── SDG&E: Pages 1, 3 (header + Detail of Charges)
        ├── ComED: Pages 1-2 (header + Meter Information)
        └── Ameren: Pages 1, 3 (header + Electric Meter Read)

Step 2: LLM EXTRACTION (Primary)
├── 2.1 Send relevant page image(s) to Bedrock Llama 4 Maverick
├── 2.2 Use universal extraction prompt (see prompt design below)
├── 2.3 Parse response:
│       ├── Extract <reasoning> section
│       └── Extract <extraction> JSON
├── 2.4 For each field, capture:
│       ├── Extracted value
│       ├── Confidence indicator (CERTAIN/LIKELY/UNCERTAIN)
│       └── Location description
└── 2.5 Store page number(s) where each field was found

Step 3: OCR VERIFICATION (Textract)
├── 3.1 For each extracted field with value:
│       ├── Send the specific page to AWS Textract
│       ├── Use AnalyzeDocument with FORMS and TABLES features
│       └── Get high-accuracy text extraction
├── 3.2 Search Textract output for LLM-extracted value:
│       ├── Exact match → OCR confirms LLM
│       ├── Similar match (1-2 char diff) → Flag for resolution
│       └── No match → Flag for resolution
├── 3.3 Also search for pattern matches (in case LLM got wrong value):
│       ├── Find all 10-digit numbers (account/meter candidates)
│       ├── Find all 8-digit numbers (meter candidates)
│       └── Find values near expected labels
└── 3.4 Record OCR findings for comparison

Step 4: MISMATCH RESOLUTION (Conditional)
├── 4.1 If LLM value ≠ Textract value:
│       ├── Prepare resolution prompt with:
│       │   ├── LLM's extracted value and reasoning
│       │   ├── Textract's OCR text (surrounding context)
│       │   ├── Expected field format
│       │   └── Ask model to determine correct value
│       └── Send to Bedrock for second opinion
├── 4.2 Resolution outcomes:
│       ├── Confirms LLM value → Use LLM value
│       ├── Confirms Textract value → Use Textract value
│       ├── Provides third value → Validate and use
│       └── Cannot determine → Flag for manual review
└── 4.3 Record resolution decision and rationale

Step 5: CONFIDENCE SCORING
├── 5.1 Calculate format_confidence (30%):
│       └── Validate against regex patterns per utility/field
├── 5.2 Calculate context_confidence (25%):
│       └── Check if found near expected labels in Textract output
├── 5.3 Calculate reasoning_confidence (20%):
│       └── Map CERTAIN/LIKELY/UNCERTAIN to scores
├── 5.4 Calculate ocr_match_confidence (15%):
│       ├── 100% if LLM = Textract (exact)
│       ├── 80% if LLM = Textract after resolution
│       ├── 50% if resolution needed but successful
│       └── 20% if resolution failed
├── 5.5 Calculate consistency_confidence (10%):
│       └── Cross-check with other fields and document metadata
└── 5.6 Compute weighted composite score

Step 6: OUTPUT AND ROUTING
├── 6.1 Based on confidence score:
│       ├── ≥95%: Auto-accept
│       ├── 80-94%: Accept with needs_verification flag
│       └── <80%: Route to QSS review queue
├── 6.2 Store in Salesforce (via API):
│       ├── Update project/opportunity record
│       ├── Set extraction confidence score
│       └── Set verification flag if applicable
├── 6.3 Store extraction metadata in S3:
│       └── Full extraction log for audit/debugging
└── 6.4 Publish CloudWatch metrics:
        ├── extraction_success_rate
        ├── average_confidence_score
        ├── ocr_mismatch_rate
        └── manual_review_rate
```

---

## 4. Component Details

### 4.1 Lambda Function Structure

```
utility-bill-extraction/
├── lambda_function.py          # Main handler
├── config/
│   ├── extraction_config.json  # Field definitions per utility
│   └── prompts.py              # Prompt templates
├── services/
│   ├── s3_service.py           # S3 operations
│   ├── bedrock_service.py      # Bedrock API wrapper
│   ├── textract_service.py     # Textract API wrapper
│   └── salesforce_service.py   # Salesforce API integration
├── extractors/
│   ├── document_processor.py   # PDF to image conversion
│   ├── utility_classifier.py   # Identify utility from document
│   ├── llm_extractor.py        # Main extraction logic
│   └── ocr_verifier.py         # Textract verification
├── scoring/
│   ├── confidence_calculator.py # Multi-signal scoring
│   ├── format_validator.py      # Regex pattern validation
│   └── mismatch_resolver.py     # Resolution prompt logic
├── models/
│   ├── extraction_result.py    # Data classes
│   └── confidence_score.py     # Score models
└── utils/
    ├── logger.py               # Structured logging
    └── metrics.py              # CloudWatch metrics
```

### 4.2 Lambda Configuration

```yaml
Runtime: Python 3.11
Memory: 1024 MB (minimum for image processing)
Timeout: 5 minutes (for complex multi-page documents)
Environment Variables:
  - S3_BUCKET: utility-bills-bucket
  - BEDROCK_MODEL_ID: meta.llama4-maverick-17b-instruct-v1:0
  - SALESFORCE_INSTANCE_URL: https://enfin.my.salesforce.com
  - SALESFORCE_CLIENT_ID: (from Secrets Manager)
  - SALESFORCE_CLIENT_SECRET: (from Secrets Manager)
  - CONFIDENCE_AUTO_ACCEPT_THRESHOLD: 0.95
  - CONFIDENCE_FLAG_THRESHOLD: 0.80
  - ENABLE_OCR_VERIFICATION: true
  - LOG_LEVEL: INFO
```

### 4.3 SQS Queue Configuration

```yaml
Queue Name: utility-bill-extraction-queue
Visibility Timeout: 6 minutes (> Lambda timeout)
Message Retention: 7 days
Dead Letter Queue: utility-bill-extraction-dlq
Max Receive Count: 3 (retry failed messages up to 3 times)
```

---

## 5. Confidence Scoring with OCR Verification

### Updated Signal Weights

| Signal | Weight | Description |
|--------|--------|-------------|
| Format Validation | 30% | Regex pattern match |
| Context Validation | 25% | Found near expected labels |
| Reasoning Confidence | 20% | LLM's self-reported confidence |
| **OCR Match Score** | **15%** | **Agreement between LLM and Textract** |
| Consistency Check | 10% | Cross-field validation |

### OCR Match Score Calculation

```python
def calculate_ocr_match_score(llm_value, textract_results, field_name):
    """
    Compare LLM extraction with Textract OCR results

    Returns:
        - match_score: 0.0 to 1.0
        - textract_value: The value found by Textract (if any)
        - match_type: 'exact' | 'fuzzy' | 'pattern' | 'not_found'
    """

    # 1. Exact match check
    if llm_value in textract_results.get_all_text():
        return 1.0, llm_value, 'exact'

    # 2. Fuzzy match (handles minor OCR/LLM differences)
    for word in textract_results.get_all_words():
        similarity = calculate_levenshtein_similarity(llm_value, word)
        if similarity >= 0.9:  # 90% similar (1-2 char difference)
            return 0.85, word, 'fuzzy'

    # 3. Pattern-based search (find candidates matching expected format)
    pattern = get_pattern_for_field(field_name)
    candidates = find_pattern_matches(textract_results.get_all_text(), pattern)

    if llm_value in candidates:
        return 0.8, llm_value, 'pattern'
    elif candidates:
        # LLM value not found, but valid candidates exist
        return 0.3, candidates[0], 'pattern_mismatch'

    # 4. Not found in Textract output
    return 0.0, None, 'not_found'
```

### Mismatch Resolution Prompt

```python
MISMATCH_RESOLUTION_PROMPT = """
You are verifying utility bill data extraction. There is a discrepancy between two extraction methods.

FIELD TO VERIFY: {field_name}
EXPECTED FORMAT: {expected_format}

EXTRACTION METHOD 1 (AI Vision):
Value: {llm_value}
Reasoning: {llm_reasoning}
Location: {llm_location}

EXTRACTION METHOD 2 (OCR):
Value: {textract_value}
Surrounding context: "{textract_context}"

OCR TEXT FROM RELEVANT SECTION:
{textract_section_text}

TASK:
1. Analyze both extractions
2. Consider which is more likely correct based on:
   - Format validity (does it match expected pattern?)
   - Context appropriateness (is it in the right section?)
   - Common OCR/vision errors (0 vs O, 1 vs l, 8 vs B, etc.)
3. Determine the correct value

<reasoning>
[Analyze the discrepancy step by step]
</reasoning>

<decision>
{
  "correct_value": "the value you determined is correct",
  "source": "llm|textract|neither",
  "confidence": "CERTAIN|LIKELY|UNCERTAIN",
  "explanation": "brief explanation of your decision"
}
</decision>
"""
```

---

## 6. Data Flow

### Input Message (SQS)
```json
{
  "path": "utility-bills/2024/11/project-12345/utility-bill.pdf",
  "project_id": "a]1234567890ABC",
  "timestamp": "2024-11-20T10:30:00Z",
  "source": "partner-portal-upload"
}
```

### Extraction Output (Internal)
```json
{
  "extraction_id": "ext-uuid-12345",
  "project_id": "a]1234567890ABC",
  "source_file": "s3://bucket/utility-bills/2024/11/project-12345/utility-bill.pdf",
  "processing_timestamp": "2024-11-20T10:30:45Z",

  "classification": {
    "utility_provider": "PG&E",
    "utility_state": "CA",
    "program": "ELRP",
    "classification_confidence": 0.99
  },

  "extracted_fields": {
    "meter_id": {
      "value": "1011207547",
      "final_confidence": 0.96,
      "status": "high_confidence",
      "signals": {
        "format_validation": 1.0,
        "context_validation": 0.95,
        "reasoning_confidence": 0.95,
        "ocr_match_score": 1.0,
        "consistency_check": 1.0
      },
      "extraction_details": {
        "llm_value": "1011207547",
        "textract_value": "1011207547",
        "match_type": "exact",
        "resolution_needed": false,
        "page_found": 3,
        "section": "Service Information",
        "nearby_label": "Meter #"
      }
    }
  },

  "overall_status": "success",
  "requires_review": false,
  "review_reasons": [],

  "processing_metadata": {
    "total_pages": 5,
    "pages_processed": 2,
    "llm_calls": 1,
    "textract_calls": 1,
    "resolution_calls": 0,
    "processing_time_ms": 4521,
    "model_used": "llama-4-maverick"
  },

  "raw_llm_reasoning": "...(full reasoning text)..."
}
```

### Salesforce Update Payload
```json
{
  "Id": "a1234567890ABC",
  "Meter_ID__c": "1011207547",
  "Extraction_Confidence__c": 0.96,
  "Extraction_Status__c": "Verified",
  "Needs_Manual_Verification__c": false,
  "Last_Extraction_Date__c": "2024-11-20T10:30:45Z",
  "Extraction_Source__c": "AI_Automated"
}
```

---

## 7. Error Handling

### Error Categories and Actions

| Error Type | Action | Retry | Alert |
|------------|--------|-------|-------|
| S3 download failure | Retry with backoff | 3x | After 3 failures |
| PDF conversion failure | DLQ + alert | No | Immediate |
| Bedrock API error | Retry with backoff | 3x | After 3 failures |
| Textract API error | Continue without OCR verification | No | Log warning |
| Salesforce API error | Retry + DLQ | 3x | After 3 failures |
| Low confidence (<60%) | Route to manual review | No | Daily summary |
| Unrecognized utility | DLQ + manual processing | No | Immediate |

### Dead Letter Queue Processing

Documents that fail extraction after retries:
1. Move to DLQ
2. Create Salesforce task for manual processing
3. Store failure reason and partial extraction (if any)
4. Include in daily failure report

---

## 8. Cost Optimization

### Cost Components

| Service | Cost Factor | Optimization |
|---------|-------------|--------------|
| Bedrock Llama 4 | Per token (input/output) | Send only relevant pages |
| Textract | Per page | Only analyze pages with extracted values |
| Lambda | Duration × Memory | Optimize image conversion |
| S3 | Storage + requests | Use Intelligent-Tiering |

### Cost Optimization Strategies

1. **Smart Page Selection**: Only process pages likely to contain target fields
2. **Conditional Textract**: Only run OCR verification on pages where values were found
3. **Conditional Resolution**: Only run mismatch resolution when LLM ≠ Textract
4. **Image Optimization**: Use 150 DPI (balance quality vs. file size)
5. **Caching**: Cache utility classification patterns

### Estimated Cost Per Document

| Step | Average Cost |
|------|--------------|
| S3 operations | $0.0001 |
| PDF to image conversion | $0.0002 |
| Bedrock LLM (2 pages avg) | $0.008 |
| Textract (1-2 pages) | $0.003 |
| Resolution prompt (20% of docs) | $0.002 × 0.2 = $0.0004 |
| **Total per document** | **~$0.012** |

At 1000 documents/month: ~$12/month

---

## 9. Configuration Management

### extraction_config.json

```json
{
  "version": "1.0.0",
  "utilities": {
    "PG&E": {
      "state": "CA",
      "program": "ELRP",
      "required_fields": ["meter_id"],
      "page_hints": [3, 4],
      "field_configs": {
        "meter_id": {
          "labels": ["Meter #", "Meter Number", "Electric Meter"],
          "pattern": "^\\d{10}$",
          "section_hints": ["Service Information", "Meter Information"]
        }
      },
      "logo_keywords": ["PG&E", "Pacific Gas", "pge.com"]
    },
    "SCE": {
      "state": "CA",
      "program": "ELRP",
      "required_fields": ["meter_id"],
      "page_hints": [3],
      "field_configs": {
        "meter_id": {
          "labels": ["Meter", "Smart Meter", "Meter Number"],
          "pattern": "^222\\d{7,11}$",
          "section_hints": ["electricity usage", "usage details"]
        }
      },
      "logo_keywords": ["SCE", "Edison", "Southern California Edison"]
    },
    "SDG&E": {
      "state": "CA",
      "program": "ELRP",
      "required_fields": ["meter_id"],
      "page_hints": [3],
      "field_configs": {
        "meter_id": {
          "labels": ["Meter Number", "Meter No"],
          "pattern": "^\\d{8}$",
          "section_hints": ["Detail of Current Charges", "Electric Service"],
          "notes": "Often starts with 0 - preserve leading zeros"
        }
      },
      "logo_keywords": ["SDG&E", "San Diego Gas", "sdge.com"]
    },
    "ComED": {
      "state": "IL",
      "program": "PJM",
      "required_fields": ["account_number", "electric_choice_id", "meter_number"],
      "page_hints": [1, 2],
      "field_configs": {
        "account_number": {
          "labels": ["Account #", "Account Number", "Account"],
          "pattern": "^\\d{10}$",
          "section_hints": ["header", "top of page"]
        },
        "electric_choice_id": {
          "labels": ["Electric Choice ID", "Choice ID", "ECID"],
          "pattern": "^\\d{10}$",
          "section_hints": ["SERVICE ADDRESS", "service details"]
        },
        "meter_number": {
          "labels": ["Meter Number", "Meter", "Meter No"],
          "pattern": "^\\d{8}$",
          "section_hints": ["METER INFORMATION", "meter table"]
        }
      },
      "logo_keywords": ["ComEd", "Commonwealth Edison", "comed.com"]
    },
    "Ameren": {
      "state": "IL",
      "program": "MISO",
      "required_fields": ["account_number", "meter_number"],
      "page_hints": [1, 3],
      "field_configs": {
        "account_number": {
          "labels": ["Account Number", "Account No", "Número de cuenta"],
          "pattern": "^\\d{10}$",
          "section_hints": ["header", "customer information"]
        },
        "meter_number": {
          "labels": ["METER NUMBER", "Meter Number", "Meter"],
          "pattern": "^\\d{8}$",
          "section_hints": ["Electric Meter Read", "meter details", "billing detail"]
        }
      },
      "logo_keywords": ["Ameren", "AmerenIllinois", "ameren.com"]
    }
  },
  "confidence_thresholds": {
    "auto_accept": 0.95,
    "flag_for_review": 0.80,
    "reject": 0.60
  },
  "signal_weights": {
    "format_validation": 0.30,
    "context_validation": 0.25,
    "reasoning_confidence": 0.20,
    "ocr_match_score": 0.15,
    "consistency_check": 0.10
  }
}
```

---

## 10. Monitoring & Alerting

### CloudWatch Metrics

| Metric | Type | Alert Threshold |
|--------|------|-----------------|
| `ExtractionSuccessRate` | Percentage | < 90% |
| `AverageConfidenceScore` | Gauge | < 0.85 |
| `OCRMismatchRate` | Percentage | > 20% |
| `ManualReviewRate` | Percentage | > 15% |
| `ProcessingDuration` | Timer | > 30s avg |
| `TextractErrorRate` | Percentage | > 5% |
| `BedrockErrorRate` | Percentage | > 2% |

### CloudWatch Alarms

```yaml
Alarms:
  - Name: LowExtractionSuccessRate
    Metric: ExtractionSuccessRate
    Threshold: < 90%
    Period: 1 hour
    Action: SNS notification to ops team

  - Name: HighManualReviewRate
    Metric: ManualReviewRate
    Threshold: > 15%
    Period: 24 hours
    Action: SNS notification to QSS team lead

  - Name: BedrockApiFailures
    Metric: BedrockErrorRate
    Threshold: > 5%
    Period: 15 minutes
    Action: PagerDuty alert
```

### Daily Summary Report

Automated email to stakeholders with:
- Total documents processed
- Success/failure/review breakdown
- Average confidence by utility type
- OCR mismatch rate trends
- Top 5 failure reasons
- Documents requiring manual attention

---

## Next Steps

1. **Approve this architecture plan**
2. **Create extraction prompt** - Design the universal prompt for all utilities
3. **Implement Lambda function** - Build the extraction pipeline
4. **Test with sample bills** - Validate against the 5 provided samples
5. **Deploy to staging** - Test with real data
6. **Create Salesforce fields** - Work with SF admin on custom fields
7. **Build QSS review interface** - Dashboard for manual verification
8. **Production deployment** - With monitoring and alerting
