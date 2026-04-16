# Confidence Scoring Strategy for Utility Bill Extraction

## Overview

Achieving >95% accuracy requires a robust confidence scoring mechanism that can self-assess extraction quality. Since Llama 4 Maverick outputs reasoning before the final answer, we can leverage this for multi-dimensional confidence scoring.

---

## Confidence Scoring Architecture

### 1. Multi-Signal Confidence Framework

Rather than relying on a single confidence score, we implement a **composite confidence system** that evaluates multiple independent signals:

```
Final Confidence = weighted_average(
    format_confidence,      # Does the value match expected patterns?
    context_confidence,     # Was it found in the expected location?
    reasoning_confidence,   # How certain was the model's reasoning?
    cross_validation_score, # Do multiple extraction paths agree?
    consistency_score       # Is it consistent with other extracted fields?
)
```

### 2. Signal Definitions

#### A. Format Confidence (Weight: 30%)
Validates extracted values against known format rules:

| Utility | Field | Expected Pattern | Validation |
|---------|-------|------------------|------------|
| PG&E | Account Number | `^\d{10}-?\d?$` | 10 digits, optional check digit |
| PG&E | Service Agreement ID | `^\d{10}$` | Exactly 10 digits |
| PG&E | Meter ID | `^\d{10}$` | Exactly 10 digits |
| SCE | Account Number | `^\d{10,12}$` | 10-12 digits |
| SCE | Service Agreement ID | `^\d{10}$` | Exactly 10 digits |
| SCE | Meter ID | `^222\d{7,11}$` | Starts with 222, 10-14 digits |
| SDG&E | Account Number | `^\d{10,16}$` | 10-16 digits (strip spaces) |
| SDG&E | Meter Number | `^\d{8}$` | Exactly 8 digits (preserve leading zeros) |
| ComED | Account Number | `^\d{10}$` | Exactly 10 digits |
| ComED | Electric Choice ID | `^\d{10}$` | Exactly 10 digits |
| ComED | Meter Number | `^\d{8,9}$` | 8-9 digits |
| Ameren | Account Number | `^\d{10}$` | Exactly 10 digits |
| Ameren | Meter Number | `^\d{8,9}$` | 8-9 digits |

**Scoring:**
- 100%: Exact pattern match
- 70%: Partial match (correct length but includes non-digits)
- 30%: Numeric but wrong length
- 0%: Non-numeric or clearly invalid

#### B. Context Confidence (Weight: 25%)
Evaluates whether the value was found near expected labels:

**Expected Label Proximity:**
```json
{
  "meter_id": ["Meter #", "Meter Number", "Meter No", "Electric Meter", "Electric Meter #", "Smart Meter", "Medidor"],
  "account_number": ["Account #", "Account Number", "Account No", "Customer account", "ACCOUNT NUMBER", "Cuenta", "Número de cuenta"],
  "service_agreement_id": ["Service Agreement ID", "Service Agreement", "SA ID", "Service account"],
  "electric_choice_id": ["Electric Choice ID", "Service Agreement", "Choice ID", "ECID"]
}
```

**Scoring:**
- 100%: Found directly adjacent to expected label
- 80%: Found within 50 characters of expected label
- 50%: Found in expected section but not near label
- 20%: Found but no recognizable context
- 0%: Not found

#### C. Reasoning Confidence (Weight: 20%)
Parsed from the model's reasoning output using specific markers:

**Prompt instructs model to output confidence indicators:**
```
In your reasoning, indicate your certainty level:
- "CERTAIN:" for values you're highly confident about
- "LIKELY:" for values that match patterns but have some ambiguity
- "UNCERTAIN:" for values you're guessing at
- "NOT_FOUND:" when you cannot locate the field
```

**Scoring:**
- CERTAIN: 95%
- LIKELY: 70%
- UNCERTAIN: 30%
- NOT_FOUND: 0%

#### D. Cross-Validation Score (Weight: 15%)
For critical fields, perform multiple extraction strategies and compare:

1. **Visual extraction**: Direct prompt to find field
2. **Table extraction**: Look specifically in tabular data
3. **Pattern search**: Regex scan of entire document
4. **Section-based**: Navigate to known section first

**Scoring:**
- 4/4 strategies agree: 100%
- 3/4 strategies agree: 85%
- 2/4 strategies agree: 50%
- No agreement: 0%

#### E. Consistency Score (Weight: 10%)
Cross-checks between related fields:

- Account numbers on different pages should match
- Meter number format should be consistent with utility
- Service address state should match utility jurisdiction

**Scoring:**
- All consistency checks pass: 100%
- Minor inconsistencies: 70%
- Major inconsistencies: 30%
- Contradictions found: 0%

---

## Implementation Approach

### Phase 1: Extraction with Reasoning

```python
EXTRACTION_PROMPT = """
You are extracting utility bill information. Analyze this utility bill document carefully.

TASK: Extract the following fields based on utility type:
- For California utilities (PG&E, SCE, SDG&E): Meter ID/Number
- For Illinois ComED: Account Number, Electric Choice ID, Meter Number
- For Illinois Ameren: Account Number, Meter Number

REASONING PROCESS:
1. First identify the utility company from logos, headers, or contact information
2. Locate the relevant section for each required field
3. Extract the value and validate against expected format
4. Rate your confidence for each extraction

OUTPUT FORMAT:
<reasoning>
[Your step-by-step analysis here]
For each field, state:
- Where you found it (page, section, nearby labels)
- The exact value extracted
- Confidence indicator: CERTAIN/LIKELY/UNCERTAIN/NOT_FOUND
- Any validation concerns
</reasoning>

<extraction>
{
  "utility_provider": "string",
  "utility_state": "CA|IL",
  "program": "ELRP|PJM|MISO",
  "fields": {
    "meter_id": {"value": "string|null", "confidence_indicator": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND", "location": "string"},
    "account_number": {"value": "string|null", "confidence_indicator": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND", "location": "string"},
    "electric_choice_id": {"value": "string|null", "confidence_indicator": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND", "location": "string"},
    "meter_number": {"value": "string|null", "confidence_indicator": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND", "location": "string"}
  }
}
</extraction>
"""
```

### Phase 2: Post-Processing Confidence Calculation

```python
def calculate_composite_confidence(extraction_result, raw_text):
    """
    Calculate composite confidence score from multiple signals
    """
    scores = {}

    for field_name, field_data in extraction_result['fields'].items():
        if field_data['value'] is None:
            scores[field_name] = {'value': None, 'confidence': 0, 'status': 'not_found'}
            continue

        # Signal 1: Format validation
        format_score = validate_format(field_name, field_data['value'], extraction_result['utility_provider'])

        # Signal 2: Context validation
        context_score = validate_context(field_data['value'], field_data['location'], raw_text)

        # Signal 3: Reasoning confidence
        reasoning_score = map_confidence_indicator(field_data['confidence_indicator'])

        # Signal 4: Cross-validation (optional, for high-value extractions)
        cross_val_score = cross_validate(field_name, field_data['value'], raw_text) if ENABLE_CROSS_VALIDATION else None

        # Signal 5: Consistency check
        consistency_score = check_consistency(field_name, field_data['value'], extraction_result)

        # Weighted composite
        composite = calculate_weighted_score(
            format_score=format_score,
            context_score=context_score,
            reasoning_score=reasoning_score,
            cross_val_score=cross_val_score,
            consistency_score=consistency_score
        )

        scores[field_name] = {
            'value': field_data['value'],
            'confidence': composite,
            'signals': {
                'format': format_score,
                'context': context_score,
                'reasoning': reasoning_score,
                'cross_validation': cross_val_score,
                'consistency': consistency_score
            },
            'status': determine_status(composite)
        }

    return scores

def determine_status(confidence):
    """
    Determine extraction status based on confidence threshold
    """
    if confidence >= 0.95:
        return 'high_confidence'  # Auto-accept
    elif confidence >= 0.80:
        return 'medium_confidence'  # Accept with flag
    elif confidence >= 0.60:
        return 'low_confidence'  # Requires manual review
    else:
        return 'failed'  # Reject, requires manual entry
```

### Phase 3: Decision Matrix

| Confidence Level | Range | Action | QSS Review |
|------------------|-------|--------|------------|
| High | ≥95% | Auto-accept, store in Salesforce | Spot-check 5% |
| Medium | 80-94% | Accept with "needs_verification" flag | Review 25% |
| Low | 60-79% | Queue for manual review | Review 100% |
| Failed | <60% | Reject, manual entry required | N/A |

---

## Additional Confidence Enhancements

### 1. Historical Learning
Store extraction results with human-verified corrections to:
- Identify systematic errors by utility type
- Adjust confidence weights based on actual accuracy
- Build a "known issues" database for edge cases

### 2. Checksum Validation (where applicable)
Some utility account numbers have built-in check digits:
- PG&E: Account number has check digit (format: XXXXXXXXXX-X)
- Validate using Luhn algorithm or utility-specific checksum

### 3. Duplicate Detection
Cross-reference extracted values against existing Salesforce records:
- Same meter ID for different customers → flag for review
- Same account with different meter → verify service change

### 4. Anomaly Detection
Flag unusual patterns:
- Meter numbers that don't match regional patterns
- Account numbers outside expected ranges
- Missing fields that are normally present

---

## Output Schema

```json
{
  "extraction_id": "uuid",
  "timestamp": "ISO8601",
  "source_file": "s3://bucket/path/to/file.pdf",
  "utility_classification": {
    "provider": "PG&E",
    "state": "CA",
    "program": "ELRP",
    "classification_confidence": 0.98
  },
  "extracted_fields": {
    "meter_id": {
      "value": "1011207547",
      "confidence": 0.96,
      "status": "high_confidence",
      "signals": {
        "format": 1.0,
        "context": 0.95,
        "reasoning": 0.95,
        "cross_validation": 0.92,
        "consistency": 1.0
      },
      "location": {
        "page": 3,
        "section": "Service Information",
        "nearby_label": "Meter #"
      }
    }
  },
  "overall_confidence": 0.96,
  "requires_review": false,
  "review_reasons": [],
  "raw_reasoning": "...(model reasoning output)...",
  "processing_metadata": {
    "model": "llama-4-maverick",
    "processing_time_ms": 2340,
    "pages_processed": 5
  }
}
```

---

## Handling Edge Cases

### 1. Multiple Meters on One Bill
Some commercial accounts have multiple meters:
- Extract all meter numbers as array
- Flag for review with "multiple_meters" reason
- Let QSS team determine primary meter

### 2. Partial/Damaged Documents
- Track "page_quality" score for each page
- If OCR confidence is low, flag entire extraction
- Suggest manual re-scan if quality < threshold

### 3. Non-Standard Bill Formats
- Maintain a "bill_version" indicator
- Store unrecognized formats for prompt refinement
- Graceful degradation: extract what's possible, flag rest

### 4. Language Variations
For Spanish bills:
- Include Spanish labels in context matching
- "Medidor" = "Meter", "Cuenta" = "Account"
- Confidence scoring works identically

---

## Recommended Thresholds (Initial)

Based on the requirement of >95% accuracy, I recommend:

| Metric | Threshold | Rationale |
|--------|-----------|-----------|
| Auto-accept | ≥95% confidence | Achieves target accuracy |
| Manual review trigger | <80% confidence | Ensures human oversight for uncertain extractions |
| Rejection | <60% confidence | Too risky to accept even with review |
| QSS spot-check rate | 5% of high-confidence | Validates model performance over time |

These thresholds should be tuned based on actual production data after initial deployment.
