# Refined Architecture Plan: Utility Bill Data Extraction System

## Critical Review & Refinements

Based on your feedback, I've refined the pipeline. Here's my critical analysis:

---

## Revised Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           REFINED EXTRACTION PIPELINE                                    │
└─────────────────────────────────────────────────────────────────────────────────────────┘

STEP 1: DOCUMENT INGESTION
├── 1.1 Receive SQS message → Download PDF from S3
├── 1.2 Use PyMuPDF (fitz) to convert ALL pages to images
└── 1.3 Store page images temporarily

STEP 2: Document content is extracted as Text preserving format using Docling including image details if any present

Step 3 : PDF is created for only those pages containing

STEP 4: DATA EXTRACTION (LLM Call #1)
├── 4.1 Send Docling Text content as well as page images to LLM
├── 4.2 Use extraction prompt such that page image is used to detect layout and identify where entity is pesent and then text present in docling content is used to return values with reasoning
├── 4.3 LLM extracts: field values + page numbers + confidence indicators
└── 4.4 Output: Extracted values with locations

STEP 5: CONFIDENCE SCORING (Rule based)
├── 5.1 Loop through all extracted values
│       ├── IF extracted value is present in docling : confidence unchanged
│       ├── IF extracted value is absent in docling text : confidence changes to 0.75
│       ├── Check all Expected formats/patterns, match - confidence unchanged, regex mismatch - confidence drops to 0.75
└── 5.2 Output: Final values with confidence scores

STEP 6: Post to SF - Post to SF using similar mechanism as present in EnFin_SN_Scheduled_Salesforce_Update
```

---

## Critical Analysis: What I Got Wrong Before

### Previous Approach Issues:

1. **Mismatch Resolution Prompt was Unnecessary**
   - Old: If LLM ≠ Textract → Run separate resolution prompt
   - Problem: This adds complexity and another LLM call
   - Better: Use confidence scoring prompt to do cross-validation in one step

2. **Textract Processing Was Inefficient**
   - Old: Run Textract, then compare programmatically, then maybe resolve
   - Problem: Multiple steps with branching logic
   - Better: Create reference text once, pass to confidence scoring prompt

3. **Confidence Calculation Was Split**
   - Old: Part programmatic (format regex), part LLM (reasoning)
   - Problem: Fragmented logic, harder to maintain
   - Better: Let the confidence scoring prompt handle all validation

### Your Approach is Better Because:

1. **Cleaner Data Flow**: No conditional branching (no "if mismatch then...")
2. **Single Source of Truth**: Textract reference text is the ground truth
3. **LLM Does the Thinking**: The confidence scoring prompt can reason about edge cases humans would catch
4. **Simpler Code**: Fewer conditional paths = fewer bugs

---

## Refined Pipeline Details

### STEP 2: Smart Page Detection Prompt

```python
PAGE_DETECTION_PROMPT = """
You are analyzing a utility bill document to identify which pages contain customer account and meter information.

LOOK FOR PAGES CONTAINING:
- Account Numbers (usually 10-digit numbers near "Account #" or "Account Number")
- Meter Numbers/IDs (usually 8-10 digit numbers near "Meter #", "Meter Number", or "Service Information")
- Electric Choice IDs (for Illinois utilities, near "Electric Choice ID")
- Service Information sections with detailed billing data

ALSO IDENTIFY:
- The utility company (PG&E, SCE, SDG&E, ComED, or Ameren)
- The state (California or Illinois)

<output>
{
  "utility_provider": "string (PG&E|SCE|SDG&E|ComED|Ameren|Unknown)",
  "state": "string (CA|IL|Unknown)",
  "relevant_pages": [list of page numbers (1-indexed)],
  "reasoning": "brief explanation of why these pages were selected"
}
</output>
"""
```

**Why this is better**: We're not guessing which pages - we ask the LLM to scan all pages and tell us. This is dynamic and works for any utility/bill format.

---

### STEP 3: Data Extraction Prompt

```python
EXTRACTION_PROMPT = """
You are extracting utility bill data from the provided page images.

CONTEXT:
- Utility Provider: {utility_provider}
- State: {state}
- Program: {program}

FIELDS TO EXTRACT based on program:
{fields_to_extract}

For each field, extract:
1. The value (digits only, no formatting)
2. The page number where you found it
3. The section/label near the value
4. Your confidence: CERTAIN | LIKELY | UNCERTAIN | NOT_FOUND

IMPORTANT:
- Read digits carefully. Common confusions: 0↔O, 1↔l, 5↔S, 8↔B
- If a value appears multiple times, use the one in the most authoritative section
- Preserve leading zeros (important for meter numbers)

<reasoning>
[Your step-by-step analysis for each field]
</reasoning>

<extraction>
{
  "fields": {
    "field_name": {
      "value": "extracted_value or null",
      "page": page_number,
      "section": "section/label name",
      "confidence": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND"
    }
  }
}
</extraction>
"""
```

---

### STEP 4: Textract Reference Text Generation

```python
def generate_reference_text(page_images, relevant_pages):
    """
    Generate high-confidence reference text from Textract
    """
    reference_texts = {}

    for page_num in relevant_pages:
        # Call Textract on the page
        response = textract_client.analyze_document(
            Document={'Bytes': page_images[page_num]},
            FeatureTypes=['FORMS', 'TABLES']
        )

        # Filter to HIGH confidence words only (≥90%)
        high_confidence_words = []
        for block in response['Blocks']:
            if block['BlockType'] == 'WORD':
                if block['Confidence'] >= 90.0:
                    high_confidence_words.append({
                        'text': block['Text'],
                        'confidence': block['Confidence']
                    })

        # Create reference text
        # Option 1: Simple concatenation
        reference_text = ' '.join([w['text'] for w in high_confidence_words])

        # Option 2: Preserve some structure (lines)
        # This might be better for context validation
        lines = extract_lines_with_confidence(response, min_confidence=90.0)
        structured_reference = '\n'.join(lines)

        reference_texts[page_num] = {
            'raw_text': reference_text,
            'structured_text': structured_reference,
            'word_count': len(high_confidence_words)
        }

    return reference_texts
```

**Key insight**: By filtering to ≥90% confidence words only, we create a "clean" reference that the LLM can trust. If a digit wasn't captured with high confidence by Textract, we don't include it - this prevents Textract's own errors from propagating.

---

### STEP 5: Confidence Scoring Prompt (The Key Innovation)

```python
CONFIDENCE_SCORING_PROMPT = """
You are a data validation expert. Your job is to verify extracted utility bill data against OCR reference text and assign confidence scores.

## EXTRACTED DATA (from AI vision):
{extracted_data_json}

## REFERENCE TEXT (from high-confidence OCR):
Page {page_num}:
{reference_text}

## VALIDATION TASKS:

For each extracted field, perform these checks:

### 1. VALUE PRESENCE CHECK (Cross-Validation)
- Does the extracted value appear EXACTLY in the reference text?
- If not exact, is there a very similar value (1-2 character difference)?
- If value not found, is there a different value that matches the expected format near the expected label?

### 2. FORMAT VALIDATION
- Does the value match the expected pattern?
{format_rules}

### 3. CONTEXT VALIDATION
- Is the value found near expected labels in the reference text?
- Expected labels: {expected_labels}
- Search the reference text for these labels and check proximity

### 4. CONSISTENCY VALIDATION
- Do the extracted values make sense together?
- Is the utility provider consistent with the state?
- Are there any contradictions?

## SCORING RUBRIC:

For each field, calculate a confidence score (0.0 to 1.0):

| Condition | Score Impact |
|-----------|--------------|
| Exact match in reference text | +0.40 |
| Near expected label in reference | +0.25 |
| Correct format | +0.20 |
| Consistent with other fields | +0.10 |
| LLM indicated CERTAIN | +0.05 |
| --- | --- |
| Value NOT in reference text | -0.30 |
| Wrong format | -0.30 |
| Near wrong label | -0.20 |
| LLM indicated UNCERTAIN | -0.15 |

## OUTPUT:

<reasoning>
For each field:
1. [VALUE PRESENCE] Did I find "{value}" in reference text? Where?
2. [FORMAT] Does it match pattern {pattern}? Yes/No
3. [CONTEXT] Is it near "{expected_label}"? Yes/No (show nearby text)
4. [CONSISTENCY] Any issues with other extracted values?
5. [SCORE CALCULATION] Show math: base + adjustments = final
</reasoning>

<validation_result>
{
  "fields": {
    "field_name": {
      "extracted_value": "original value",
      "validated_value": "value after validation (may be corrected)",
      "value_source": "extraction|reference|both_match",
      "confidence_score": 0.XX,
      "score_breakdown": {
        "presence_in_reference": 0.XX,
        "format_valid": 0.XX,
        "context_valid": 0.XX,
        "consistency": 0.XX
      },
      "validation_notes": "any issues or corrections made"
    }
  },
  "overall_confidence": 0.XX,
  "requires_review": true/false,
  "review_reasons": ["list of concerns if any"]
}
</validation_result>
"""
```

**This prompt does everything**:
1. Cross-validation (LLM vs Textract)
2. Format validation (pattern matching)
3. Context validation (label proximity)
4. Consistency checks
5. Score calculation with clear rubric
6. Potential value correction (if reference text has different value)

---

## What is Cross-Validation in This Context?

You asked about cross-validation. In this pipeline, it means:

**Cross-validation = Comparing two independent extraction methods**

| Method 1: LLM Vision | Method 2: Textract OCR |
|----------------------|------------------------|
| Looks at image, "reads" the value | Uses ML OCR to extract text |
| May hallucinate or misread | High confidence on clear text |
| Good at understanding context | No semantic understanding |
| May confuse similar digits | Better at digit recognition |

**When both agree**: High confidence that value is correct
**When they disagree**: Need to determine which is right (that's what the scoring prompt does)

The confidence scoring prompt essentially asks:
> "I extracted {value} from the image. The OCR reference text contains these words: {reference}. Is my extraction correct? If not, what's the right value?"

---

## Comparison: Old vs. New Pipeline

| Aspect | Old Pipeline | New Pipeline |
|--------|--------------|--------------|
| **LLM Calls** | 1-3 (extraction + optional resolution) | 3 (detection + extraction + scoring) |
| **Branching Logic** | Complex (if mismatch → resolve) | Linear (always same steps) |
| **Confidence Calculation** | Split between code and LLM | Unified in scoring prompt |
| **Textract Usage** | Compare programmatically | Create reference text, LLM validates |
| **Value Correction** | Separate resolution prompt | Built into scoring prompt |
| **Maintainability** | Multiple conditional paths | Single clear flow |

### LLM Call Count Analysis

| Scenario | Old Pipeline | New Pipeline |
|----------|--------------|--------------|
| Perfect extraction | 1 LLM call | 3 LLM calls |
| Extraction with mismatch | 2-3 LLM calls | 3 LLM calls |

**Trade-off**: New pipeline always uses 3 calls, but:
- Simpler code
- More consistent behavior
- Scoring prompt can catch errors that programmatic comparison would miss
- Total tokens may be similar (scoring prompt is smaller than full extraction)

---

## Potential Issues & Solutions

### Issue 1: What if Textract misses a value?
**Scenario**: LLM extracts "1011207547" but Textract didn't capture it with high confidence

**Solution**: The scoring prompt will note:
> "Value not found in reference text. However, format is valid and LLM confidence was CERTAIN. Assigning lower confidence (0.65) - recommend manual verification."

### Issue 2: What if both LLM and Textract are wrong?
**Scenario**: Actual value is "1011207547" but both read "1011207S47"

**Solution**: Format validation catches this. The pattern `^\d{10}$` would reject "1011207S47" because S is not a digit. The scoring prompt would flag:
> "Value contains non-digit character. Format validation FAILED. Confidence: 0.30"

### Issue 3: What if the reference text is too sparse?
**Scenario**: Textract's 90% confidence filter removes too much text

**Solution**:
1. Make confidence threshold configurable (try 85% if 90% is too strict)
2. Include word count in reference - if below threshold, lower confidence or flag for review
3. Consider including medium-confidence words but marking them

### Issue 4: Page detection finds wrong pages
**Scenario**: LLM says "Page 3" but the info is actually on Page 4

**Solution**:
1. Detection prompt should err on the side of including more pages
2. If extraction fails, retry with all pages
3. Textract all pages up-front (cost is minimal) and create reference text for all

---

## Recommended Adjustments

Based on this analysis, I recommend these refinements:

### 1. Textract All Pages Up-Front
Instead of Textract only on "relevant" pages, run Textract on all pages during Step 1. This:
- Removes dependency on page detection being perfect
- Allows scoring prompt to search entire document if needed
- Cost difference is minimal ($0.0015/page)

### 2. Configurable Confidence Threshold for Textract
```python
TEXTRACT_MIN_CONFIDENCE = float(os.environ.get('TEXTRACT_MIN_CONFIDENCE', '90.0'))
```

### 3. Include "Candidate Values" in Reference Text
In addition to raw reference text, extract candidate values:
```python
{
  "reference_text": "full text...",
  "candidate_meter_ids": ["1011207547", "31665876"],  # 10-digit numbers found
  "candidate_account_numbers": ["4479518448"],
  "labels_found": ["Meter #", "Account No", "Service Information"]
}
```

This helps the scoring prompt reason: "LLM extracted 1011207547. This appears in candidate_meter_ids and is near 'Meter #' label."

---

## Final Refined Pipeline

```
STEP 1: INGESTION & PREP
├── Download PDF from S3
├── Convert ALL pages to images using fitz
└── Run Textract on ALL pages, create reference text (high-confidence only)

STEP 2: PAGE DETECTION (LLM Call #1)
├── Send all page images (thumbnails or full)
├── Prompt: "Which pages contain utility account/meter information?"
└── Output: relevant page numbers + utility classification

STEP 3: DATA EXTRACTION (LLM Call #2)
├── Send only relevant page images
├── Extract field values with reasoning
└── Output: extracted values + page locations + confidence indicators

STEP 4: CONFIDENCE SCORING (LLM Call #3)
├── Send: extracted values + reference text for relevant pages
├── Perform: cross-validation, format, context, consistency checks
├── Calculate: confidence scores with transparent breakdown
└── Output: final values (possibly corrected) + confidence scores

STEP 5: OUTPUT ROUTING
├── ≥95% → Salesforce (auto-accept)
├── 80-94% → Salesforce (flagged)
└── <80% → QSS review queue
```

---

## Questions for You

1. **Textract scope**: Should we run Textract on all pages up-front, or only on pages identified by the LLM?

2. **Confidence threshold**: Is 90% Textract confidence the right cutoff, or would 85% capture more useful data?

3. **Candidate value extraction**: Should we pre-extract all numbers matching expected patterns from reference text to help the scoring prompt?

Let me know if this refined approach makes sense, and I'll proceed with implementation!
