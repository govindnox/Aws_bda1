# Final Architecture Plan: Utility Bill Data Extraction System

## Critical Review of Your Suggestions

### 1. Moving Textract Before LLM#2 - Analysis

**Your suggestion**: Run Textract on relevant pages, pass structured text to LLM#2 along with images.

**Critical thoughts**:

**PROS**:
- LLM can cross-reference visual understanding with OCR text
- Reduces chance of digit misreading (LLM sees both image AND text)
- Using Llama 4 Scout for text extraction is appropriate (smaller, faster model for simpler task)

**CONS/CONSIDERATIONS**:
- **Timing dependency**: We need LLM#1 to identify pages BEFORE running Textract
- **Format preservation**: Textract output needs careful handling to maintain spatial relationships

**RECOMMENDED ADJUSTMENT**:
Instead of running Textract BEFORE LLM#2, I suggest:
1. Run LLM#1 (page detection)
2. In PARALLEL: Send relevant pages to both Textract AND LLM#2
3. LLM#3 (confidence scoring) receives both LLM#2 extraction AND Textract reference

**Why parallel?** Saves time - Textract and LLM#2 can run simultaneously.

BUT - if you specifically want LLM#2 to SEE the Textract text while extracting, then sequential is correct.

---

### 2. Configurable Retry Loop - Analysis

**Your suggestion**: When LLM#3 produces low confidence for any field, lock high-confidence values and retry LLM#2+#3.

**Critical thoughts**:

**PROS**:
- Gives system another chance to extract difficult fields
- Locked values prevent regressions
- Could focus attention on specific fields in retry

**CONS/CONSIDERATIONS**:
- **Infinite loop risk**: Need max retry limit
- **Cost**: Each retry = 2 more LLM calls
- **Diminishing returns**: If extraction failed once, why would it succeed on retry?
- **What changes on retry?**: Need to modify prompt/approach for retry to be useful

**RECOMMENDED APPROACH**:
The retry should be DIFFERENT from the first attempt. Options:

**Option A: Focused Retry**
```
Retry prompt: "You previously extracted field X with low confidence.
              Focus specifically on finding X.
              Already confirmed values: {locked_values}
              Reference text: {textract_text}
              Look for these patterns: {specific_patterns_for_X}"
```

**Option B: Alternative Extraction Strategy**
- First attempt: Vision-based extraction
- Retry: Pure text-based extraction from Textract output
- Compare and reconcile

**Option C: Human-in-the-Loop Hint**
- If retry still fails, flag for QSS with specific question
- "Could not find Electric Choice ID. Is it on page 2 near Service Address?"

**RECOMMENDATION**: Option A (Focused Retry) with max 2 retries.

---

### 3. Passing Text Without Changing Format

**Your ask**: Tools that pass text to LLM without changing format for extraction.

**The challenge**: Textract returns structured data (blocks with bounding boxes), not plain text. To preserve format, we need to reconstruct the layout.

**Options**:

**Option 1: Line-by-Line Reconstruction**
```python
def reconstruct_text_layout(textract_response):
    """
    Reconstruct text preserving line structure
    """
    lines = []
    for block in textract_response['Blocks']:
        if block['BlockType'] == 'LINE':
            lines.append({
                'text': block['Text'],
                'confidence': block['Confidence'],
                'top': block['Geometry']['BoundingBox']['Top']
            })

    # Sort by vertical position
    lines.sort(key=lambda x: x['top'])

    # Filter by confidence and join
    return '\n'.join([l['text'] for l in lines if l['confidence'] >= threshold])
```

**Option 2: Table-Aware Reconstruction**
For utility bills with tables (meter info, charges), use Textract's TABLE and CELL blocks:
```python
def extract_tables_as_markdown(textract_response):
    """
    Convert Textract tables to markdown format
    """
    # Preserves structure: | Col1 | Col2 | Col3 |
```

**Option 3: Key-Value Pair Extraction**
Textract's FORMS feature extracts key-value pairs:
```python
# Returns: {"Account Number": "1234567890", "Meter #": "1011207547"}
```

**RECOMMENDATION**: Use a combination:
- Key-Value pairs for labeled fields (most useful for extraction)
- Line-by-line for context (nearby text)
- Table format for structured sections (meter information table)

---

## Final Revised Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              FINAL EXTRACTION PIPELINE                                   │
│                                    (with retry loop)                                     │
└─────────────────────────────────────────────────────────────────────────────────────────┘

STEP 1: DOCUMENT INGESTION
├── 1.1 Receive SQS message → Download PDF from S3
├── 1.2 Use PyMuPDF (fitz) to convert ALL pages to images
└── 1.3 Store page images in memory/temp

STEP 2: PAGE DETECTION (LLM Call #1 - Llama 4 Maverick)
├── 2.1 Send ALL page images (or thumbnails) to LLM
├── 2.2 Prompt: "Which pages contain utility account/meter information?"
├── 2.3 Also identify: utility provider, state
└── 2.4 Output:
        {
          "utility": "PG&E",
          "state": "CA",
          "relevant_pages": [3, 4]
        }

STEP 3: TEXTRACT PROCESSING (Parallel with extraction prep)
├── 3.1 Send ONLY relevant pages (from Step 2) to Textract
├── 3.2 Use AnalyzeDocument with FORMS, TABLES features
├── 3.3 For each page, generate:
│       ├── structured_text: Line-by-line text (high confidence)
│       ├── key_value_pairs: {"Label": "Value"} extracted
│       ├── tables: Markdown-formatted table content
│       └── candidate_values: (if enabled) Numbers matching patterns
└── 3.4 Combine into reference_text per page

STEP 4: DATA EXTRACTION (LLM Call #2 - Llama 4 Scout)
├── 4.1 Send to LLM:
│       ├── Relevant page IMAGES (from Step 2)
│       ├── Reference TEXT from Textract (from Step 3)
│       ├── Utility context (provider, state, program)
│       └── Fields to extract with expected formats
├── 4.2 Prompt structure:
│       """
│       Page 2 Image: [image]
│       Page 2 Reference Text:
│       {textract_structured_text_page_2}
│       Key-Value Pairs Found: {key_value_pairs_page_2}
│
│       Page 3 Image: [image]
│       Page 3 Reference Text:
│       {textract_structured_text_page_3}
│       Key-Value Pairs Found: {key_value_pairs_page_3}
│
│       Extract these fields: {fields_list}
│       """
├── 4.3 LLM extracts values using BOTH image and text reference
└── 4.4 Output:
        {
          "meter_id": {"value": "1011207547", "page": 3, "confidence": "CERTAIN"},
          "account_number": {"value": "4479518448", "page": 1, "confidence": "LIKELY"}
        }

STEP 5: CONFIDENCE SCORING (LLM Call #3 - Llama 4 Maverick)
├── 5.1 Send to LLM:
│       ├── Extracted values from Step 4
│       ├── Reference text from Step 3
│       ├── Validation rules (format patterns, expected labels)
│       └── Already locked values (if retry)
├── 5.2 LLM performs validation:
│       ├── Cross-validation: Is extracted value in reference text?
│       ├── Format validation: Does it match expected pattern?
│       ├── Context validation: Is it near expected label?
│       └── Consistency check: Do values make sense together?
├── 5.3 Output:
│       {
│         "meter_id": {
│           "value": "1011207547",
│           "confidence_score": 0.96,
│           "validation_passed": true
│         },
│         "account_number": {
│           "value": "4479518448",
│           "confidence_score": 0.72,    // LOW!
│           "validation_notes": "Value not found in reference text"
│         }
│       }
└── 5.4 Check for low confidence fields

STEP 6: RETRY LOOP (Conditional - if low confidence detected)
├── 6.1 If ANY field has confidence < RETRY_THRESHOLD (e.g., 0.80):
│       ├── Lock HIGH confidence fields (≥0.95)
│       ├── Identify LOW confidence fields for retry
│       └── Increment retry counter
├── 6.2 If retry_count < MAX_RETRIES (default: 2):
│       ├── Create FOCUSED retry prompt for low-confidence fields
│       ├── Include locked values as context
│       ├── Include specific guidance for the failed field
│       └── Go back to Step 4 (extraction) with focused prompt
├── 6.3 If retry_count >= MAX_RETRIES:
│       └── Accept current results, flag for manual review
└── 6.4 Continue to Step 7

STEP 7: OUTPUT ROUTING
├── 7.1 Calculate overall confidence (weighted average)
├── 7.2 Route based on confidence:
│       ├── ≥95% all fields: Auto-accept to Salesforce
│       ├── ≥80% all fields: Accept with verification flag
│       └── <80% any field (after retries): Route to QSS review
└── 7.3 Store extraction results + metadata
```

---

## Retry Loop Detail

```python
class ExtractionPipeline:
    def __init__(self, config):
        self.max_retries = config.get('MAX_RETRIES', 2)
        self.retry_threshold = config.get('RETRY_THRESHOLD', 0.80)
        self.lock_threshold = config.get('LOCK_THRESHOLD', 0.95)

    def extract_with_retry(self, document):
        # Step 1-3: Ingestion, page detection, Textract
        pages, textract_results = self.prepare_document(document)

        locked_values = {}
        retry_count = 0

        while retry_count <= self.max_retries:
            # Step 4: Extraction
            if retry_count == 0:
                # First attempt: full extraction
                extraction = self.extract_all_fields(pages, textract_results)
            else:
                # Retry: focused extraction for low-confidence fields
                extraction = self.extract_focused(
                    pages,
                    textract_results,
                    locked_values,
                    low_confidence_fields
                )

            # Step 5: Confidence scoring
            scored_results = self.score_confidence(
                extraction,
                textract_results,
                locked_values
            )

            # Check results
            low_confidence_fields = self.get_low_confidence_fields(
                scored_results,
                self.retry_threshold
            )

            if not low_confidence_fields:
                # All fields have acceptable confidence
                return scored_results

            # Lock high-confidence values for next retry
            new_locks = self.get_high_confidence_fields(
                scored_results,
                self.lock_threshold
            )
            locked_values.update(new_locks)

            retry_count += 1

            if retry_count > self.max_retries:
                # Max retries reached, return with flags
                return self.finalize_with_review_flags(scored_results)

        return scored_results
```

### Focused Retry Prompt

```python
FOCUSED_RETRY_PROMPT = """
You are re-attempting extraction for specific fields that had low confidence.

## ALREADY CONFIRMED VALUES (DO NOT CHANGE):
{locked_values_json}

## FIELDS NEEDING RE-EXTRACTION:
{low_confidence_fields}

## GUIDANCE FOR EACH FIELD:

{field_name}:
- Expected format: {expected_format}
- Expected location: {expected_location}
- Look for labels: {expected_labels}
- Previous attempt found: {previous_value} (confidence: {previous_confidence})
- Issue: {validation_notes}

## PAGE IMAGES AND REFERENCE TEXT:
[Same as before]

FOCUS specifically on finding the fields listed above. The other fields have already been
confirmed with high confidence.

<extraction>
{
  "field_name": {
    "value": "...",
    "page": X,
    "confidence": "CERTAIN|LIKELY|UNCERTAIN",
    "extraction_notes": "explain where you found it or why you couldn't"
  }
}
</extraction>
"""
```

---

## Configuration Options

```python
# Environment variables / config file
EXTRACTION_CONFIG = {
    # Textract settings
    "TEXTRACT_MIN_CONFIDENCE": 90.0,  # Configurable as requested
    "TEXTRACT_INCLUDE_TABLES": True,
    "TEXTRACT_INCLUDE_FORMS": True,

    # Candidate extraction (configurable as requested)
    "EXTRACT_CANDIDATE_VALUES": True,  # Enable/disable
    "CANDIDATE_PATTERNS": {
        "meter_id_10digit": r"\d{10}",
        "meter_id_8digit": r"\d{8}",
        "sce_meter": r"222\d{7,11}",
        # Add more as needed
    },

    # Retry settings
    "MAX_RETRIES": 2,
    "RETRY_THRESHOLD": 0.80,  # Retry if any field below this
    "LOCK_THRESHOLD": 0.95,   # Lock values above this

    # Confidence thresholds
    "AUTO_ACCEPT_THRESHOLD": 0.95,
    "FLAG_THRESHOLD": 0.80,

    # Model selection
    "PAGE_DETECTION_MODEL": "meta.llama4-maverick-17b-instruct-v1:0",
    "EXTRACTION_MODEL": "meta.llama4-scout-17b-instruct-v1:0",  # Faster for text extraction
    "SCORING_MODEL": "meta.llama4-maverick-17b-instruct-v1:0",
}
```

---

## Critical Questions Remaining

### 1. Llama 4 Scout Availability
I referenced "Llama 4 Scout" for text extraction (LLM#2). Need to verify:
- Is this model available in your Bedrock region?
- What's the model ID?
- If not available, we can use Maverick for all calls (just slightly more expensive)

### 2. Parallel vs Sequential Processing
**Current plan**: Textract runs BEFORE LLM#2 so we can pass reference text to extraction.
**Alternative**: Run Textract and LLM#2 in parallel, combine in LLM#3.

Which do you prefer? The sequential approach (Textract → LLM#2) means:
- LLM#2 takes longer (waits for Textract)
- But LLM#2 has more information to work with

### 3. What Happens When Utility Can't Be Identified?
If LLM#1 returns "Unknown" for utility:
- Option A: Extract all possible fields (union of all utilities)
- Option B: Flag for manual review immediately
- Option C: Try to infer from state (CA → try all 3 CA utilities)

### 4. Multi-Meter Accounts
Some commercial accounts have multiple meters. How to handle?
- Option A: Extract first/primary meter only
- Option B: Extract all meters as array
- Option C: Flag for manual review if >1 meter detected

---

## Summary

The refined pipeline is:

```
PDF → Images → LLM#1 (page detection) → Textract (relevant pages)
    → LLM#2 (extraction with text reference) → LLM#3 (confidence scoring)
    → [Retry loop if low confidence] → Output
```

**Key features**:
1. Textract runs BEFORE extraction, reference text passed to LLM#2
2. Configurable confidence thresholds for Textract filtering
3. Configurable candidate value extraction (pattern matching)
4. Retry loop with value locking for low-confidence fields
5. Llama 4 Scout for text extraction (faster/cheaper)
6. Llama 4 Maverick for page detection and scoring (more capable)

Ready to proceed to implementation when you confirm the remaining questions!
