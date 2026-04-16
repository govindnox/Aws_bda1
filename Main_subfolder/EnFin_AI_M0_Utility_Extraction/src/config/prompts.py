"""
Prompt templates for utility bill extraction pipeline.

Single extraction prompt using Docling text + page images for multimodal extraction.
Images are provided as content blocks before the text prompt (Bedrock Converse API).
"""

# =============================================================================
# EXTRACTION PROMPT (Single LLM Call)
# Purpose: Identify utility provider + extract field values using images for layout
#          and Docling text for accurate value extraction
# Model: Llama 4 Maverick via Bedrock Converse API
# Input: [page images as content blocks] + [this prompt with Docling text embedded]
# =============================================================================

EXTRACTION_PROMPT = """The page images of the utility bill document are provided above.

## DOCUMENT TEXT (Extracted by Docling - format-preserving, high accuracy for text/numbers):
{docling_text}

## YOUR TASK:
You are analyzing a utility bill document. You have TWO sources of information:
1. **Page Images** (above) - Use these to detect layout, identify sections, logos, and locate where each field is on the page
2. **Docling Text** (above) - Use this for the exact text values. Docling text is MORE RELIABLE than visual reading for exact digits and characters.

## STEP 1: IDENTIFY THE UTILITY PROVIDER
Examine the page images for logos, headers, and contact information. Also check the Docling text for company names.

### KNOWN UTILITIES:
- **California**: PG&E (Pacific Gas & Electric), SCE (Southern California Edison), SDG&E (San Diego Gas & Electric)
- **Illinois**: ComED (Commonwealth Edison), Ameren Illinois

If the document is NOT a utility bill or is from an unsupported utility, set utility_provider to "Not_Utility_Bill" or the utility name and return empty fields.

## STEP 2: EXTRACT REQUIRED FIELDS
Based on the identified utility, extract the required fields.

{utility_fields_guide}

## EXTRACTION PROCESS:
For EACH required field:
1. **Visual Location**: Use the page images to identify WHERE the field is (which page, which section, near which label)
2. **Value from Docling**: Find the corresponding value in the Docling text (search near the expected labels)
3. **Cross-check**: Confirm the Docling text value matches what you see in the image
4. **Return Docling value**: Always prefer the Docling text value (more reliable for exact digits)

### UTILITY-SPECIFIC GUIDE:
- **PG&E**: Account No is top-right of page 1 (can have check digit like 4479518448-8). Service Agreement ID (10 digits) and Meter # (10 digits) are on page 3 or 4 in 'Service Information' or 'Details of Electric Charges'.
- **SCE**: Customer account (12 digits) and Service account (10 digits) are top-left of page 1. Smart Meter number (starts with 222) is on page 3 under 'Your past and current electricity usage'.
- **SDG&E**: Account Number (has spaces like 0090 5273 6230 2) is at the top of page 3. Meter Number (8 digits, may start with 0) is on page 3 in 'Detail of Current Charges'.
- **ComEd**: Account Number (10 digits) is at the top. Electric Choice ID (10 digits) is in 'SERVICE ADDRESS' box. Meter Number (8-9 digits) is in 'METER INFORMATION' table.
- **Ameren**: Account Number (10 digits) is top-left of page 1. Meter Number (8-9 digits) is on page 3 in 'Electric Service Residential Billing Detail' table.

<reasoning>
**Utility Identification**:
- What logo/company name do I see in the images?
- What company name appears in the Docling text?
- What state indicators are present?
- Is this a supported utility?

**Field-by-Field Extraction**:
For each field:
1. Visual: I see [field label] on page [N] in section [section]. The value appears to be [visual_value].
2. Docling: Searching Docling text near "[label]"... Found: [docling_value]
3. Cross-check: Visual=[visual_value], Docling=[docling_value] → [match/mismatch]
4. Final value: [value] (source: docling/visual)

**Confidence Assessment**:
- CERTAIN: Value found in both image AND Docling text, they match
- LIKELY: Value found in Docling text near expected label, image confirms location
- UNCERTAIN: Value found but cross-check is inconclusive
- NOT_FOUND: Could not locate the field in either source
</reasoning>

<output>
{{
  "utility_provider": "PG&E|SCE|SDG&E|ComED|Ameren|<Other>|Not_Utility_Bill",
  "state": "CA|IL|<Other>|Unknown",
  "is_supported_document": true|false,
  "no_relevant_pages_reason": null,
  "fields": {{
    "field_name": {{
      "value": "extracted_value_or_null",
      "page": page_number,
      "section": "section/label where found",
      "confidence": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND",
      "reasoning": "brief explanation of how value was found and cross-checked"
    }}
  }}
}}
</output>"""


# =============================================================================
# HELPER: Field Configuration by Utility
# =============================================================================

FIELD_CONFIGURATIONS = {
    "PG&E": {
        "state": "CA",
        "program": "ELRP",
        "fields": ["account_number", "service_agreement_id", "meter_id"],
        "field_details": {
            "account_number": {
                "expected_labels": ["Account No", "Account Number", "Account #"],
                "format_pattern": r"^\d{10}-?\d?$",
                "format_description": "10-digit number, optionally with check digit (e.g. 4479518448-8)",
                "location_hints": "Top-right of page 1, near 'Account No:' label in the header"
            },
            "service_agreement_id": {
                "expected_labels": ["Service Agreement ID", "Service Agreement", "SA ID"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "In the 'Details of Electric Charges' section, labeled 'Service Agreement ID:'"
            },
            "meter_id": {
                "expected_labels": ["Meter #", "Meter Number", "Electric Meter", "Electric Meter #", "Service Information"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "Usually on page 3 or 4 in the 'Service Information' or 'Service Details' section"
            }
        }
    },
    "SCE": {
        "state": "CA",
        "program": "ELRP",
        "fields": ["account_number", "service_agreement_id", "meter_id"],
        "field_details": {
            "account_number": {
                "expected_labels": ["Customer account", "Account Number"],
                "format_pattern": r"^\d{10,12}$",
                "format_description": "10-12 digit number (usually 12 digits)",
                "location_hints": "Top-left of page 1, under the SCE logo, labeled 'Customer account'"
            },
            "service_agreement_id": {
                "expected_labels": ["Service account", "Service Agreement"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "Top-left of page 1, under the Customer account, labeled 'Service account'"
            },
            "meter_id": {
                "expected_labels": ["Meter", "Smart Meter", "Meter Number"],
                "format_pattern": r"^222\d{7,11}$",
                "format_description": "10-14 digits starting with 222",
                "location_hints": "Page 3, in 'Your past and current electricity usage' section"
            }
        }
    },
    "SDG&E": {
        "state": "CA",
        "program": "ELRP",
        "fields": ["account_number", "meter_id"],
        "field_details": {
            "account_number": {
                "expected_labels": ["ACCOUNT NUMBER", "Account #"],
                "format_pattern": r"^\d{10,16}$",
                "format_description": "10-16 digit number (usually groups of digits with spaces like '0090 5273 6230 2'. Strip spaces for validation)",
                "location_hints": "Top of page 3, labeled 'ACCOUNT NUMBER'"
            },
            "meter_id": {
                "expected_labels": ["Meter Number", "Meter No", "Electric Service"],
                "format_pattern": r"^\d{8}$",
                "format_description": "8-digit number (often starts with 0 - preserve leading zeros)",
                "location_hints": "Page 3, 'Detail of Current Charges' under 'Electric Service'"
            }
        }
    },
    "ComED": {
        "state": "IL",
        "program": "PJM",
        "fields": ["account_number", "electric_choice_id", "meter_number"],
        "field_details": {
            "account_number": {
                "expected_labels": ["Account #", "Account Number", "Account"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "Top of page 1 or 2 in the header area"
            },
            "electric_choice_id": {
                "expected_labels": ["Electric Choice ID", "Choice ID", "ECID", "Service Agreement"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "Page 2 in the 'SERVICE ADDRESS' box"
            },
            "meter_number": {
                "expected_labels": ["Meter Number", "Meter", "Meter No", "METER INFORMATION"],
                "format_pattern": r"^\d{8,9}$",
                "format_description": "8-9 digit number",
                "location_hints": "Page 2 in the 'METER INFORMATION' table"
            }
        }
    },
    "Ameren": {
        "state": "IL",
        "program": "MISO",
        "fields": ["account_number", "meter_number"],
        "field_details": {
            "account_number": {
                "expected_labels": ["Account Number", "Account No", "Número de cuenta"],
                "format_pattern": r"^\d{10}$",
                "format_description": "10-digit number",
                "location_hints": "Top left of page 1, in customer information section"
            },
            "meter_number": {
                "expected_labels": ["METER NUMBER", "Meter Number", "Meter"],
                "format_pattern": r"^\d{8,9}$",
                "format_description": "8-9 digit number",
                "location_hints": "Page 3 in 'Electric Meter Read' table"
            }
        }
    }
}


def get_utility_fields_guide() -> str:
    """Generate a comprehensive guide of all utility fields for the extraction prompt."""
    lines = ["### FIELDS BY UTILITY:"]

    for utility, config in FIELD_CONFIGURATIONS.items():
        lines.append(f"\n**{utility}** ({config['state']}, {config['program']}):")
        for field_name in config["fields"]:
            details = config["field_details"][field_name]
            lines.append(f"- **{field_name}**: {details['format_description']}")
            lines.append(f"  - Labels: {', '.join(details['expected_labels'])}")
            lines.append(f"  - Location: {details['location_hints']}")

    lines.append("\nIf the utility is unknown, extract ALL fields you can find:")
    lines.append("- meter_id, account_number, service_agreement_id, electric_choice_id, meter_number")

    return "\n".join(lines)


def get_fields_to_extract_prompt(utility: str) -> str:
    """Generate the fields to extract section for a given utility"""
    if utility not in FIELD_CONFIGURATIONS:
        # Unknown utility - extract all possible fields
        return """
- **meter_id**: Meter ID/Number (CA utilities)
- **account_number**: Account Number (all utilities)
- **electric_choice_id**: Electric Choice ID (IL ComED only)
- **meter_number**: Meter Number (IL utilities)

Note: Extract ALL fields you can find. We'll determine which are applicable based on the utility.
"""

    config = FIELD_CONFIGURATIONS[utility]
    lines = []
    for field in config["fields"]:
        details = config["field_details"][field]
        lines.append(f"- **{field}**: {details['format_description']}")
        lines.append(f"  - Expected labels: {', '.join(details['expected_labels'])}")
        lines.append(f"  - Location hint: {details['location_hints']}")
        lines.append("")

    return "\n".join(lines)


def get_format_rules_prompt(utility: str) -> str:
    """Generate format rules section for confidence scoring"""
    if utility not in FIELD_CONFIGURATIONS:
        return """
- meter_id: 8-10 digits (exact pattern depends on utility)
- account_number: 10 digits
- electric_choice_id: 10 digits
- meter_number: 8 digits
"""

    config = FIELD_CONFIGURATIONS[utility]
    lines = []
    for field in config["fields"]:
        details = config["field_details"][field]
        lines.append(f"- {field}: {details['format_description']} (pattern: {details['format_pattern']})")

    return "\n".join(lines)


def get_label_rules_prompt(utility: str) -> str:
    """Generate label rules section for confidence scoring"""
    if utility not in FIELD_CONFIGURATIONS:
        return """
- meter_id: Look for "Meter #", "Meter Number", "Service Information"
- account_number: Look for "Account #", "Account Number"
- electric_choice_id: Look for "Electric Choice ID", "Choice ID"
- meter_number: Look for "Meter Number", "METER INFORMATION"
"""

    config = FIELD_CONFIGURATIONS[utility]
    lines = []
    for field in config["fields"]:
        details = config["field_details"][field]
        labels = ", ".join([f'"{l}"' for l in details["expected_labels"]])
        lines.append(f"- {field}: Look for {labels}")

    return "\n".join(lines)
