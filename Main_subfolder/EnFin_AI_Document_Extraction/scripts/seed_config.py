"""
Seed script for populating the DynamoDB config table with the
``m0_utility_bill`` process definition.

Usage:
    python scripts/seed_config.py --table TABLE_NAME --extraction-table EXTRACTION_TABLE \
        --region REGION [--sf-endpoint SF_ENDPOINT]

This creates one item in the config table with:
- PK: ``process = "m0_utility_bill"``
- Entity definitions with keywords, regex, location hints
- Updated extraction prompt template
- Conditional response rules
- Extraction table name (for file-level state + results)
- Salesforce endpoint (optional, per-process override)

Author: Reet Roy
Version: 2.0.0 (File-level processing with immediate SF push)
"""

import argparse
import json
import decimal
import boto3


def get_config_item() -> dict:
    """Build the complete m0_utility_bill process config item.

    Note: extraction_table and output_sqs_url are now global environment variables
    (DYNAMODB_EXTRACTION_TABLE and OUTPUT_SQS_QUEUE_URL) and not stored per-process.
    """
    return {
        "process": "m0_utility_bill",
        "description": "Utility bill extraction for VPP enrollment",
        "message_group_id": "m0-utility-extract",
        "prompt_arn": "arn:aws:bedrock:us-west-2:673522932440:prompt/SEXYCXGO3W:3",
        "ocr_engine": "docling",
        "textract_min_confidence": decimal.Decimal("50.0"),
        "extraction_prompt": _get_extraction_prompt(),
        "confidence_configuration": {
            "CERTAIN": decimal.Decimal("0.95"),
            "LIKELY": decimal.Decimal("0.85"),
            "UNCERTAIN": decimal.Decimal("0.70"),
            "NOT_FOUND": decimal.Decimal("0.0"),
        },
        "entities": {
            "state": {
                "identification": (
                    "US state to which the utility bill belongs. "
                    "Provide in 2 Letter short form"
                ),
                "location_hints": ["Service For", "similar address fields"],
                "expected_labels": [],
                "regex": "^[A-Z]{2}$",
                "keywords": ["Service For", "State"],
            },
            "utility_provider": {
                "identification": "Utility Provider company for the Bill",
                "location_hints": ["Header", "Company Logo", "Website"],
                "expected_labels": [],
                "regex": "",
                "keywords": [],
            },
            "account_number": {
                "identification": (
                    "10-12 digit number only, and if special characters "
                    "then include them as well"
                ),
                "expected_labels": [
                    "Account No",
                    "Account Number",
                    "Account #",
                    "Customer Account",
                ],
                "location_hints": [
                    "Top-right of page 1 or 2 near 'Account No:' or other "
                    "labels from the expected label in the header",
                    "Listed in the first page, labelled as "
                    "'Your Account Number', or 'Su numero de cuenta'",
                ],
                "regex": r"^[\d\s\-]{8,20}$",
                "keywords": [
                    "Account No",
                    "Account Number",
                    "Account #",
                    "Customer Account",
                    "Su numero de cuenta",
                ],
            },
            "service_account_id": {
                "identification": "usually 10 digit number",
                "expected_labels": [
                    "Service Agreement ID",
                    "Service Account",
                    "SA ID",
                ],
                "location_hints": [
                    "In the 'Details of Electric Charges' section, "
                    "labeled 'Service Account ID:'",
                    "In the first page, labeled 'Service Account'",
                    "In the second or third page, on the meter and "
                    "service information labelled as "
                    "'Premise Id or ID Localidad'",
                    "Meter and Service Information(INFORMACIÓN DEL MEDIDOR Y DEL SERVICIO)table "
                    "on the second or third page of the bill.",
                ],
                "regex": r"^\d{8,12}$",
                "keywords": [
                    "Service Agreement",
                    "SA ID",
                    "Service Account",
                    "Premise Id",
                    "ID Localidad",
                ],
            },
            "meter_id": {
                "identification": "usually 8-10 digit number",
                "expected_labels": [
                    "Meter #",
                    "Meter Number",
                    "Electric Meter",
                    "Electric Meter #",
                    "Service Information",
                    "Electric #",
                ],
                "location_hints": [
                    "Usually on page 3 or 4 in the 'Service Information' "
                    "or 'Service Details' section",
                    "In third page, labelled after 'meter'",
                    "In second Page, found after Meter Information, "
                    "labelled as 'Meter Number'",
                ],
                "regex": r"^\d{7,11}$",
                "keywords": [
                    "Meter #",
                    "Meter Number",
                    "Electric Meter",
                    "Medidor",
                ],
            },
            "esi_id": {
                "identification": "usually 17-22 digit number",
                "expected_labels": ["ESI ID", "Electric Service Identifier", "ESI #"],
                "location_hints": [
                    "In second page, under Meter usage details, above Meter number",
                ],
                "regex": r"^\d{17,22}$",
                "keywords": ["ESI ID", "Electric Service Identifier", "ESI #"],
            },
            "electric_choice_id": {
                "identification": "usually 8-10 digit number",
                "expected_labels": [
                    "Electric Choice ID",
                    "Choice ID",
                    "Service Agreement Id",
                ],
                "location_hints": [
                    "In second page, under Service Address box",
                ],
                "regex": r"^\d{8,12}$",
                "keywords": ["Electric Choice ID", "Choice ID", "Service Agreement Id"],
            },
            "delivery_rate_classification": {
                "identification": (
                    "determined based on building type "
                    "(single-family vs multi-family, with or without "
                    "space heating) and should not change."
                    " Example : Residential-Single"
                ),
                "expected_labels": ["Delivery Rate Classification"],
                "location_hints": [
                    "In second Page, found under charge details",
                ],
                "regex": "",
                "keywords": ["Delivery Rate Classification"],
            },
            "customer_of_records": {
                "identification": (
                    "individual or entity whose name appears on the "
                    "utility bill and is legally responsible for the account"
                ),
                "expected_labels": ["Customer Of Record"],
                "location_hints": ["In first Page of the bill"],
                "regex": "",
                "keywords": ["Customer Of Record", "Nombre del Cliente"],
            },
        },
        "aggregation_config": {
            "enabled": True,
            "conflict_threshold": decimal.Decimal("0.05"),
            "submission_window_minutes": 30,
            "high_confidence_threshold": decimal.Decimal("0.90"),
            "medium_confidence_threshold": decimal.Decimal("0.80"),
            "field_mappings": {
                "account_number": "Account_Number__c",
                "service_account_id": "Service_Account_ID__c",
                "meter_id": "Meter_ID__c",
                "utility_provider": "Utility_Provider__c",
                "esi_id": "ESI_ID__c",
                "electric_choice_id": "Electric_Choice_ID__c",
                "delivery_rate_classification": "Delivery_Rate_Classification__c",
                "customer_of_records": "Customer_Of_Record__c",
                "state": "State__c",
            },
            "program_lookup_enabled": True,
            "program_configs": {
                "CA_ELRP": {
                    "states": ["CA"],
                    "utilities": ["PG&E", "SCE", "SDG&E"],
                    "program": "ELRP",
                    "mandatory_fields": [
                        "account_number",
                        "meter_id",
                        "utility_provider",
                        "state",
                    ],
                },
                "IL_MISO": {
                    "states": ["IL"],
                    "utilities": ["AMEREN"],
                    "program": "MISO",
                    "mandatory_fields": [
                        "account_number",
                        "meter_id",
                        "utility_provider",
                        "state",
                    ],
                },
                "IL_PJM": {
                    "states": ["IL"],
                    "utilities": ["COMED"],
                    "program": "PJM",
                    "mandatory_fields": [
                        "account_number",
                        "electric_choice_id",
                        "utility_provider",
                        "state",
                    ],
                },
                "TX_ERS": {
                    "states": ["TX"],
                    "utilities": ["ANY"],
                    "program": "ERS",
                    "mandatory_fields": [
                        "account_number",
                        "esi_id",
                        "utility_provider",
                        "state",
                    ],
                },
                "PR_CBES": {
                    "states": ["PR"],
                    "utilities": ["LUMA"],
                    "program": "CBES",
                    "mandatory_fields": [
                        "account_number",
                        "meter_id",
                        "utility_provider",
                        "state",
                    ],
                },
            },
        },
        "conditional_responses": [
            {
                "conditions": {"utility_name": "PG&E", "state": "CA"},
                "additional_fields": {"program": "ELRP"},
            },
            {
                "conditions": {"utility_name": "SCE", "state": "CA"},
                "additional_fields": {"program": "ELRP"},
            },
        ],
    }


def _get_extraction_prompt() -> str:
    """Return the full extraction prompt template with merge fields."""
    return (
        "The page images of the utility bill document are provided above."
        "\n"
        "## DOCUMENT TEXT (Extracted by Docling - format-preserving, "
        "high accuracy for text/numbers): {{docling_text}}"
        "\n"
        "<persona>\n"
        "You are an expert OCR extractor, capable of extracting "
        "information from pdf, images clearly and distinctly.\n"
        "</persona>\n"
        "\n"
        "<goal>\n"
        "Your goal is to analyze the document, check whether it is a "
        "utility bill or not, if utility bill then provide as much "
        "information as you got from the document, as per the <output>.\n"
        "</goal>\n"
        "\n"
        "<task>\n"
        "You are analyzing a utility bill document. You have TWO "
        "sources of information:\n"
        "1. **Page Images** (above) - Use these to detect layout, "
        "identify sections, logos, and locate where each field is "
        "on the page and visual reading for exact digits and characters.\n"
        "2. **Docling Text** (above) - Use this for the exact text values. "
        "Docling text is MORE RELIABLE than visual reading for exact digits and characters.\n\n"
        "You can be provided with other language utility bill "
        "also (e.g. Spanish), you have to decode the language, and "
        "understand the meaning of that and properly fill the value "
        "with that of our requirement.\n"
        "</task>\n"
        "\n"
        "<information>\n"
        "Utility Bill is a monthly invoice or statement outlining "
        "charges for essential household services. This could include "
        "water bill, electricity bill, gas bill, or any document which "
        "is provided by public utility companies.\n"
        "</information>\n"
        "\n"
        "<guardrails>\n"
        "- If for any field you are not able to find the value exactly, "
        "then leave that field as empty\n"
        "- Do not create any kind of data by yourself, ONLY provide "
        "what is there in the document\n"
        "</guardrails>\n"
        "\n"
        "<steps_to_follow>\n"
        "## STEP 1: IDENTIFY THE UTILITY PROVIDER\n"
        "Examine the page images for logos, headers, and contact "
        "information.\n"
        "\n"
        "### KNOWN UTILITIES:\n"
        "- **California**: PG&E (Pacific Gas & Electric), SCE "
        "(Southern California Edison), SDG&E (San Diego Gas & Electric)\n"
        "- **Illinois**: ComED (Commonwealth Edison), Ameren Illinois\n"
        "- **Luma**: LUMA energy\n"
        "\n"
        "If the document is NOT a utility bill, set utility_provider "
        'to "Not_Utility_Bill" or the utility name and return empty '
        "fields.\n"
        "\n"
        "## STEP 2: EXTRACT REQUIRED FIELDS\n"
        "Based on the identified utility, extract the required fields.\n"
        "\n"
        "{field_details}\n"
        "\n"
        "## EXTRACTION PROCESS:\n"
        "For EACH required field:\n"
        "1. **Visual Location**: Use the page images to identify WHERE "
        "the field is (which page, which section, near which label)\n"
        "2. **Return value**: Return the required field values by cross-referencing with the document text\n"
        "</steps_to_follow>\n"
        "\n"
        "**Field-by-Field Extraction**:\n"
        "For each field:\n"
        "1. Visual: I see [field label] on page [N] in section "
        "[section]. The value appears to be [visual_value].\n"
        "2. Keywords: Any keywords present near which confirms the value\n"
        "3. Docling: Searching Docling text near [label]... Found: [docling_value]\n"
        "4. Cross-check: Visual=[visual_value], Docling=[docling_value] -> [match/mismatch]\n"
        "5. Final value: [value](source: docling/visual)\n"
        "6. Assess if final value matches description of the field and proceed to Confidence Assessment\n"
        "\n"
        "**Confidence Assessment**:\n"
        "- CERTAIN: Value found in both docling_text and document image confidently "
        "along with keywords which "
        "confirms it and is consistent/ same across docling_text and document image\n"
        "- LIKELY: Value found but not consistent across docling_text and document image "
        "but matching keywords/description and format\n"
        "\n"
        "- UNCERTAIN: Value found but not consistent across docling_text and not confident "
        "based on keywords/description and format\n"
        "or format\n"
        "- NOT_FOUND: Could not locate value\n"
        "</reasoning>\n"
        "\n"
        "<output>\n"
        "{\n"
        '  "is_supported_document": true|false,\n'
        '  "no_relevant_pages_reason": null,\n'
        '  "fields": [\n'
        '    {\n'
        '      "name": "field_name",\n'
        '      "value": "extracted_value_or_null",\n'
        '      "page": page_number,\n'
        '      "section": "section/label where found",\n'
        '      "confidence": "CERTAIN|LIKELY|UNCERTAIN|NOT_FOUND",\n'
        '      "reasoning": "brief explanation"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "</output>"
    )


def main():
    """Parse arguments and seed the config table."""
    parser = argparse.ArgumentParser(
        description="Seed DynamoDB config table with process definitions"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="DynamoDB config table name",
    )
    parser.add_argument(
        "--region",
        default="us-west-2",
        help="AWS region (default: us-west-2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the item JSON without writing to DynamoDB",
    )
    args = parser.parse_args()

    item = get_config_item()

    if args.dry_run:
        print(json.dumps(item, indent=2))
        return

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table)

    table.put_item(Item=item)
    print(
        f"Successfully seeded config table '{args.table}' with "
        f"process='{item['process']}'"
    )


if __name__ == "__main__":
    main()
