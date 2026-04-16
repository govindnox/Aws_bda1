"""
Salesforce service for posting extraction results via Apex REST endpoint.

Uses the same pattern as EnFin_SN_Scheduled_Salesforce_Update/salesforce_handler.py:
- TokenManager Lambda layer for authentication
- POST to Apex REST endpoint with JSON payload
"""

import json
import logging
import requests
import traceback
from typing import Dict, Any, Optional

from token_manager import TokenManager
from config.settings import settings

logger = logging.getLogger(__name__)


class SalesforceService:
    """Service for posting extraction results to Salesforce via Apex REST."""

    def _get_access_token(self) -> str:
        """
        Fetch access token using TokenManager layer.

        Returns:
            string: Salesforce access token
        """
        session_object = {
            'token_table_name': settings.SF_TOKEN_TABLE_NAME,
            'host': settings.SF_HOST,
            'auth_path': settings.SF_AUTH_PATH,
            'diff_time': settings.SF_TOKEN_DIFF_TIME,
            'contact_center_username': settings.SF_USERNAME,
            'region_name': settings.REGION,
            'secret_name': settings.SF_SECRET_NAME
        }
        token_manager = TokenManager(session_object)
        token = token_manager.get_access_token()
        return token

    def post_extraction_result(
        self,
        app_no: str,
        extraction_result
    ) -> Dict[str, Any]:
        """
        Post extraction results to Salesforce Apex REST endpoint.

        Follows the same pattern as salesforce_handler.update_records():
        POST to HOST + APEX_PATH with Bearer token and JSON payload.

        Args:
            app_no: Application number (parentAppNumber)
            extraction_result: ExtractionResult from the pipeline

        Returns:
            Dict with success status and response details
        """
        try:
            token = self._get_access_token()

            # Build Salesforce Apex REST URL
            salesforce_url = f"{settings.SF_HOST}{settings.SF_APEX_PATH}"

            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            # Build payload matching Apex REST endpoint format
            payload = self._build_payload(app_no, extraction_result)
            payload_json = json.dumps(payload)

            logger.info(f"Pushing extraction result to Salesforce: {salesforce_url}")

            response = requests.post(
                salesforce_url,
                headers=headers,
                data=payload_json,
                timeout=30
            )

            logger.info(f"Salesforce response status: {response.status_code}")

            if response.status_code == 200:
                logger.info("Successfully pushed extraction data to Salesforce")
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return {'success': True, 'response': response.text}

            return {
                'success': False,
                'error': f"Salesforce API returned status {response.status_code}",
                'details': response.text
            }

        except Exception as e:
            logger.error(f"Error posting to Salesforce: {traceback.format_exc()}")
            return {
                'success': False,
                'error': 'Unexpected error',
                'details': str(e)
            }

    def _build_payload(self, app_no: str, extraction_result) -> Dict[str, Any]:
        """
        Build the Apex REST payload from extraction result.

        Args:
            app_no: Application number
            extraction_result: ExtractionResult from pipeline

        Returns:
            Payload dict for Salesforce Apex REST endpoint
        """
        # Build fields with values and confidence
        fields_data = {}
        for field_name, field_data in extraction_result.fields.items():
            fields_data[field_name] = {
                "value": field_data.get("value"),
                "confidence": field_data.get("confidence_score", 0)
            }

        # Map recommendation to status
        status_map = {
            'auto_accept': 'Verified',
            'flag_for_review': 'Needs Review',
            'manual_required': 'Manual Entry Required'
        }

        payload = {
            "parentAppNumber": app_no,
            "completionFlag": True,
            "utilityData": {
                "utilityProvider": extraction_result.utility_provider,
                "state": extraction_result.state,
                "program": extraction_result.program,
                "fields": fields_data,
                "overallConfidence": extraction_result.overall_confidence,
                "recommendation": extraction_result.recommendation,
                "extractionStatus": status_map.get(
                    extraction_result.recommendation, 'Unknown'
                ),
                "extractionId": extraction_result.extraction_id,
                "requiresReview": extraction_result.requires_review,
                "reviewReasons": extraction_result.review_reasons
            }
        }

        return payload
