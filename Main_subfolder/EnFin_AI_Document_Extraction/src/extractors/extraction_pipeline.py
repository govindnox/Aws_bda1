"""
Extraction pipeline — orchestrates document processing, text extraction,
LLM inference, and confidence scoring.

Flow:
    1. Process document to page images (embed images in PDF if needed)
    2. Extract text with Docling (lazy-loaded)
    3. Build prompt from process config + Docling text
    4. Call LLM via Bedrock Converse API (batched if needed)
    5. Rule-based confidence scoring
    6. Apply conditional response keys
    7. Return structured ExtractionResult

Author: Reet Roy
Version: 1.0.0
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple

from models.data_models import (
    ExtractionResult,
    ExtractedField,
    ProcessConfig,
)
from services import bedrock_service
from services.bedrock_service import LLMResponse

logger = logging.getLogger(__name__)


class ExtractionPipeline:
    """Orchestrates the complete extraction flow for one document."""

    def __init__(self, process_config: ProcessConfig):
        """Initialise with a process configuration.

        Args:
            process_config: Per-process definitions loaded from
                the DynamoDB config table.
        """
        self._config = process_config

        # Lazy-loaded at runtime
        self._doc_processor = None
        self._scorer = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        file_bytes: bytes,
        path: str,
        app_no: str,
    ) -> ExtractionResult:
        """Execute the full extraction pipeline for a single file.

        Args:
            file_bytes: Raw file bytes.
            path: S3 object key.
            app_no: Application number.

        Returns:
            Populated ``ExtractionResult``.
        """

        start = datetime.now()
        llm_calls = 0

        # -------------------------------------------------------------
        # Step 1 — Document ingestion (PDF / image → page images)
        # -------------------------------------------------------------
        logger.info("Step 1: Processing document to page images")
        doc_processor = self._get_doc_processor()
        processed = doc_processor.process_document(file_bytes, path, app_no)
        logger.info(
            "Document processed: %d pages, type=%s, is_image=%s",
            processed.page_count,
            processed.file_type.value,
            processed.is_image,
        )

        # -------------------------------------------------------------
        # Step 2 — Text extraction (engine selected by process config)
        # -------------------------------------------------------------
        ocr_engine = self._config.ocr_engine.lower()
        logger.info("Step 2: Extracting text with %s", ocr_engine)
        docling_processed = False
        docling_text = ""
        page_texts: Dict[int, str] = {}

        try:
            if ocr_engine == "textract":
                from extractors.textract_extractor import (
                    extract_text_textract,
                )

                text_result = extract_text_textract(
                    processed.page_images,
                    min_confidence=self._config.textract_min_confidence,
                )
            else:
                from extractors.text_extractor import extract_text

                text_result = extract_text(file_bytes, path)

            docling_text = text_result.full_markdown
            page_texts = text_result.page_texts
            docling_processed = True
            logger.info(
                "%s OK: %d pages, markdown_len=%d",
                ocr_engine,
                text_result.page_count,
                len(docling_text),
            )
            logger.info(
                "--- Extracted Markdown (%s) ---\n%s\n-----------------------------------------",
                ocr_engine,
                docling_text,
            )
        except Exception as exc:
            logger.warning("%s extraction failed: %s", ocr_engine, exc)
            # Pipeline continues — LLM will use images only

        # -------------------------------------------------------------
        # Step 3 — LLM extraction
        # -------------------------------------------------------------
        logger.info("Step 3: Extracting fields with LLM")
        llm_response, batch_count = self._extract_fields(
            processed.page_images, page_texts, app_no=app_no
        )
        llm_calls += batch_count

        extraction_output = llm_response.output
        is_supported = extraction_output.get("is_supported_document", True)
        no_relevant_reason = extraction_output.get("no_relevant_pages_reason")

        # Handle fields as list (from LLM prompt) or dict (legacy)
        fields_data = extraction_output.get("fields", [])
        if isinstance(fields_data, list):
            # Convert list to dict for internal processing
            raw_fields = {field["name"]: field for field in fields_data if "name" in field}
        else:
            # Already a dict (legacy format)
            raw_fields = fields_data

        logger.info(
            "LLM extraction: fields=%s",
            list(raw_fields.keys()),
        )

        # Handle unsupported documents
        if not is_supported:
            elapsed = int((datetime.now() - start).total_seconds() * 1000)
            return ExtractionResult(
                is_supported_document=False,
                no_relevant_pages_reason=(
                    no_relevant_reason or "Document is not a supported type"
                ),
                overall_confidence=0.0,
                recommendation="manual_required",
                llm_calls=llm_calls,
                processing_time_ms=elapsed,
                file_type=processed.file_type.value,
                docling_processed=docling_processed,
            )

        # -------------------------------------------------------------
        # Step 4 — Rule-based confidence scoring
        # -------------------------------------------------------------
        logger.info("Step 4: Scoring confidence")
        scorer = self._get_scorer()
        scored_fields = scorer.score_fields(raw_fields, docling_text, self._config)

        overall_confidence = scorer.calculate_overall_confidence(scored_fields)
        recommendation = scorer.determine_recommendation(overall_confidence)

        # -------------------------------------------------------------
        # Step 5 — Apply conditional response keys
        # -------------------------------------------------------------
        additional = scorer.apply_conditional_responses(raw_fields, self._config)

        # Build typed ExtractedField objects
        typed_fields: Dict[str, ExtractedField] = {}
        for name, data in scored_fields.items():
            typed_fields[name] = ExtractedField(
                name=name,
                value=data.get("value"),
                confidence=data.get("confidence", "NOT_FOUND"),
                confidence_score=data.get("confidence_score", 0.0),
                page=data.get("page"),
                section=data.get("section"),
                reasoning=data.get("reasoning", ""),
                docling_match=data.get("docling_match", False),
                format_match=data.get("format_match", False),
                validation_passed=data.get("validation_passed", False),
                validation_notes=data.get("validation_notes", ""),
            )

        elapsed = int((datetime.now() - start).total_seconds() * 1000)

        result = ExtractionResult(
            is_supported_document=True,
            fields=typed_fields,
            overall_confidence=overall_confidence,
            recommendation=recommendation,
            additional_response=additional,
            llm_calls=llm_calls,
            processing_time_ms=elapsed,
            file_type=processed.file_type.value,
            docling_processed=docling_processed,
        )

        logger.info(
            "Extraction complete: confidence=%.2f, recommendation=%s",
            overall_confidence,
            recommendation,
        )
        return result

    # ------------------------------------------------------------------
    # LLM field extraction (with batching)
    # ------------------------------------------------------------------

    def _extract_fields(
        self,
        page_images: Dict[int, bytes],
        page_texts: Dict[int, str],
        app_no: str = "",
    ) -> Tuple[LLMResponse, int]:
        """Run LLM extraction, batching images if needed.

        Args:
            page_images: ``{page_num: png_bytes}``.
            page_texts: ``{page_num: markdown}``.
            app_no: Application number forwarded to requestMetadata.

        Returns:
            ``(merged_response, number_of_llm_calls)``.
        """
        from config import config
        from extractors.prompt_builder import (
            build_extraction_prompt,
            build_extraction_prompt_for_batch,
        )

        max_images = config.model.max_images_per_call
        process = self._config.process

        # Sorted list of (page_num, image_bytes)
        images = sorted(page_images.items())

        if len(images) <= max_images:
            prompt = build_extraction_prompt(self._config, page_texts)
            response = bedrock_service.invoke_with_images(
                prompt=prompt, images=images, app_no=app_no, process=process
            )
            return response, 1

        # Batch
        batches = [
            images[i: i + max_images] for i in range(0, len(images), max_images)
        ]
        logger.info(
            "Splitting %d images into %d batches (max %d/call)",
            len(images),
            len(batches),
            max_images,
        )

        responses: List[LLMResponse] = []
        for idx, batch in enumerate(batches):
            page_nums = [p for p, _ in batch]
            prompt = build_extraction_prompt_for_batch(
                self._config, page_nums, page_texts
            )
            logger.info(
                "Processing batch %d/%d: pages %s",
                idx + 1,
                len(batches),
                page_nums,
            )
            resp = bedrock_service.invoke_with_images(
                prompt=prompt, images=batch, app_no=app_no, process=process
            )
            responses.append(resp)

        return self._merge_responses(responses), len(batches)

    # ------------------------------------------------------------------
    # Response merging (multi-batch)
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_responses(responses: List[LLMResponse]) -> LLMResponse:
        """Merge results from multiple batched LLM calls.

        Classification comes from the first batch (page 1).
        For fields, the value with the highest confidence wins.

        Args:
            responses: A list of LLMResponse objects from batched calls.

        Returns:
            A single consolidated LLMResponse.
        """
        if len(responses) == 1:
            return responses[0]

        confidence_rank = {
            "CERTAIN": 4,
            "LIKELY": 3,
            "UNCERTAIN": 2,
            "NOT_FOUND": 1,
        }

        base_output = dict(responses[0].output)

        # Handle fields as list or dict
        base_fields = base_output.get("fields", [])
        if isinstance(base_fields, list):
            # Convert list to dict for merging
            merged_fields = {field["name"]: field for field in base_fields if "name" in field}
        else:
            # Already a dict
            merged_fields = dict(base_fields)

        for resp in responses[1:]:
            resp_fields = resp.output.get("fields", [])

            # Convert to dict if it's a list
            if isinstance(resp_fields, list):
                resp_fields_dict = {field["name"]: field for field in resp_fields if "name" in field}
            else:
                resp_fields_dict = resp_fields

            # Merge fields based on confidence
            for field_name, field_data in resp_fields_dict.items():
                existing = merged_fields.get(field_name)
                if existing is None:
                    merged_fields[field_name] = field_data
                    continue
                old = confidence_rank.get(existing.get("confidence", "NOT_FOUND"), 0)
                new = confidence_rank.get(field_data.get("confidence", "NOT_FOUND"), 0)
                if new > old:
                    merged_fields[field_name] = field_data

        base_output["fields"] = merged_fields

        return LLMResponse(
            reasoning="\n\n---\n\n".join(
                f"[Batch {i + 1}] {r.reasoning}" for i, r in enumerate(responses)
            ),
            output=base_output,
            raw_response="\n\n---\n\n".join(r.raw_response for r in responses),
            model_used=responses[0].model_used,
            input_tokens=sum(r.input_tokens for r in responses),
            output_tokens=sum(r.output_tokens for r in responses),
        )

    # ------------------------------------------------------------------
    # Lazy service initialisation
    # ------------------------------------------------------------------

    def _get_doc_processor(self):
        """Lazy-init DocumentProcessor."""
        if self._doc_processor is None:
            from extractors.document_processor import DocumentProcessor

            self._doc_processor = DocumentProcessor()
        return self._doc_processor

    def _get_scorer(self):
        """Lazy-init ConfidenceScorer."""
        if self._scorer is None:
            from scoring.confidence_scorer import ConfidenceScorer

            self._scorer = ConfidenceScorer()
        return self._scorer
