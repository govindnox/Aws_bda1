"""
Main extraction pipeline orchestrating the complete utility bill extraction process.

New flow using Docling + single LLM call + rule-based scoring:
1. Document ingestion (PDF/DOCX to images via PyMuPDF)
2. Docling text extraction (format-preserving text from full document)
3. LLM extraction (images + Docling text via Bedrock Converse API)
4. Rule-based confidence scoring (validate against Docling text + format patterns)
5. Determine recommendation
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import uuid

from config.settings import settings
from config.prompts import (
    EXTRACTION_PROMPT as FALLBACK_EXTRACTION_PROMPT,
    FIELD_CONFIGURATIONS,
    get_utility_fields_guide
)
from services.bedrock_service import BedrockService, PromptBuilder, LLMResponse
from services.docling_service import DoclingService, UnsupportedFileTypeError
from services.prompt_service import get_prompt_service
from scoring.confidence_scorer import RuleBasedScorer
from extractors.document_processor import DocumentProcessor, ImagePreparer, FileType


logger = logging.getLogger(__name__)


# Image file extensions that should be rejected
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.tiff', '.tif', '.bmp', '.gif', '.webp'}


@dataclass
class ExtractionResult:
    """Complete extraction result"""
    extraction_id: str
    timestamp: str
    source_file: str
    process: str  # Extraction process name

    # Classification
    utility_provider: str
    state: str
    program: str
    classification_confidence: str

    # Extracted and validated fields
    fields: Dict[str, Dict[str, Any]]

    # Overall status
    overall_confidence: float
    requires_review: bool
    review_reasons: List[str]
    recommendation: str  # auto_accept | flag_for_review | manual_required

    # Metadata
    pages_processed: List[int]
    llm_calls: int
    docling_processed: bool
    processing_time_ms: int

    # Document info
    file_type: str = "pdf"
    is_single_image: bool = False
    no_relevant_pages: bool = False

    # Debug info
    raw_reasoning: Dict[str, str] = field(default_factory=dict)


class ExtractionPipeline:
    """
    Main extraction pipeline implementing the simplified flow:
    1. Document ingestion (PDF/DOCX to images)
    2. Docling text extraction
    3. LLM data extraction (single call with images + Docling text)
    4. Rule-based confidence scoring
    5. Determine recommendation
    """

    def __init__(self, process: str = None):
        """
        Initialize the extraction pipeline.

        Args:
            process: Extraction process name (e.g., 'm0_utility_bill').
                    If None, uses settings.DEFAULT_PROCESS
        """
        self.document_processor = DocumentProcessor()
        self.docling_service = DoclingService()
        self.bedrock_service = BedrockService()
        self.prompt_builder = PromptBuilder()
        self.scorer = RuleBasedScorer()
        self.prompt_service = get_prompt_service()

        # Set process
        self.process = process or settings.DEFAULT_PROCESS

        # Thresholds from settings
        self.auto_accept_threshold = settings.AUTO_ACCEPT_THRESHOLD
        self.flag_threshold = settings.FLAG_THRESHOLD

        # Load extraction prompt
        self._load_prompts()

    def _load_prompts(self):
        """Load extraction prompt from Bedrock Prompt Management with fallback"""
        logger.info(f"Loading prompts for process: {self.process}")

        prompt_ids = settings.get_process_prompt_ids(self.process)

        # Extraction Prompt
        if prompt_ids.get("extraction"):
            self.extraction_prompt = self.prompt_service.get_prompt(
                prompt_ids["extraction"]
            )
        else:
            self.extraction_prompt = None

        if not self.extraction_prompt:
            logger.warning("Using fallback extraction prompt")
            self.extraction_prompt = FALLBACK_EXTRACTION_PROMPT

        logger.info("Prompts loaded successfully")

    def extract(
        self,
        file_bytes: bytes,
        source_file: str,
        app_no: Optional[str] = None,
        process: Optional[str] = None
    ) -> ExtractionResult:
        """
        Execute the complete extraction pipeline.

        Args:
            file_bytes: Raw file bytes (PDF or DOCX)
            source_file: Source file path/identifier
            app_no: Application number for tracking
            process: Extraction process (overrides instance process)

        Returns:
            ExtractionResult with validated fields and confidence scores
        """
        start_time = datetime.now()
        extraction_id = str(uuid.uuid4())
        llm_calls = 0
        raw_reasoning = {}

        # Use process from parameter or instance
        current_process = process or self.process

        logger.info(f"Starting extraction: {extraction_id} for {source_file} (process: {current_process})")

        # =====================================================================
        # STEP 0: Check file type - reject image files early
        # =====================================================================
        ext = self._get_file_extension(source_file)
        if ext in IMAGE_EXTENSIONS:
            logger.warning(f"Image file rejected: {source_file} ({ext})")
            end_time = datetime.now()
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)

            return ExtractionResult(
                extraction_id=extraction_id,
                timestamp=start_time.isoformat(),
                source_file=source_file,
                process=current_process,
                utility_provider="Unknown",
                state="Unknown",
                program="Unknown",
                classification_confidence="LOW",
                fields={},
                overall_confidence=0.0,
                requires_review=True,
                review_reasons=[
                    f"Unsupported file type: {ext}. Only document formats (PDF, DOCX) are supported."
                ],
                recommendation="manual_required",
                pages_processed=[],
                llm_calls=0,
                docling_processed=False,
                processing_time_ms=processing_time_ms,
                file_type=ext.lstrip('.'),
                is_single_image=True,
                no_relevant_pages=True,
                raw_reasoning={}
            )

        # =====================================================================
        # STEP 1: Document Ingestion - Convert document to page images for LLM
        # =====================================================================
        logger.info("Step 1: Processing document to page images...")
        processed_doc = self.document_processor.process_document(
            file_bytes,
            file_path=source_file
        )
        all_page_images = processed_doc.page_images

        logger.info(
            f"Document processed: {processed_doc.page_count} pages, "
            f"type: {processed_doc.file_type.value}"
        )

        # =====================================================================
        # STEP 2: Docling Text Extraction - Extract text from full document
        # =====================================================================
        logger.info("Step 2: Extracting text with Docling...")
        try:
            docling_result = self.docling_service.extract_text(
                file_bytes=file_bytes,
                file_path=source_file
            )
            docling_text = docling_result.full_markdown
            docling_processed = True

            logger.info(
                f"Docling extraction complete: {docling_result.page_count} pages, "
                f"text length: {len(docling_text)}"
            )

        except UnsupportedFileTypeError as e:
            logger.error(f"Docling rejected file: {e}")
            end_time = datetime.now()
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)

            return ExtractionResult(
                extraction_id=extraction_id,
                timestamp=start_time.isoformat(),
                source_file=source_file,
                process=current_process,
                utility_provider="Unknown",
                state="Unknown",
                program="Unknown",
                classification_confidence="LOW",
                fields={},
                overall_confidence=0.0,
                requires_review=True,
                review_reasons=[str(e)],
                recommendation="manual_required",
                pages_processed=[],
                llm_calls=0,
                docling_processed=False,
                processing_time_ms=processing_time_ms,
                file_type=processed_doc.file_type.value,
                no_relevant_pages=True,
                raw_reasoning={}
            )

        # =====================================================================
        # STEP 3: Data Extraction - Single LLM call with images + Docling text
        # =====================================================================
        logger.info("Step 3: Extracting fields with LLM (Converse API)...")
        extraction_response, batch_count = self._extract_fields(
            all_page_images,
            docling_result.page_texts
        )
        llm_calls += batch_count
        raw_reasoning["extraction"] = extraction_response.reasoning

        # Parse extraction output
        extraction_output = extraction_response.output
        utility_provider = extraction_output.get("utility_provider", "Unknown")
        state = extraction_output.get("state", "Unknown")
        is_supported = extraction_output.get("is_supported_document", True)
        no_relevant_reason = extraction_output.get("no_relevant_pages_reason")
        extracted_fields = extraction_output.get("fields", {})

        # Determine program
        program = self._get_program(utility_provider)

        logger.info(
            f"LLM extraction: utility={utility_provider}, state={state}, "
            f"fields={list(extracted_fields.keys())}"
        )

        # Handle unsupported documents
        if not is_supported or utility_provider == "Not_Utility_Bill":
            logger.warning(f"Unsupported document: {no_relevant_reason}")
            end_time = datetime.now()
            processing_time_ms = int((end_time - start_time).total_seconds() * 1000)

            return ExtractionResult(
                extraction_id=extraction_id,
                timestamp=start_time.isoformat(),
                source_file=source_file,
                process=current_process,
                utility_provider=utility_provider,
                state=state,
                program=program,
                classification_confidence="LOW",
                fields={},
                overall_confidence=0.0,
                requires_review=True,
                review_reasons=[
                    no_relevant_reason or "Document is not a supported utility bill"
                ],
                recommendation="manual_required",
                pages_processed=list(all_page_images.keys()),
                llm_calls=llm_calls,
                docling_processed=docling_processed,
                processing_time_ms=int((datetime.now() - start_time).total_seconds() * 1000),
                file_type=processed_doc.file_type.value,
                no_relevant_pages=True,
                raw_reasoning=raw_reasoning
            )

        # =====================================================================
        # STEP 4: Rule-based Confidence Scoring
        # =====================================================================
        logger.info("Step 4: Scoring confidence (rule-based)...")
        scored_fields = self.scorer.score_fields(
            extracted_fields=extracted_fields,
            docling_text=docling_text,
            utility=utility_provider
        )

        overall_confidence = self.scorer.calculate_overall_confidence(scored_fields)
        recommendation = self.scorer.determine_recommendation(overall_confidence)

        # Determine review status
        requires_review = overall_confidence < self.flag_threshold
        review_reasons = []
        for field_name, field_data in scored_fields.items():
            if not field_data.get("validation_passed", False):
                review_reasons.append(
                    f"{field_name}: {field_data.get('validation_notes', 'Validation failed')}"
                )

        # =====================================================================
        # STEP 5: Finalize Result
        # =====================================================================
        end_time = datetime.now()
        processing_time_ms = int((end_time - start_time).total_seconds() * 1000)

        result = ExtractionResult(
            extraction_id=extraction_id,
            timestamp=start_time.isoformat(),
            source_file=source_file,
            process=current_process,
            utility_provider=utility_provider,
            state=state,
            program=program,
            classification_confidence="HIGH" if utility_provider in FIELD_CONFIGURATIONS else "LOW",
            fields=scored_fields,
            overall_confidence=overall_confidence,
            requires_review=requires_review,
            review_reasons=review_reasons,
            recommendation=recommendation,
            pages_processed=list(all_page_images.keys()),
            llm_calls=llm_calls,
            docling_processed=docling_processed,
            processing_time_ms=processing_time_ms,
            file_type=processed_doc.file_type.value,
            is_single_image=processed_doc.is_single_image,
            no_relevant_pages=False,
            raw_reasoning=raw_reasoning
        )

        logger.info(
            f"Extraction complete: {extraction_id}, "
            f"Confidence: {overall_confidence:.2f}, "
            f"Recommendation: {recommendation}"
        )

        return result

    def _extract_fields(
        self,
        page_images: Dict[int, bytes],
        page_texts: Dict[int, str]
    ) -> Tuple[LLMResponse, int]:
        """
        Step 3: Extract fields using LLM with page images + Docling text.

        Images are batched if they exceed MAX_IMAGES_PER_LLM_CALL.
        Each batch receives ONLY the page-partitioned Docling markdown
        for the pages whose images are in that batch.
        Results from multiple batches are aggregated by keeping
        the highest-confidence value for each field.

        Args:
            page_images: Dict of page_number -> image bytes
            page_texts: Per-page markdown from Docling

        Returns:
            Tuple of (LLMResponse, number_of_llm_calls)
        """
        max_images = settings.MAX_IMAGES_PER_LLM_CALL
        utility_fields_guide = get_utility_fields_guide()

        # Prepare images for Converse API (sorted by page number)
        images = ImagePreparer.prepare_images_for_prompt(page_images)

        if len(images) <= max_images:
            # All images fit in a single call — build page-partitioned text
            all_page_nums = [img[0] for img in images]
            partitioned_text = self._build_batch_text(all_page_nums, page_texts)
            prompt = self.prompt_builder.build_docling_extraction_prompt(
                docling_text=partitioned_text,
                utility_fields_guide=utility_fields_guide,
                extraction_prompt_template=self.extraction_prompt
            )
            response = self.bedrock_service.invoke_with_images(
                prompt=prompt,
                images=images,
                model_id=settings.EXTRACTION_MODEL
            )
            return response, 1

        # Split images into batches of max_images
        batches = [
            images[i:i + max_images]
            for i in range(0, len(images), max_images)
        ]
        logger.info(
            "Splitting %d images into %d batches (max %d per call)",
            len(images), len(batches), max_images
        )

        all_responses: List[LLMResponse] = []
        for batch_idx, batch in enumerate(batches):
            page_nums = [img[0] for img in batch]

            # Build per-batch markdown from only the pages in this batch
            batch_text = self._build_batch_text(page_nums, page_texts)

            logger.info(
                "Processing batch %d/%d: pages %s (text_length=%d)",
                batch_idx + 1, len(batches), page_nums, len(batch_text)
            )

            prompt = self.prompt_builder.build_docling_extraction_prompt(
                docling_text=batch_text,
                utility_fields_guide=utility_fields_guide,
                extraction_prompt_template=self.extraction_prompt
            )
            response = self.bedrock_service.invoke_with_images(
                prompt=prompt,
                images=batch,
                model_id=settings.EXTRACTION_MODEL
            )
            all_responses.append(response)

        # Aggregate results from all batches
        merged = self._merge_llm_responses(all_responses)
        return merged, len(batches)

    @staticmethod
    def _build_batch_text(
        page_nums: List[int], page_texts: Dict[int, str]
    ) -> str:
        """
        Assemble Docling markdown for a specific set of pages.

        Each page's text is wrapped in <pageN> XML tags so the LLM
        can correlate text with the corresponding page image.
        """
        sections = []
        for page_num in sorted(page_nums):
            text = page_texts.get(page_num, "").strip()
            content = text if text else "[No text extracted for this page]"
            sections.append(f"<page{page_num}>\n{content}\n</page{page_num}>")
        return "\n\n".join(sections)

    def _merge_llm_responses(
        self, responses: List[LLMResponse]
    ) -> LLMResponse:
        """
        Merge extraction results from multiple batched LLM calls.

        Classification (utility_provider, state) comes from the first batch
        (page 1 typically contains logo/header). For fields, the value with
        the highest confidence across batches is kept.
        """
        if len(responses) == 1:
            return responses[0]

        confidence_rank = {
            "CERTAIN": 4, "LIKELY": 3, "UNCERTAIN": 2, "NOT_FOUND": 1
        }

        # Use first batch output as base (has page-1 classification)
        base_output = dict(responses[0].output)
        merged_fields = dict(base_output.get("fields", {}))

        # Merge fields from subsequent batches
        for resp in responses[1:]:
            for field_name, field_data in resp.output.get("fields", {}).items():
                existing = merged_fields.get(field_name)
                if existing is None:
                    merged_fields[field_name] = field_data
                    continue

                existing_conf = confidence_rank.get(
                    existing.get("confidence", "NOT_FOUND"), 0
                )
                new_conf = confidence_rank.get(
                    field_data.get("confidence", "NOT_FOUND"), 0
                )
                if new_conf > existing_conf:
                    merged_fields[field_name] = field_data

        base_output["fields"] = merged_fields

        # Aggregate token counts and reasoning
        total_input = sum(r.input_tokens for r in responses)
        total_output = sum(r.output_tokens for r in responses)
        combined_reasoning = "\n\n---\n\n".join(
            f"[Batch {i + 1}] {r.reasoning}" for i, r in enumerate(responses)
        )

        return LLMResponse(
            reasoning=combined_reasoning,
            output=base_output,
            raw_response="\n\n---\n\n".join(r.raw_response for r in responses),
            model_used=responses[0].model_used,
            input_tokens=total_input,
            output_tokens=total_output,
        )

    def _get_program(self, utility: str) -> str:
        """Get program name from utility"""
        config = FIELD_CONFIGURATIONS.get(utility, {})
        return config.get("program", "Unknown")

    def _get_file_extension(self, file_path: str) -> str:
        """Get lowercase file extension from path."""
        import os
        if not file_path:
            return ''
        _, ext = os.path.splitext(file_path.lower())
        return ext
