FROM public.ecr.aws/lambda/python:3.13

# Model storage paths — all under /tmp/. Models auto-download on cold start.
ENV HF_HOME=/tmp/huggingface
ENV EASYOCR_MODULE_PATH=/tmp/easyocr
ENV MODULE_PATH=/tmp/modules
ENV TORCH_HOME=/tmp/torch
# Force CPU-only PyTorch — suppresses pin_memory accelerator warning
ENV CUDA_VISIBLE_DEVICES=""

# Install system dependencies required by PIL, fitz, and OCR engines
RUN dnf install -y gcc gcc-c++ zlib-devel libjpeg-turbo-devel mesa-libGL glib2 && dnf clean all

# Install CPU-only PyTorch first (reduces image size significantly)
RUN pip install --no-cache-dir torch torchvision \
    --extra-index-url https://download.pytorch.org/whl/cpu

# Copy requirements and install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy application source code
COPY src/ ${LAMBDA_TASK_ROOT}/

# Set the Lambda handler — thin handler delegates to controller
CMD ["lambda_function.lambda_handler"]
