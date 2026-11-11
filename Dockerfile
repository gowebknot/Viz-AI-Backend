# Use Python base image
FROM python:3.11

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Connector gateway (VizAI side of the client connector), installed from a vizai-connector checkout
# at a release tag. Build with: docker build --build-context connector=../vizai-connector -t vizai-backend .
# Installed after requirements.txt, whose grpcio and protobuf pins already satisfy it.
COPY --from=connector gateway /tmp/vizai-gateway
RUN pip install --no-cache-dir /tmp/vizai-gateway && rm -rf /tmp/vizai-gateway

# 8000: API (FastAPI). 50051: connector gateway (publish it only where the gateway is enabled).
EXPOSE 8000 50051

# Run FastAPI app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]