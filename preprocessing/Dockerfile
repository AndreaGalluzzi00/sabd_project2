FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY preprocess.py .

# Raw dataset (read-only) and prepared output volume are mounted at runtime
ENV RAW_DATASET_PATH=/data \
    PREPARED_PATH=/prepared/flights_prepared.parquet

CMD ["python", "preprocess.py"]
