FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Prepared dataset volume is mounted at runtime (see docker-compose volumes)
ENV PREPARED_PATH=/prepared/flights_prepared.parquet \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=flights \
    ACCELERATION_FACTOR=3600

CMD ["python", "producer.py"]
