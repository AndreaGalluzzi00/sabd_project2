FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Dataset directory is mounted at runtime (see docker-compose volumes)
ENV DATASET_PATH=/data \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=flights \
    ACCELERATION_FACTOR=3600

CMD ["python", "producer.py"]
