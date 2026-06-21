FROM python:3.11-slim

WORKDIR /app

COPY production/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY production/producer.py .

ENV PYTHONPATH=/app
ENV CONFIG_PATH=/config/base.yml

CMD ["python", "producer.py"]