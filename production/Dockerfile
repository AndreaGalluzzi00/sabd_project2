FROM python:3.11-slim

WORKDIR /app

COPY production/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY production /app
COPY common /app/common

ENV PYTHONPATH=/app
ENV CONFIG_PATH=/config/base.yml

CMD ["python", "producer.py"]