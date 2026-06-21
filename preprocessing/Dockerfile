FROM python:3.11-slim

WORKDIR /app

COPY preprocessing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY preprocessing/preprocess.py .

ENV PYTHONPATH=/app
ENV CONFIG_PATH=/config/base.yml

CMD ["python", "preprocess.py"]