# PyFlink (apache-flink) non pubblica wheel Linux aarch64: forziamo amd64.
# Su Apple Silicon gira via Rosetta (abilitare in Docker Desktop).
FROM --platform=linux/amd64 flink:1.20-java17

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.20.0 PyYAML

RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

COPY common /opt/flink/jobs/common
COPY flink_jobs /opt/flink/jobs

ENV PYTHONPATH=/opt/flink/jobs
ENV CONFIG_PATH=/config/base.yml