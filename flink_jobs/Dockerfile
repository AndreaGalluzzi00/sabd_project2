
FROM --platform=linux/amd64 flink:1.20-java17

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir --timeout 120 --retries 10 \
    apache-flink==1.20.0 \
    PyYAML \
    "ddsketch==3.0.1"

RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.0/flink-sql-avro-confluent-registry-1.20.0.jar


RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

COPY common /opt/flink/jobs/common
COPY flink_jobs /opt/flink/jobs

ENV PYTHONPATH=/opt/flink/jobs:/opt/flink/jobs/q1:/opt/flink/jobs/q2:/opt/flink/jobs/q3
ENV CONFIG_PATH=/config/base.yml
