FROM flink:1.20-java17

# Install Python 3
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# Install PyFlink (must match Flink version)
RUN pip3 install --no-cache-dir apache-flink==1.20.0

# Kafka SQL connector JAR for Flink 1.20
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# Copy job files
COPY . /opt/flink/jobs/
