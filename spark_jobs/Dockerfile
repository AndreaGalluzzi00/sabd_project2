FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    PyYAML \
    "numpy==1.24.4" \
    "pandas==2.0.3" \
    "pyarrow==12.0.1"
RUN mkdir -p /tmp/.ivy2 /tmp/spark-events && chmod -R 777 /tmp/.ivy2 /tmp/spark-events

USER root

WORKDIR /opt/spark/app

ENV PYTHONPATH=/opt/spark/app:/opt/spark/app/spark_jobs:/opt/spark/app/common
ENV HOME=/tmp
ENV SPARK_SUBMIT_OPTS="-Duser.home=/tmp -Divy.home=/tmp/.ivy2"
