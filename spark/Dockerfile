FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

RUN pip install --no-cache-dir requests

COPY spark_streaming_job.py .

# Crear carpeta de checkpoints para Structured Streaming
RUN mkdir -p /tmp/spark-checkpoints && chmod -R 777 /tmp/spark-checkpoints

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--driver-memory", "1g", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/app/spark_streaming_job.py"]
