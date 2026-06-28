FROM bitnami/spark:3.5.1

USER root
WORKDIR /app

COPY spark_streaming_job.py .

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4", \
     "/app/spark_streaming_job.py"]
