"""
spark_streaming_job.py

Job de Apache Spark Structured Streaming para la Tarea 3.

Flujo:
1. Lee el stream de eventos desde Kafka (metrics-topic).
2. Parsea cada mensaje JSON (timestamp, tipo_consulta, latencia_ms, cache_hit, retry_count, estado_final).
3. Aplica ventanas de tiempo deslizantes (sliding windows) con actualización periódica.
4. Calcula por ventana: throughput, latencia p50/p95, hit rate, retry rate.
5. Escribe los resultados agregados en Elasticsearch (índice: metrics-aggregated).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, expr,
    percentile_approx, sum as spark_sum, when, to_json, struct,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ES_HOST      = os.getenv("ES_HOST", "elasticsearch")
ES_PORT      = os.getenv("ES_PORT", "9200")
METRICS_TOPIC = "metrics-topic"
ES_INDEX      = "metrics-aggregated"

# Ventana deslizante: agrupa cada 30s, recalculando cada 10s
WINDOW_DURATION = "30 seconds"
SLIDE_DURATION  = "10 seconds"

# ── Esquema del mensaje JSON publicado por metricas.py ──────
schema = StructType([
    StructField("timestamp",     StringType(),  True),
    StructField("evento",        StringType(),  True),
    StructField("tipo_consulta", StringType(),  True),
    StructField("latencia_ms",   DoubleType(),  True),
    StructField("cache_hit",     BooleanType(), True),
    StructField("retry_count",   IntegerType(), True),
    StructField("consulta_id",   StringType(),  True),
    StructField("fuente",        StringType(),  True),
    StructField("estado_final",  StringType(),  True),
])


def crear_spark_session():
    return (
        SparkSession.builder
        .appName("MetricasStreamingTarea3")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def leer_stream_kafka(spark):
    """Lee el stream crudo desde Kafka y parsea el JSON."""
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", METRICS_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_parsed = (
        df_raw
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", expr("to_timestamp(timestamp)"))
        # filtrar filas con timestamp nulo (mensajes corruptos)
        .filter(col("event_time").isNotNull())
    )

    return df_parsed


def calcular_agregaciones(df):
    """
    Aplica ventana deslizante y calcula:
    - throughput: cantidad de consultas exitosas (HIT+MISS) por ventana
    - p50 / p95 de latencia
    - hit_rate: proporción de cache hits
    - retry_rate: proporción de consultas con retry_count > 0
    """
    df_con_flags = df.withColumn(
        "es_exitoso", col("evento").isin("HIT", "MISS")
    ).withColumn(
        "tuvo_retry", col("retry_count") > 0
    )

    agregado = (
        df_con_flags
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION))
        .agg(
            count("*").alias("total_eventos"),
            spark_sum(when(col("es_exitoso"), 1).otherwise(0)).alias("exitosos"),
            spark_sum(when(col("evento") == "HIT", 1).otherwise(0)).alias("hits"),
            spark_sum(when(col("evento") == "MISS", 1).otherwise(0)).alias("misses"),
            spark_sum(when(col("evento") == "RETRY", 1).otherwise(0)).alias("retries"),
            spark_sum(when(col("evento") == "DLQ", 1).otherwise(0)).alias("dlq"),
            percentile_approx("latencia_ms", 0.5).alias("p50_ms"),
            percentile_approx("latencia_ms", 0.95).alias("p95_ms"),
            avg("latencia_ms").alias("latencia_promedio_ms"),
        )
        .withColumn(
            "hit_rate",
            when(col("total_eventos") > 0, col("hits") / col("total_eventos")).otherwise(0.0)
        )
        .withColumn(
            "retry_rate",
            when(col("total_eventos") > 0, col("retries") / col("total_eventos")).otherwise(0.0)
        )
        .withColumn(
            "dlq_rate",
            when(col("total_eventos") > 0, col("dlq") / col("total_eventos")).otherwise(0.0)
        )
        .withColumn(
            # Throughput: exitosos por minuto, normalizado desde la ventana de 30s
            "throughput_por_min",
            (col("exitosos") / 30.0) * 60.0
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_eventos", "exitosos", "hits", "misses", "retries", "dlq",
            "p50_ms", "p95_ms", "latencia_promedio_ms",
            "hit_rate", "retry_rate", "dlq_rate", "throughput_por_min"
        )
        # Elasticsearch necesita un _id explícito para upsert en modo streaming.
        # Usamos el inicio de la ventana (epoch ms) como identificador único por ventana.
        .withColumn("doc_id", expr("CAST(unix_timestamp(window_start) AS STRING)"))
    )

    return agregado


def escribir_a_elasticsearch(df):
    """
    Escribe los resultados agregados en Elasticsearch vía REST HTTP usando foreachBatch.

    Se evita el conector nativo org.elasticsearch.spark.sql porque presenta
    incompatibilidades binarias con Spark 3.5.x (NoSuchMethodError en RowEncoder).
    foreachBatch + requests es más simple y evita el problema de versiones.
    """
    import requests
    import json as json_lib

    es_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX}/_doc"

    def procesar_batch(batch_df, batch_id):
        filas = batch_df.collect()
        print(f"[spark] Batch {batch_id}: {len(filas)} ventana(s) agregada(s) -> enviando a Elasticsearch")

        for fila in filas:
            doc = fila.asDict()
            doc_id = doc.pop("doc_id")

            # Convertir tipos no serializables (Timestamp, etc.) a string
            for k, v in doc.items():
                if hasattr(v, "isoformat"):
                    doc[k] = v.isoformat()

            try:
                resp = requests.put(
                    f"{es_url}/{doc_id}",
                    data=json_lib.dumps(doc),
                    headers={"Content-Type": "application/json"},
                    timeout=5
                )
                if resp.status_code not in (200, 201):
                    print(f"[spark] ES respondió {resp.status_code}: {resp.text}")
            except Exception as e:
                print(f"[spark] Error escribiendo a Elasticsearch: {e}")

    es_query = (
        df.writeStream
        .outputMode("update")
        .foreachBatch(procesar_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/metrics-agg")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return es_query


def escribir_a_consola(df):
    """Sink adicional a consola para debug (visible en docker logs)."""
    console_query = (
        df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return console_query


if __name__ == "__main__":
    print(f"[spark] Iniciando job de Structured Streaming")
    print(f"[spark] Kafka broker: {KAFKA_BROKER}")
    print(f"[spark] Elasticsearch: {ES_HOST}:{ES_PORT}")
    print(f"[spark] Ventana: {WINDOW_DURATION} deslizante cada {SLIDE_DURATION}")

    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_stream    = leer_stream_kafka(spark)
    df_agregado  = calcular_agregaciones(df_stream)

    # Sink a consola para verificar visualmente que el job funciona
    query_console = escribir_a_consola(df_agregado)

    # Sink a Elasticsearch (el real para Kibana)
    query_es = escribir_a_elasticsearch(df_agregado)

    print("[spark] Streams iniciados. Esperando datos...")
    query_es.awaitTermination()
