import os
import json
import time
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime
import csv
from kafka import KafkaProducer

app = Flask(__name__)

LOG_FILE = "data/metricas_sistema.csv"
HEADERS = ["timestamp", "evento", "key", "latencia_ms", "fuente", "consulta_id", "retry_count"]

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
METRICS_TOPIC = "metrics-topic"

def inicializar_csv():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

# Siempre reinicia el CSV al arrancar el servicio
inicializar_csv()


def crear_producer():
    """Crea el KafkaProducer con reintentos en caso de que Kafka aún no esté listo."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10
            )
            print(f"[metricas] Producer Kafka conectado en {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"[metricas] Kafka no disponible aún, reintentando en 3s... ({e})")
            time.sleep(3)


producer = crear_producer()


def extraer_tipo_consulta(key):
    """Extrae el tipo de consulta (Q1-Q5) del campo 'key', formato 'Q1:Z1:conf=0.5'."""
    if not key:
        return "desconocido"
    return key.split(":")[0]


def publicar_metrica_kafka(data):
    """
    Publica el evento en metrics-topic con el formato que espera Spark:
    timestamp, tipo de consulta, latencia, cache_hit (bool), retry_count, estado final.
    """
    evento = data.get('evento', '')
    key    = data.get('key', '')

    mensaje = {
        "timestamp":     datetime.now().isoformat(),
        "evento":        evento,                                  # HIT | MISS | RETRY | DLQ
        "tipo_consulta": extraer_tipo_consulta(key),               # Q1, Q2, Q3, Q4, Q5
        "latencia_ms":   data.get('latencia_ms'),
        "cache_hit":     evento == "HIT",
        "retry_count":   data.get('retry_count', 0),
        "consulta_id":   data.get('consulta_id', ''),
        "fuente":        data.get('fuente', ''),
        "estado_final":  "exitoso" if evento in ("HIT", "MISS") else evento.lower()
    }

    try:
        producer.send(METRICS_TOPIC, value=mensaje)
        producer.flush(timeout=2)
    except Exception as e:
        print(f"[metricas] Error publicando a Kafka: {e}")


@app.route('/registrar', methods=['POST'])
def registrar():
    data = request.json

    # ── 1. Persistir en CSV (compatibilidad Tarea 2) ──────
    nuevo_registro = [
        datetime.now().isoformat(),
        data.get('evento'),
        data.get('key'),
        data.get('latencia_ms'),
        data.get('fuente'),
        data.get('consulta_id', ''),
        data.get('retry_count', 0)
    ]
    try:
        with open(LOG_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(nuevo_registro)
            f.flush()
    except Exception as e:
        print(f"Error al escribir en el log: {e}")
        return jsonify({"error": "No se pudo escribir el registro"}), 500

    # ── 2. Publicar a Kafka para Spark Structured Streaming ──
    publicar_metrica_kafka(data)

    return jsonify({"status": "registrado"}), 201


@app.route('/resumen', methods=['GET'])
def resumen():
    """Endpoint de respaldo para consultar métricas desde el CSV (sin Spark)."""
    try:
        df = pd.read_csv(LOG_FILE)
        total   = len(df)
        hits    = len(df[df['evento'] == 'HIT'])
        misses  = len(df[df['evento'] == 'MISS'])
        retries = len(df[df['evento'] == 'RETRY'])
        dlq     = len(df[df['evento'] == 'DLQ'])
        return jsonify({
            "total":      total,
            "hits":       hits,
            "misses":     misses,
            "retries":    retries,
            "dlq":        dlq,
            "hit_rate":   round(hits / total, 4) if total > 0 else 0,
            "retry_rate": round(retries / total, 4) if total > 0 else 0,
            "dlq_rate":   round(dlq / total, 4) if total > 0 else 0,
            "p50_ms":     round(df['latencia_ms'].median(), 2),
            "p95_ms":     round(df['latencia_ms'].quantile(0.95), 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000)
