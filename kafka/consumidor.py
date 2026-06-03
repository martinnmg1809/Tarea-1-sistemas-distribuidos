import os
import json
import time
import uuid
import requests
from kafka import KafkaConsumer, KafkaProducer

# ── Configuración desde variables de entorno ──
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL      = os.getenv("CACHE_URL", "http://sistema-cache:8000/consultar")
RESPUESTAS_URL = os.getenv("RESPUESTAS_URL", "http://generador-respuestas:5000/procesar")
METRICAS_URL   = os.getenv("METRICAS_URL", "http://almacenamiento-metricas:9000/registrar")
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "500"))
CONSUMER_MODE  = os.getenv("CONSUMER_MODE", "main")  # "main" | "retry"

TOPIC_MAIN  = "consultas"
TOPIC_RETRY = "consultas-retry"
TOPIC_DLQ   = "consultas-dlq"
GROUP_ID    = "consumidores-principales" if CONSUMER_MODE == "main" else "consumidores-retry"


def registrar_metrica(evento, key, latencia_ms, fuente, extra=None):
    payload = {
        "evento":     evento,
        "key":        key,
        "latencia_ms": latencia_ms,
        "fuente":     fuente
    }
    if extra:
        payload.update(extra)
    try:
        requests.post(METRICAS_URL, json=payload, timeout=1)
    except Exception:
        pass


def crear_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
            print(f"[producer] Listo en {KAFKA_BROKER}")
            return p
        except Exception as e:
            print(f"[producer] Esperando Kafka... ({e})")
            time.sleep(3)


def crear_consumer(topic):
    while True:
        try:
            c = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            print(f"[consumer] Suscrito a '{topic}' (grupo={GROUP_ID})")
            return c
        except Exception as e:
            print(f"[consumer] Esperando Kafka... ({e})")
            time.sleep(3)


def procesar_consulta(msg, producer):
    """
    Flujo principal según Figura 1 y Figura 2:
    1. Intentar cache (HIT → registrar y terminar)
    2. Cache MISS → llamar a generador-respuestas
    3. Si falla → enviar a retry (si retry_count < MAX_RETRIES) o a DLQ
    """
    start = time.time()
    consulta = msg.value

    # Asegurar campos obligatorios
    if "id" not in consulta:
        consulta["id"] = str(uuid.uuid4())
    if "retry_count" not in consulta:
        consulta["retry_count"] = 0

    cid         = consulta["id"]
    retry_count = consulta["retry_count"]
    tipo        = consulta.get("tipo", "?")
    zona        = consulta.get("zona_id", "?")

    print(f"[{CONSUMER_MODE}] Procesando {cid} | tipo={tipo} zona={zona} retry={retry_count}")

    # ── 1. Consultar caché ──────────────────────────────
    try:
        r = requests.post(CACHE_URL, json=consulta, timeout=5)
        data = r.json()
        latencia_ms = (time.time() - start) * 1000

        if data.get("fuente") == "cache":
            print(f"[{CONSUMER_MODE}] HIT  {cid} ({latencia_ms:.1f}ms)")
            registrar_metrica("HIT", f"{tipo}:{zona}", latencia_ms, "cache",
                              {"consulta_id": cid, "retry_count": retry_count})
            return  # éxito vía caché

        # MISS: la caché ya llamó a respuestas internamente, éxito igual
        print(f"[{CONSUMER_MODE}] MISS {cid} ({latencia_ms:.1f}ms)")
        registrar_metrica("MISS", f"{tipo}:{zona}", latencia_ms, "generador_respuestas",
                          {"consulta_id": cid, "retry_count": retry_count})
        return

    except Exception as e:
        # ── 2. Fallo: decidir entre retry y DLQ ────────
        latencia_ms = (time.time() - start) * 1000
        print(f"[{CONSUMER_MODE}] FALLO {cid} retry={retry_count}/{MAX_RETRIES} → {e}")

        if retry_count < MAX_RETRIES:
            consulta["retry_count"] += 1
            time.sleep(RETRY_DELAY_MS / 1000)
            producer.send(TOPIC_RETRY, value=consulta)
            producer.flush()
            registrar_metrica("RETRY", f"{tipo}:{zona}", latencia_ms, "retry",
                              {"consulta_id": cid, "retry_count": consulta["retry_count"]})
            print(f"[{CONSUMER_MODE}] → Enviado a retry ({consulta['retry_count']}/{MAX_RETRIES})")
        else:
            producer.send(TOPIC_DLQ, value=consulta)
            producer.flush()
            registrar_metrica("DLQ", f"{tipo}:{zona}", latencia_ms, "dlq",
                              {"consulta_id": cid, "retry_count": retry_count})
            print(f"[{CONSUMER_MODE}] → Enviado a DLQ (agotó {MAX_RETRIES} reintentos)")


# ── MAIN ──────────────────────────────────────────────
if __name__ == "__main__":
    topic = TOPIC_RETRY if CONSUMER_MODE == "retry" else TOPIC_MAIN
    print(f"[consumidor] Iniciando modo={CONSUMER_MODE} topic={topic} max_retries={MAX_RETRIES}")

    producer = crear_producer()
    consumer = crear_consumer(topic)

    try:
        for msg in consumer:
            procesar_consulta(msg, producer)
    except KeyboardInterrupt:
        print("[consumidor] Detenido.")
    finally:
        consumer.close()
        producer.close()
