import os
import json
import time
import uuid
import random
import requests
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
CACHE_URL      = os.getenv("CACHE_URL", "http://sistema-cache:8000/consultar")
RESPUESTAS_URL = os.getenv("RESPUESTAS_URL", "http://generador-respuestas:5000/procesar")
METRICAS_URL   = os.getenv("METRICAS_URL", "http://almacenamiento-metricas:9000/registrar")
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "500"))
CONSUMER_MODE  = os.getenv("CONSUMER_MODE", "main")
FAILURE_RATE   = float(os.getenv("FAILURE_RATE", "0.0"))  # 0.0 a 1.0

TOPIC_MAIN  = "consultas"
TOPIC_RETRY = "consultas-retry"
TOPIC_DLQ   = "consultas-dlq"
GROUP_ID    = "consumidores-principales" if CONSUMER_MODE == "main" else "consumidores-retry"

def registrar_metrica(evento, key, latencia_ms, fuente, extra=None):
    payload = {"evento": evento, "key": key, "latencia_ms": latencia_ms, "fuente": fuente}
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
                acks="all", retries=5
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
    start   = time.time()
    consulta = msg.value

    if "id" not in consulta:
        consulta["id"] = str(uuid.uuid4())
    if "retry_count" not in consulta:
        consulta["retry_count"] = 0

    cid         = consulta["id"]
    retry_count = consulta["retry_count"]
    tipo        = consulta.get("tipo", "?")
    zona        = consulta.get("zona_id", "?")

    print(f"[{CONSUMER_MODE}] Procesando {cid} | tipo={tipo} zona={zona} retry={retry_count}")

    # Simular falla aleatoria si FAILURE_RATE > 0
    if FAILURE_RATE > 0 and random.random() < FAILURE_RATE:
        latencia_ms = (time.time() - start) * 1000
        print(f"[{CONSUMER_MODE}] FALLA SIMULADA {cid}")
        enviar_a_retry_o_dlq(consulta, cid, tipo, zona, retry_count, latencia_ms, producer)
        return

    try:
        r = requests.post(CACHE_URL, json=consulta, timeout=5)
        data = r.json()
        latencia_ms = (time.time() - start) * 1000

        if data.get("fuente") == "cache":
            print(f"[{CONSUMER_MODE}] HIT  {cid} ({latencia_ms:.1f}ms)")
            registrar_metrica("HIT", f"{tipo}:{zona}", latencia_ms, "cache",
                              {"consulta_id": cid, "retry_count": retry_count})
        else:
            print(f"[{CONSUMER_MODE}] MISS {cid} ({latencia_ms:.1f}ms)")
            registrar_metrica("MISS", f"{tipo}:{zona}", latencia_ms, "generador_respuestas",
                              {"consulta_id": cid, "retry_count": retry_count})

    except Exception as e:
        latencia_ms = (time.time() - start) * 1000
        print(f"[{CONSUMER_MODE}] FALLO {cid} retry={retry_count}/{MAX_RETRIES} → {e}")
        enviar_a_retry_o_dlq(consulta, cid, tipo, zona, retry_count, latencia_ms, producer)

def enviar_a_retry_o_dlq(consulta, cid, tipo, zona, retry_count, latencia_ms, producer):
    if retry_count < MAX_RETRIES:
        consulta["retry_count"] += 1
        time.sleep(RETRY_DELAY_MS / 1000)
        producer.send(TOPIC_RETRY, value=consulta)
        producer.flush()
        registrar_metrica("RETRY", f"{tipo}:{zona}", latencia_ms, "retry",
                          {"consulta_id": cid, "retry_count": consulta["retry_count"]})
        print(f"[{CONSUMER_MODE}] → Retry ({consulta['retry_count']}/{MAX_RETRIES})")
    else:
        producer.send(TOPIC_DLQ, value=consulta)
        producer.flush()
        registrar_metrica("DLQ", f"{tipo}:{zona}", latencia_ms, "dlq",
                          {"consulta_id": cid, "retry_count": retry_count})
        print(f"[{CONSUMER_MODE}] → DLQ (agotó {MAX_RETRIES} reintentos)")

if __name__ == "__main__":
    topic = TOPIC_RETRY if CONSUMER_MODE == "retry" else TOPIC_MAIN
    print(f"[consumidor] modo={CONSUMER_MODE} topic={topic} failure_rate={FAILURE_RATE}")

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
