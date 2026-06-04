#!/bin/bash
echo "=== Escenario 4: Falla Temporal (FAILURE_RATE=0.7) ==="

cd "$(dirname "$0")/.."

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas consumidor-retry

echo "Esperando servicios..."
sleep 15

# Levantar consumidor con 70% de tasa de fallo simulada
docker compose run -d --name consumidor-falla \
  -e KAFKA_BROKER=kafka:9092 \
  -e CACHE_URL=http://sistema-cache:8000/consultar \
  -e RESPUESTAS_URL=http://generador-respuestas:5000/procesar \
  -e METRICAS_URL=http://almacenamiento-metricas:9000/registrar \
  -e MAX_RETRIES=3 \
  -e RETRY_DELAY_MS=500 \
  -e FAILURE_RATE=0.7 \
  consumidor-kafka

sleep 5

echo "--- Generando 300 consultas con falla simulada ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=300 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico

echo "Esperando procesamiento del backlog y reintentos..."
sleep 40

docker stop consumidor-falla 2>/dev/null
docker rm consumidor-falla 2>/dev/null
docker compose down
echo "=== Escenario 4 finalizado ==="
