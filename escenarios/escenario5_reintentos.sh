#!/bin/bash
echo "=== Escenario 5: Reintentos con Falla Intermitente ==="

cd "$(dirname "$0")/.."

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas consumidor-retry

echo "Esperando servicios..."
sleep 15

# Consumidor con 50% de fallo para forzar reintentos pero también recuperaciones
docker compose run -d --name consumidor-reintentos \
  -e KAFKA_BROKER=kafka:9092 \
  -e CACHE_URL=http://sistema-cache:8000/consultar \
  -e RESPUESTAS_URL=http://generador-respuestas:5000/procesar \
  -e METRICAS_URL=http://almacenamiento-metricas:9000/registrar \
  -e MAX_RETRIES=3 \
  -e RETRY_DELAY_MS=500 \
  -e FAILURE_RATE=0.5 \
  consumidor-kafka

sleep 5

echo "--- Generando 300 consultas con falla intermitente ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=300 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico

echo "Esperando procesamiento de reintentos..."
sleep 40

docker stop consumidor-reintentos 2>/dev/null
docker rm consumidor-reintentos 2>/dev/null
docker compose down
echo "=== Escenario 5 finalizado ==="
