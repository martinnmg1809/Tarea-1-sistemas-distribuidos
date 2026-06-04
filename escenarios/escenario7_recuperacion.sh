#!/bin/bash
# Escenario 7: Comparación recuperación síncrono vs Kafka
echo "=== Escenario 7: Recuperación ante Fallos - Síncrono vs Kafka ==="

cd "$(dirname "$0")/.."

# ── PARTE A: Sistema síncrono (Tarea 1) ──────────────────
echo ""
echo "--- PARTE A: Sistema SÍNCRONO ---"
> data/metricas_sistema.csv

docker compose up -d redis-db sistema-cache generador-respuestas almacenamiento-metricas
sleep 10

echo "Fase normal síncrona (100 consultas)..."
docker compose run --rm \
  -e MODO=sincrono \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "Simulando falla en síncrono (detener generador-respuestas)..."
docker compose stop generador-respuestas

echo "Tráfico durante falla síncrona (100 consultas - se pierden)..."
docker compose run --rm \
  -e MODO=sincrono \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

cp data/metricas_sistema.csv data/metricas_sincrono.csv
echo "Métricas síncrono guardadas en data/metricas_sincrono.csv"
docker compose down

# ── PARTE B: Sistema Kafka ────────────────────────────────
echo ""
echo "--- PARTE B: Sistema KAFKA ---"
> data/metricas_sistema.csv

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas \
  consumidor-kafka consumidor-retry
sleep 15

echo "Fase normal Kafka (100 consultas)..."
docker compose run --rm \
  -e MODO=kafka \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "Simulando falla en Kafka (detener generador-respuestas)..."
docker compose stop generador-respuestas

echo "Tráfico durante falla Kafka (100 consultas - van a retry)..."
docker compose run --rm \
  -e MODO=kafka \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "Recuperando servicio..."
docker compose start generador-respuestas
sleep 30

cp data/metricas_sistema.csv data/metricas_kafka.csv
echo "Métricas Kafka guardadas en data/metricas_kafka.csv"

docker compose down

echo ""
echo "=== Escenario 7 finalizado ==="
echo "Comparar con:"
echo "  python3 metricas/analisis.py 'Sincrono'  (usando data/metricas_sincrono.csv)"
echo "  python3 metricas/analisis.py 'Kafka'     (usando data/metricas_kafka.csv)"
