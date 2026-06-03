#!/bin/bash
# Escenario 6: Spike de tráfico
echo "=== Escenario 6: Spike de Tráfico ==="

cd "$(dirname "$0")/.."
> data/metricas_sistema.csv

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas \
  consumidor-kafka consumidor-retry

echo "Esperando servicios..."
sleep 15

# Fase 1: Tráfico normal
echo "--- Fase 1: Tráfico normal (100 consultas, 0.1s intervalo) ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico

# Fase 2: SPIKE - ráfaga masiva de consultas
echo "--- Fase 2: SPIKE (300 consultas, 0.01s intervalo) ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=uniform \
  -e TOTAL_PETICIONES=300 \
  -e INTERVALO_SEG=0.01 \
  generador-trafico

# Fase 3: Vuelta a tráfico normal
echo "--- Fase 3: Vuelta a tráfico normal (100 consultas) ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico

echo "Esperando que el backlog se vacíe..."
sleep 30

docker compose down
echo "=== Escenario 6 finalizado. Ejecuta: python3 metricas/analisis.py 'Spike Trafico' ==="
