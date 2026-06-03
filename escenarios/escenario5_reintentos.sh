#!/bin/bash
# Escenario 5: Reintentos - consultas que fallan antes de resolverse
echo "=== Escenario 5: Reintentos con Falla Intermitente ==="

cd "$(dirname "$0")/.."
> data/metricas_sistema.csv

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas \
  consumidor-kafka consumidor-retry

echo "Esperando servicios..."
sleep 15

# Ciclos de falla y recuperación para forzar reintentos
for i in 1 2 3; do
  echo "--- Ciclo $i: 50 consultas normales ---"
  docker compose run --rm \
    -e MODO=kafka \
    -e DISTRIBUCION=zipf \
    -e TOTAL_PETICIONES=50 \
    -e INTERVALO_SEG=0.05 \
    generador-trafico

  echo "--- Ciclo $i: Falla de 8 segundos ---"
  docker compose stop generador-respuestas
  sleep 8
  docker compose start generador-respuestas
  sleep 5
done

echo "Esperando procesamiento final del backlog..."
sleep 30

docker compose down
echo "=== Escenario 5 finalizado. Ejecuta: python3 metricas/analisis.py 'Reintentos' ==="
