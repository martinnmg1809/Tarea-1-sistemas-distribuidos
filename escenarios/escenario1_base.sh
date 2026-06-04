#!/bin/bash
# Escenario 1: Sistema base síncrono (Tarea 1, sin Kafka)
echo "=== Escenario 1: Sistema Base Síncrono ==="

cd "$(dirname "$0")/.."

# Limpiar métricas anteriores
> data/metricas_sistema.csv

# Modo síncrono: el generador llama directo al cache
docker compose run --rm \
  -e MODO=sincrono \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=500 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "=== Escenario 1 finalizado. Ejecuta: python3 metricas/analisis.py 'Base Sincrono' ==="
