#!/bin/bash
# Escenario 2: Kafka + 1 consumidor
echo "=== Escenario 2: Kafka + 1 Consumidor ==="

cd "$(dirname "$0")/.."
> data/metricas_sistema.csv

# Levantar infraestructura con 1 solo consumidor
docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas

echo "Esperando que los servicios estén listos..."
sleep 15

docker compose up -d --scale consumidor-kafka=1 consumidor-kafka consumidor-retry

sleep 5

# Generar tráfico en modo kafka
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=500 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "Esperando que los consumidores procesen el backlog..."
sleep 20

docker compose down
echo "=== Escenario 2 finalizado. Ejecuta: python3 metricas/analisis.py 'Kafka 1 Consumer' ==="
