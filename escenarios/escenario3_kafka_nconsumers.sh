#!/bin/bash
# Escenario 3: Kafka + N consumidores (pasar N como argumento)
N=${1:-3}
echo "=== Escenario 3: Kafka + $N Consumidores ==="

cd "$(dirname "$0")/.."
> data/metricas_sistema.csv

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas

echo "Esperando servicios..."
sleep 15

# Escalar a N consumidores
docker compose up -d --scale consumidor-kafka=$N consumidor-kafka consumidor-retry

sleep 5

docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=500 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

echo "Esperando que los $N consumidores procesen el backlog..."
sleep 20

docker compose down
echo "=== Escenario 3 ($N consumers) finalizado. Ejecuta: python3 metricas/analisis.py 'Kafka ${N} Consumers' ==="
