#!/bin/bash
# Escenario 4: Falla temporal del Generador de Respuestas
echo "=== Escenario 4: Falla Temporal del Generador de Respuestas ==="

cd "$(dirname "$0")/.."
> data/metricas_sistema.csv

docker compose up -d kafka kafka-init redis-db sistema-cache \
  generador-respuestas almacenamiento-metricas \
  consumidor-kafka consumidor-retry

echo "Esperando servicios..."
sleep 15

# Generar tráfico inicial (sistema funcionando bien)
echo "--- Fase 1: Sistema normal (100 consultas) ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=100 \
  -e INTERVALO_SEG=0.05 \
  generador-trafico

# Simular falla: detener el generador de respuestas
echo "--- Fase 2: FALLA - Deteniendo generador-respuestas ---"
docker compose stop generador-respuestas

# Generar tráfico durante la falla (irán a retry/DLQ)
echo "--- Fase 3: Tráfico durante falla (150 consultas) ---"
docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=150 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico

# Recuperar el servicio
echo "--- Fase 4: RECUPERACIÓN - Reiniciando generador-respuestas ---"
docker compose start generador-respuestas
sleep 10

echo "Esperando que el sistema procese las consultas pendientes..."
sleep 30

docker compose down
echo "=== Escenario 4 finalizado. Ejecuta: python3 metricas/analisis.py 'Falla Temporal' ==="
