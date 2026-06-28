# Dashboards de Kibana — Tarea 3

## Contenido

- `export.ndjson`: Export de Kibana con el dashboard **"Tarea 3 - Monitoreo en Tiempo Real"**, sus 4 visualizaciones y el data view `metrics-aggregated`.

## Visualizaciones incluidas

| Visualización | Métrica | Qué muestra |
|---|---|---|
| Throughput por Minuto | `throughput_por_min` | Consultas exitosas procesadas por minuto, en ventanas deslizantes de 30s |
| Latencia p50/p95 | `p50_ms`, `p95_ms` | Percentiles de tiempo de respuesta por ventana |
| Hit Rate | `hit_rate` | Proporción de consultas resueltas desde caché |
| Retry Rate y DLQ Rate | `retry_rate`, `dlq_rate` | Proporción de consultas reintentadas / enviadas a Dead Letter Queue |

## Cómo importar este dashboard

### Requisitos previos
1. El sistema completo debe estar corriendo (`docker compose up`), incluyendo Elasticsearch, Kibana y Spark Streaming.
2. El índice `metrics-aggregated` debe existir en Elasticsearch (se crea automáticamente cuando Spark escribe el primer batch).

### Pasos
1. Abre Kibana en `http://localhost:5601`
2. Ve a **Stack Management → Saved Objects**
3. Click en **"Import"** (arriba a la derecha)
4. Selecciona el archivo `kibana/export.ndjson`
5. Si te pregunta sobre conflictos de index pattern, elige **"Create new"** la primera vez, o **"Overwrite"** si ya existe uno con el mismo nombre.
6. Click **"Import"**
7. El dashboard **"Tarea 3 - Monitoreo en Tiempo Real"** aparecerá en **Dashboards**

### Generar datos para ver el dashboard con contenido
Con el sistema corriendo, genera tráfico:
```bash
sudo docker compose run --rm \
  -e MODO=kafka \
  -e DISTRIBUCION=zipf \
  -e TOTAL_PETICIONES=200 \
  -e INTERVALO_SEG=0.1 \
  generador-trafico
```

Ajusta el rango de tiempo en Kibana (esquina superior derecha del dashboard) a **"Last 15 minutes"** con auto-refresh para ver las métricas actualizarse en vivo.
