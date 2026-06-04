# Tarea 2: Procesamiento y Fallback con Apache Kafka

## 🔗 Links
- **Video de demostración:** (pendiente)
- **Repositorio:** https://github.com/martinnmg1809/Tarea-1-sistemas-distribuidos

---

## 📄 Resumen

Esta entrega evoluciona la arquitectura de la Tarea 1 incorporando **Apache Kafka** como sistema de mensajería asíncrona. El objetivo principal es evitar la pérdida de consultas ante fallas temporales del sistema y mejorar la tolerancia a fallos mediante colas de reintento y una Dead Letter Queue (DLQ).

---

## 🏗️ Arquitectura

```
Generador de Tráfico
        │
        ▼ (publica consultas)
   Kafka Topic: consultas (3 particiones)
        │
        ▼ (consumen)
  Consumidores Kafka ──► Sistema Caché (Redis)
        │                      │
        │               HIT ◄──┘
        │               MISS
        │                │
        ▼                ▼
  Generador de Respuestas
        │
     ¿Éxito?
     Sí ──► Métricas
     No ──► Topic: consultas-retry
                │
          ¿retry_count >= MAX_RETRIES?
          Sí ──► Topic: consultas-dlq (DLQ)
          No ──► reintenta
```

---

## 🗂️ Componentes

| Servicio | Descripción |
|---|---|
| `kafka` | Broker Kafka 3.7.0 en modo KRaft (sin Zookeeper) |
| `kafka-init` | Inicializa los 3 tópicos al arrancar |
| `generador-trafico` | Publica consultas Q1–Q5 en Kafka (modo `kafka`) o llama directo al caché (modo `sincrono`) |
| `consumidor-kafka` | Lee del tópico `consultas`, consulta caché y generador de respuestas |
| `consumidor-retry` | Lee del tópico `consultas-retry` y reintenta el procesamiento |
| `sistema-cache` | Proxy Redis con política LFU, TTL 5 min, 50MB |
| `generador-respuestas` | Procesa consultas geoespaciales sobre el dataset |
| `almacenamiento-metricas` | Registra eventos HIT, MISS, RETRY, DLQ en CSV |

---

## 📨 Tópicos Kafka

| Tópico | Particiones | Descripción |
|---|---|---|
| `consultas` | 3 | Tópico principal — soporta hasta 3 consumidores en paralelo |
| `consultas-retry` | 1 | Consultas fallidas que se reintentan |
| `consultas-dlq` | 1 | Dead Letter Queue — consultas irrecuperables |

---

## ⚙️ Configuración de Reintentos

| Parámetro | Valor | Descripción |
|---|---|---|
| `MAX_RETRIES` | 3 | Intentos máximos antes de enviar a DLQ |
| `RETRY_DELAY_MS` | 500 | Espera entre reintentos (ms) |

**Política:** ante cualquier falla en el procesamiento (timeout, error HTTP, servicio caído), la consulta se reenvía al tópico `consultas-retry` incrementando su `retry_count`. Al alcanzar `MAX_RETRIES`, se envía a la DLQ.

---

## 🚀 Guía de Ejecución

### Modo Kafka (Tarea 2)
```bash
# Asegurarse de que MODO=kafka en docker-compose.yml
sudo docker compose up --build
```

### Modo Síncrono (Tarea 1)
```bash
# Cambiar MODO=sincrono en docker-compose.yml
sudo docker compose up redis-db sistema-cache generador-respuestas almacenamiento-metricas generador-trafico
```

### Escalar consumidores
```bash
sudo docker compose up --scale consumidor-kafka=3
```

---

## 🧪 Escenarios de Evaluación

| # | Script | Descripción |
|---|---|---|
| 1 | `escenarios/escenario1_base.sh` | Sistema síncrono base (Tarea 1) |
| 2 | `escenarios/escenario2_kafka_1consumer.sh` | Kafka + 1 consumidor |
| 3 | `escenarios/escenario3_kafka_nconsumers.sh N` | Kafka + N consumidores |
| 4 | `escenarios/escenario4_falla_temporal.sh` | Falla del generador de respuestas |
| 5 | `escenarios/escenario5_reintentos.sh` | Fallas intermitentes con reintentos |
| 6 | `escenarios/escenario6_spike.sh` | Spike de tráfico |
| 7 | `escenarios/escenario7_recuperacion.sh` | Comparación recuperación síncrono vs Kafka |

### Flujo para capturar métricas por escenario
```bash
# 1. Correr el escenario
sudo bash escenarios/escenario2_kafka_1consumer.sh

# 2. Guardar el CSV con nombre del escenario
cp data/metricas_sistema.csv data/metricas_kafka_1c.csv

# 3. Analizar métricas
python3 metricas/analisis.py "Kafka 1 Consumer"

# 4. Generar gráficos comparativos (después de todos los escenarios)
python3 metricas/graficos.py
```

---

## 📊 Métricas Registradas

| Métrica | Descripción |
|---|---|
| Throughput | Consultas procesadas exitosamente por segundo |
| Latencia p50/p95 | Percentiles de tiempo de respuesta |
| Hit Rate | Porcentaje de consultas resueltas desde caché |
| Retry Rate | Consultas reenviadas al tópico de reintento |
| DLQ Rate | Consultas enviadas a la Dead Letter Queue |
| Recovery Rate | Consultas recuperadas exitosamente tras fallos |
| Recovery Time | Tiempo en vaciar la cola de reintentos tras una falla |

---

## 🛠️ Stack Tecnológico

- **Apache Kafka 3.7.0** (modo KRaft, sin Zookeeper)
- **Redis** (caché con política LFU, 50MB)
- **Python 3.12** con Flask, kafka-python-ng, Pandas
- **Docker + Docker Compose**
