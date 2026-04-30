# Análisis de Rendimiento en Memoria Caché
## 🔗 Link del video 
https://youtu.be/5nWnmfqwd0o

## 📄 Resumen
Este proyecto presenta el diseño, implementación y evaluación de un sistema distribuido escalable para el procesamiento de datos urbanos masivos. La arquitectura utiliza microservicios orquestados en contenedores para realizar consultas analíticas sobre un dataset de edificaciones en la Región Metropolitana de Santiago, implementando estrategias de **Cache-Aside** y políticas de reemplazo de datos para optimizar la eficiencia del sistema.

## 🏗️ Arquitectura y Componentes

El ecosistema está fragmentado en cuatro componentes desacoplados que interactúan a través de una red interna de **Docker**:

* **Generador de Tráfico Analítico (`trafico.py`)**: Emulador de carga de trabajo que genera peticiones concurrentes utilizando distribuciones probabilísticas (**Zipf** y **Uniforme**). Permite validar la localidad de referencia y el comportamiento del sistema ante tráfico sesgado o aleatorio.
* **Orquestador de Caché (`cache.py`)**: Actúa como un *Proxy* de baja latencia. Implementa la lógica de búsqueda en **Redis** y gestiona la comunicación con el backend solo en caso de *cache miss*, minimizando el costo computacional.
* **Motor de Cómputo (`respuestas.py`)**: Backend encargado del procesamiento de datos pesados. Utiliza **Pandas** y **NumPy** para realizar cálculos geoespaciales, filtrado por confianza y análisis de histogramas sobre el dataset cargado en memoria volátil.
* **Sumidero de Telemetría (`metricas.py`)**: Servicio de monitoreo asíncrono que persiste los registros de cada transacción (timestamps, fuentes y latencias) en archivos CSV para su posterior auditoría.



## 🛠️ Especificaciones Técnicas

### 📈 Consultas Soportadas (Q1-Q5)
El sistema resuelve operaciones de agregación y comparación:
1.  **Conteo (Q1)** y **Cálculo de Áreas (Q2)** mediante filtrado por Bounding Box.
2.  **Densidad Urbana (Q3)** y **Análisis Comparativo (Q4)** entre zonas geográficas.
3.  **Distribución de Confianza (Q5)** mediante la generación de histogramas dinámicos.

### 💾 Estrategia de Gestión de Memoria
Se implementó un mecanismo de **Padding** (relleno de carga útil) para simular objetos de gran tamaño, permitiendo estresar las políticas de reemplazo de Redis (**LRU - Least Recently Used**) incluso con volúmenes controlados de peticiones.



## 🧪 Metodología de Evaluación

Para el análisis crítico, se utiliza el script `analisis.py`, el cual procesa los datos crudos y calcula métricas fundamentales de sistemas distribuidos:

* **Hit Rate**: Eficacia de la capa de almacenamiento temporal.
* **Throughput**: Tasa de transferencia efectiva del sistema (Consultas/Seg).
* **Percentiles de Latencia (p50/p95)**: Caracterización del tiempo de respuesta, identificando cuellos de botella en el peor escenario (p95).
* **Cache Efficiency**: Métrica de costo-beneficio que pondera el ahorro de tiempo frente a la penalización por acceso a la base de datos.
* **Eviction Rate**: Frecuencia de expulsión de datos en la caché por saturación de memoria.

## 🚀 Guía de Ejecución

1.  **Despliegue de Infraestructura**:
    ```bash
    sudo docker compose up --build
    ```
2.  **Configuración de Escenarios**: 
    Modificar el parámetro `maxmemory` en `docker-compose.yml` para evaluar límites de 50MB, 200MB y 500MB.
3.  **Extracción de Resultados**:
    Tras completar el ciclo de peticiones, ejecutar el análisis de métricas:
    ```bash
    python3 analisis.py
    ```
