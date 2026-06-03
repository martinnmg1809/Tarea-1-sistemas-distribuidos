"""
graficos.py — Genera gráficos comparativos entre escenarios para el informe.

Uso:
    python3 metricas/graficos.py

Los gráficos se guardan en data/graficos/
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os
import glob

OUTPUT_DIR = "data/graficos"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Paleta de colores consistente ─────────────────────────
COLORES = {
    "Base Sincrono":     "#4C72B0",
    "Kafka 1 Consumer":  "#DD8452",
    "Kafka 2 Consumers": "#55A868",
    "Kafka 3 Consumers": "#C44E52",
    "Falla Temporal":    "#8172B2",
    "Reintentos":        "#937860",
    "Spike Trafico":     "#DA8BC3",
}
COLOR_DEFAULT = "#64B5CD"


def cargar_csv(ruta):
    """Carga un CSV de métricas y lo retorna como DataFrame limpio."""
    df = pd.read_csv(ruta)
    df.columns = df.columns.str.strip()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def calcular_metricas(df, nombre):
    """Calcula todas las métricas de un DataFrame."""
    total    = len(df)
    hits     = len(df[df['evento'] == 'HIT'])
    misses   = len(df[df['evento'] == 'MISS'])
    retries  = len(df[df['evento'] == 'RETRY'])
    dlq      = len(df[df['evento'] == 'DLQ'])
    exitosos = hits + misses

    tiempo_seg = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()

    return {
        "nombre":        nombre,
        "total":         total,
        "hits":          hits,
        "misses":        misses,
        "retries":       retries,
        "dlq":           dlq,
        "hit_rate":      hits / total if total > 0 else 0,
        "retry_rate":    retries / total if total > 0 else 0,
        "dlq_rate":      dlq / total if total > 0 else 0,
        "recovery_rate": (retries - dlq) / retries if retries > 0 else 0,
        "throughput":    exitosos / tiempo_seg if tiempo_seg > 0 else 0,
        "p50":           df['latencia_ms'].median(),
        "p95":           df['latencia_ms'].quantile(0.95),
        "p50_hit":       df[df['evento'] == 'HIT']['latencia_ms'].median() if hits > 0 else 0,
        "p50_miss":      df[df['evento'] == 'MISS']['latencia_ms'].median() if misses > 0 else 0,
        "duracion_seg":  tiempo_seg,
    }


def grafico_throughput(datos):
    """Gráfico de barras: Throughput por escenario."""
    nombres    = [d['nombre'] for d in datos]
    throughput = [d['throughput'] for d in datos]
    colores    = [COLORES.get(n, COLOR_DEFAULT) for n in nombres]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(nombres, throughput, color=colores, edgecolor='white', linewidth=0.8)
    ax.bar_label(bars, fmt='%.1f', padding=3, fontsize=9)
    ax.set_title('Throughput por Escenario (consultas/seg)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Consultas por segundo')
    ax.set_xlabel('Escenario')
    plt.xticks(rotation=20, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/throughput_comparativo.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/throughput_comparativo.png")


def grafico_latencias(datos):
    """Gráfico de barras agrupadas: p50 y p95 por escenario."""
    nombres = [d['nombre'] for d in datos]
    p50     = [d['p50'] for d in datos]
    p95     = [d['p95'] for d in datos]
    x       = np.arange(len(nombres))
    ancho   = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))
    b1 = ax.bar(x - ancho/2, p50, ancho, label='p50', color='#4C72B0', edgecolor='white')
    b2 = ax.bar(x + ancho/2, p95, ancho, label='p95', color='#DD8452', edgecolor='white')
    ax.bar_label(b1, fmt='%.1f', padding=2, fontsize=8)
    ax.bar_label(b2, fmt='%.1f', padding=2, fontsize=8)
    ax.set_title('Latencia p50 y p95 por Escenario (ms)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Latencia (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(nombres, rotation=20, ha='right')
    ax.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/latencias_comparativo.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/latencias_comparativo.png")


def grafico_hit_rate(datos):
    """Gráfico de barras: Hit Rate por escenario."""
    nombres  = [d['nombre'] for d in datos]
    hit_rate = [d['hit_rate'] * 100 for d in datos]
    colores  = [COLORES.get(n, COLOR_DEFAULT) for n in nombres]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(nombres, hit_rate, color=colores, edgecolor='white')
    ax.bar_label(bars, fmt='%.1f%%', padding=3, fontsize=9)
    ax.set_title('Cache Hit Rate por Escenario (%)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Hit Rate (%)')
    ax.set_ylim(0, 100)
    plt.xticks(rotation=20, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/hit_rate_comparativo.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/hit_rate_comparativo.png")


def grafico_retry_dlq(datos):
    """Gráfico de barras apiladas: Retry Rate y DLQ Rate."""
    nombres    = [d['nombre'] for d in datos]
    retry_rate = [d['retry_rate'] * 100 for d in datos]
    dlq_rate   = [d['dlq_rate'] * 100 for d in datos]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(nombres, retry_rate, label='Retry Rate', color='#DD8452', edgecolor='white')
    ax.bar(nombres, dlq_rate, bottom=retry_rate, label='DLQ Rate', color='#C44E52', edgecolor='white')
    ax.set_title('Retry Rate y DLQ Rate por Escenario (%)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Porcentaje de consultas (%)')
    ax.legend()
    plt.xticks(rotation=20, ha='right')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/retry_dlq_comparativo.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/retry_dlq_comparativo.png")


def grafico_latencia_overtime(df, nombre):
    """Gráfico de latencia en el tiempo para un escenario específico."""
    df = df.sort_values('timestamp').copy()
    df['segundos'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds()

    fig, ax = plt.subplots(figsize=(12, 4))
    hits_df  = df[df['evento'] == 'HIT']
    miss_df  = df[df['evento'] == 'MISS']
    retry_df = df[df['evento'] == 'RETRY']

    ax.scatter(hits_df['segundos'],  hits_df['latencia_ms'],
               s=8, alpha=0.5, color='#55A868', label='HIT')
    ax.scatter(miss_df['segundos'],  miss_df['latencia_ms'],
               s=8, alpha=0.5, color='#DD8452', label='MISS')
    if len(retry_df) > 0:
        ax.scatter(retry_df['segundos'], retry_df['latencia_ms'],
                   s=15, alpha=0.8, color='#C44E52', marker='x', label='RETRY')

    ax.set_title(f'Latencia en el Tiempo — {nombre}', fontsize=13, fontweight='bold')
    ax.set_xlabel('Tiempo (segundos)')
    ax.set_ylabel('Latencia (ms)')
    ax.legend()
    plt.tight_layout()
    nombre_archivo = nombre.lower().replace(' ', '_')
    plt.savefig(f"{OUTPUT_DIR}/latencia_tiempo_{nombre_archivo}.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/latencia_tiempo_{nombre_archivo}.png")


def grafico_consumidores_throughput():
    """Gráfico de línea: throughput vs número de consumidores."""
    archivos = {
        1: "data/metricas_kafka_1c.csv",
        2: "data/metricas_kafka_2c.csv",
        3: "data/metricas_kafka_3c.csv",
    }

    consumidores = []
    throughputs  = []

    for n, ruta in archivos.items():
        if os.path.exists(ruta):
            df = cargar_csv(ruta)
            m  = calcular_metricas(df, f"Kafka {n}c")
            consumidores.append(n)
            throughputs.append(m['throughput'])

    if len(consumidores) < 2:
        print("  (Faltan archivos para gráfico de consumidores, omitiendo)")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(consumidores, throughputs, marker='o', linewidth=2,
            color='#4C72B0', markersize=8)
    for x, y in zip(consumidores, throughputs):
        ax.annotate(f'{y:.1f}', (x, y), textcoords="offset points",
                    xytext=(0, 8), ha='center', fontsize=9)
    ax.set_title('Throughput vs Número de Consumidores Kafka', fontsize=13, fontweight='bold')
    ax.set_xlabel('Número de consumidores')
    ax.set_ylabel('Consultas por segundo')
    ax.set_xticks(consumidores)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/throughput_vs_consumidores.png", dpi=150)
    plt.close()
    print(f"  Guardado: {OUTPUT_DIR}/throughput_vs_consumidores.png")


# ── MAIN ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== Generando gráficos comparativos ===")

    # Buscar todos los CSV de métricas guardados
    archivos_escenarios = {
        "Base Sincrono":     "data/metricas_sincrono.csv",
        "Kafka 1 Consumer":  "data/metricas_kafka_1c.csv",
        "Kafka 2 Consumers": "data/metricas_kafka_2c.csv",
        "Kafka 3 Consumers": "data/metricas_kafka_3c.csv",
        "Falla Temporal":    "data/metricas_falla.csv",
        "Reintentos":        "data/metricas_reintentos.csv",
        "Spike Trafico":     "data/metricas_spike.csv",
    }

    datos = []
    dfs   = {}

    for nombre, ruta in archivos_escenarios.items():
        if os.path.exists(ruta):
            df = cargar_csv(ruta)
            m  = calcular_metricas(df, nombre)
            datos.append(m)
            dfs[nombre] = df
            print(f"  Cargado: {ruta} ({len(df)} registros)")
        else:
            print(f"  Omitido (no existe): {ruta}")

    if not datos:
        print("\nNo hay archivos de métricas. Ejecuta los escenarios primero.")
        print("Ejemplo: sudo bash escenarios/escenario2_kafka_1consumer.sh")
        print("Luego copia el CSV: cp data/metricas_sistema.csv data/metricas_kafka_1c.csv")
        exit(0)

    print(f"\nGenerando gráficos con {len(datos)} escenario(s)...")

    # Gráficos comparativos (necesitan al menos 1 escenario)
    grafico_throughput(datos)
    grafico_latencias(datos)
    grafico_hit_rate(datos)
    grafico_retry_dlq(datos)
    grafico_consumidores_throughput()

    # Gráfico de latencia en el tiempo por escenario
    for nombre, df in dfs.items():
        grafico_latencia_overtime(df, nombre)

    print(f"\n✓ Gráficos guardados en {OUTPUT_DIR}/")
    print("  Archivos generados:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"    {f}")
