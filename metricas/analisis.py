import pandas as pd
import numpy as np
import redis
import os
import sys

ARCHIVO_METRICAS = os.getenv("METRICAS_FILE", "data/metricas_sistema.csv")

def obtener_evictions():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        info = r.info('stats')
        return int(info.get('evicted_keys', 0))
    except:
        return 0

def analizar_experimento(escenario=""):
    if not os.path.exists(ARCHIVO_METRICAS):
        print(f"Error: No existe {ARCHIVO_METRICAS}")
        return

    df = pd.read_csv(ARCHIVO_METRICAS)
    df.columns = df.columns.str.strip()

    if 'evento' not in df.columns:
        print(f"Error: Columnas mal formadas. Detectadas: {list(df.columns)}")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # ── Métricas base ──────────────────────────────────────
    total   = len(df)
    hits    = len(df[df['evento'] == 'HIT'])
    misses  = len(df[df['evento'] == 'MISS'])
    retries = len(df[df['evento'] == 'RETRY'])
    dlq     = len(df[df['evento'] == 'DLQ'])
    exitosos = hits + misses  # procesados correctamente

    hit_rate      = hits / total if total > 0 else 0
    retry_rate    = retries / total if total > 0 else 0
    dlq_rate      = dlq / total if total > 0 else 0
    recovery_rate = (retries - dlq) / retries if retries > 0 else 0

    # ── Throughput ─────────────────────────────────────────
    tiempo_total_seg = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
    throughput = exitosos / tiempo_total_seg if tiempo_total_seg > 0 else 0

    # ── Latencias ──────────────────────────────────────────
    p50 = df['latencia_ms'].median()
    p95 = df['latencia_ms'].quantile(0.95)

    p50_hit  = df[df['evento'] == 'HIT']['latencia_ms'].median() if hits > 0 else 0
    p50_miss = df[df['evento'] == 'MISS']['latencia_ms'].median() if misses > 0 else 0

    # ── Evictions Redis ────────────────────────────────────
    evictions_totales = obtener_evictions()
    tiempo_total_min  = tiempo_total_seg / 60
    eviction_rate     = evictions_totales / tiempo_total_min if tiempo_total_min > 0 else 0

    # ── Cache efficiency ───────────────────────────────────
    t_cache = df[df['evento'] == 'HIT']['latencia_ms'].mean() if hits > 0 else 0
    t_db    = df[df['evento'] == 'MISS']['latencia_ms'].mean() if misses > 0 else 0
    cache_efficiency = ((hits * t_cache) - (misses * t_db)) / total if total > 0 else 0

    # ── Recovery time (tiempo en vaciar cola tras fallo) ───
    if retries > 0:
        retry_df = df[df['evento'] == 'RETRY']
        recovery_time_seg = (retry_df['timestamp'].max() - retry_df['timestamp'].min()).total_seconds()
    else:
        recovery_time_seg = 0

    # ── Imprimir resultados ────────────────────────────────
    titulo = f"ESCENARIO: {escenario}" if escenario else "RESULTADOS DEL EXPERIMENTO"
    print("\n" + "=" * 55)
    print(f"  {titulo}")
    print("=" * 55)
    print(f"{'Métrica':<25} | {'Valor':<25}")
    print("-" * 55)
    print(f"{'Total eventos':<25} | {total:,}")
    print(f"{'Exitosos (HIT+MISS)':<25} | {exitosos:,}")
    print(f"{'Reintentos (RETRY)':<25} | {retries:,}")
    print(f"{'Dead Letter Queue':<25} | {dlq:,}")
    print("-" * 55)
    print(f"{'Hit Rate':<25} | {hit_rate:.4f} ({hit_rate*100:.2f}%)")
    print(f"{'Retry Rate':<25} | {retry_rate:.4f} ({retry_rate*100:.2f}%)")
    print(f"{'DLQ Rate':<25} | {dlq_rate:.4f} ({dlq_rate*100:.2f}%)")
    print(f"{'Recovery Rate':<25} | {recovery_rate:.4f} ({recovery_rate*100:.2f}%)")
    print("-" * 55)
    print(f"{'Throughput':<25} | {throughput:.2f} req/seg")
    print(f"{'Latencia p50 (total)':<25} | {p50:.2f} ms")
    print(f"{'Latencia p95 (total)':<25} | {p95:.2f} ms")
    print(f"{'Latencia p50 HIT':<25} | {p50_hit:.2f} ms")
    print(f"{'Latencia p50 MISS':<25} | {p50_miss:.2f} ms")
    print(f"{'Recovery time':<25} | {recovery_time_seg:.2f} seg")
    print("-" * 55)
    print(f"{'Eviction Rate':<25} | {eviction_rate:.2f} evic/min")
    print(f"{'Cache Efficiency':<25} | {cache_efficiency:.4f}")
    print("=" * 55)
    print(f"Duración total: {tiempo_total_seg:.1f} seg | Registros: {total:,}")

if __name__ == "__main__":
    escenario = sys.argv[1] if len(sys.argv) > 1 else ""
    analizar_experimento(escenario)
