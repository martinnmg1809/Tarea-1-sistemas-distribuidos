import pandas as pd
import numpy as np
import redis
import os

ARCHIVO_METRICAS = "/home/keso/Escritorio/T1_SD/data/metricas_sistema.csv"

def obtener_evictions():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        info = r.info('stats')
        return int(info.get('evicted_keys', 0))
    except:
        return 0

def analizar_experimento():
    if not os.path.exists(ARCHIVO_METRICAS):
        print(f"Error: No existe {ARCHIVO_METRICAS}")
        return
    df = pd.read_csv(ARCHIVO_METRICAS)
    df.columns = df.columns.str.strip()

    if 'evento' not in df.columns:
        print(f"Error: Columnas mal formadas. Detectadas: {list(df.columns)}")
        return
    
    total = len(df)
    hits = len(df[df['evento'] == 'HIT'])
    misses = len(df[df['evento'] == 'MISS'])

    hit_rate = hits / total if total > 0 else 0
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    tiempo_total_seg = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
    throughput = total / tiempo_total_seg if tiempo_total_seg > 0 else 0
    
    p50 = df['latencia_ms'].median()
    p95 = df['latencia_ms'].quantile(0.95)
    
    evictions_totales = obtener_evictions()
    tiempo_total_min = tiempo_total_seg / 60
    eviction_rate = evictions_totales / tiempo_total_min if tiempo_total_min > 0 else 0
    
    t_cache = df[df['evento'] == 'HIT']['latencia_ms'].mean() if hits > 0 else 0
    t_db = df[df['evento'] == 'MISS']['latencia_ms'].mean() if misses > 0 else 0
    cache_efficiency = ((hits * t_cache) - (misses * t_db)) / total if total > 0 else 0

    print("\n" + "="*45)
    print("      RESULTADOS FINALES DEL EXPERIMENTO")
    print("="*45)
    print(f"{'Métrica':<20} | {'Valor':<20}")
    print("-" * 45)
    print(f"{'Hit Rate':<20} | {hit_rate:.4f} ({hit_rate*100:.2f}%)")
    print(f"{'Throughput':<20} | {throughput:.2f} req/seg")
    print(f"{'Latencia p50':<20} | {p50:.2f} ms")
    print(f"{'Latencia p95':<20} | {p95:.2f} ms")
    print(f"{'Eviction Rate':<20} | {eviction_rate:.2f} evic/min")
    print(f"{'Cache Efficiency':<20} | {cache_efficiency:.4f}")
    print("="*45)
    print(f"Total de datos analizados: {total:,} registros.")

if __name__ == "__main__":
    analizar_experimento()