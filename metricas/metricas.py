import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime
import csv
import os

app = Flask(__name__)

LOG_FILE = "data/metricas_sistema.csv"
HEADERS = ["timestamp", "evento", "key", "latencia_ms", "fuente", "consulta_id", "retry_count"]

def inicializar_csv():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

# Siempre reinicia el CSV al arrancar el servicio
inicializar_csv()

@app.route('/registrar', methods=['POST'])
def registrar():
    data = request.json
    nuevo_registro = [
        datetime.now().isoformat(),
        data.get('evento'),
        data.get('key'),
        data.get('latencia_ms'),
        data.get('fuente'),
        data.get('consulta_id', ''),
        data.get('retry_count', 0)
    ]
    try:
        with open(LOG_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(nuevo_registro)
            f.flush()
    except Exception as e:
        print(f"Error al escribir en el log: {e}")
        return jsonify({"error": "No se pudo escribir el registro"}), 500
    return jsonify({"status": "registrado"}), 201

@app.route('/resumen', methods=['GET'])
def resumen():
    try:
        df = pd.read_csv(LOG_FILE)
        total   = len(df)
        hits    = len(df[df['evento'] == 'HIT'])
        misses  = len(df[df['evento'] == 'MISS'])
        retries = len(df[df['evento'] == 'RETRY'])
        dlq     = len(df[df['evento'] == 'DLQ'])
        return jsonify({
            "total":      total,
            "hits":       hits,
            "misses":     misses,
            "retries":    retries,
            "dlq":        dlq,
            "hit_rate":   round(hits / total, 4) if total > 0 else 0,
            "retry_rate": round(retries / total, 4) if total > 0 else 0,
            "dlq_rate":   round(dlq / total, 4) if total > 0 else 0,
            "p50_ms":     round(df['latencia_ms'].median(), 2),
            "p95_ms":     round(df['latencia_ms'].quantile(0.95), 2),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000)
