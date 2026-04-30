import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime
import csv
import os

app = Flask(__name__)

LOG_FILE = "data/metricas_sistema.csv"

if not os.path.exists(LOG_FILE):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "evento", "key", "latencia_ms", "fuente"])

@app.route('/registrar', methods=['POST'])
def registrar():
    data = request.json
    evento = data.get('evento')   
    key = data.get('key')         
    latencia = data.get('latencia_ms') 
    fuente = data.get('fuente')    
    
    nuevo_registro = [
        datetime.now().isoformat(),
        evento,
        key,
        latencia,
        fuente
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

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000)