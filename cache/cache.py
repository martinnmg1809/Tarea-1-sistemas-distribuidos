import os
import redis
import requests
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-db')
cache = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

RESPUESTAS_URL = "http://generador-respuestas:5000/procesar"
METRICAS_URL = "http://almacenamiento-metricas:9000/registrar"

def generar_cache_key(data):
    tipo = data.get('tipo')
    zona = data.get('zona_id')
    conf = data.get('params', {}).get('confidence_min', 0.0)
    return f"{tipo}:{zona}:conf={conf}"

@app.route('/consultar', methods=['POST'])
def consultar():
    start_time = time.time() 
    
    data = request.json
    cache_key = generar_cache_key(data)
    
    resultado_cache = cache.get(cache_key)
    
    if resultado_cache:
        evento = "HIT"
        fuente = "cache"
        respuesta_final = resultado_cache
    else:
        evento = "MISS"
        fuente = "generador_respuestas"
        try:
            response = requests.post(RESPUESTAS_URL, json=data, timeout=10)
            respuesta_final = response.json()

            cache.setex(cache_key, 300, str(respuesta_final))  #ttl de 5 minutos
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    latencia_ms = (time.time() - start_time) * 1000

    payload_metrica = {
        "evento": evento,
        "key": cache_key,
        "latencia_ms": latencia_ms,
        "fuente": fuente
    }
    
    try:
        requests.post(METRICAS_URL, json=payload_metrica, timeout=1)
    except:
        print("Error registrando métricas")

    return jsonify({"fuente": fuente, "data": respuesta_final})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)