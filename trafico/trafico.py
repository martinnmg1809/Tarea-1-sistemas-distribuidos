import numpy as np
import requests
import time
import random

ZONAS_SANTIAGO = {
    "Z1": {"nombre": "Providencia", "lat": (-33.445, -33.420), "lon": (-70.640, -70.600)},
    "Z2": {"nombre": "Las Condes", "lat": (-33.420, -33.390), "lon": (-70.600, -70.550)},
    "Z3": {"nombre": "Maipú", "lat": (-33.530, -33.490), "lon": (-70.790, -70.740)},
    "Z4": {"nombre": "Santiago Centro", "lat": (-33.460, -33.430), "lon": (-70.670, -70.630)},
    "Z5": {"nombre": "Pudahuel", "lat": (-33.470, -33.430), "lon": (-70.810, -70.760)}
}

def enviar_consulta(distribucion):
    ids_zonas = list(ZONAS_SANTIAGO.keys())
    
    if distribucion == "zipf":
        s = 1.2  
        idx = (np.random.zipf(a=s) - 1) % len(ids_zonas)
    else:
        idx = random.randint(0, len(ids_zonas) - 1)
        
    zona_id = ids_zonas[idx]
    
    tipo_q = random.choice(["Q1", "Q2", "Q3", "Q4", "Q5"])

    payload = {
        "tipo": tipo_q,
        "zona_id": zona_id,
        "params": {
            "confidence_min": round(random.uniform(0.0, 0.9), 2)
        }
    }

    if tipo_q == "Q4":
        zona_b = random.choice([z for z in ids_zonas if z != zona_id])
        payload["zona_id_b"] = zona_b
    
    elif tipo_q == "Q5":
        payload["params"]["bins"] = random.randint(3, 10)

    try:
        response = requests.post("http://sistema-cache:8000/consultar", json=payload, timeout=5)
        print(f"[{distribucion}] Enviada {tipo_q} para {zona_id}. Status: {response.status_code}")
    except Exception as e:
        print(f"Error enviando consulta: {e}")

if __name__ == "__main__":
    time.sleep(10)
    
    TOTAL_PETICIONES = 200
    contador = 0

    while contador < TOTAL_PETICIONES:
        enviar_consulta(distribucion="zipf") 
        #enviar_consulta(distribucion="uniform") 

        contador += 1
        time.sleep(0.1) 