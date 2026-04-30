import pandas as pd
import numpy as np
from flask import Flask, request, jsonify

app = Flask(__name__)

AREAS_KM2 = {
    "Z1": 5.8, "Z2": 11.2, "Z3": 18.5, "Z4": 8.4, "Z5": 14.1
}

ZONAS_SANTIAGO = {
    "Z1": {"lat": (-33.445, -33.420), "lon": (-70.640, -70.600)}, 
    "Z2": {"lat": (-33.420, -33.390), "lon": (-70.600, -70.550)}, 
    "Z3": {"lat": (-33.530, -33.490), "lon": (-70.790, -70.740)}, 
    "Z4": {"lat": (-33.460, -33.430), "lon": (-70.670, -70.630)}, 
    "Z5": {"lat": (-33.470, -33.430), "lon": (-70.810, -70.760)}  
}

def generar_padding(bytes_objetivo):
    return "x" * bytes_objetivo

def cargar_y_filtrar_dataset():
    print("Cargando dataset en memoria...")
    try:
        ruta_csv = 'data/santiagoCSV.gz' 
        df_full = pd.read_csv(ruta_csv, usecols=['latitude', 'longitude', 'area_in_meters', 'confidence']) 
        
        filtros = []
        for z in ZONAS_SANTIAGO.values():
            condicion = (
                (df_full['latitude'] >= z['lat'][0]) & (df_full['latitude'] <= z['lat'][1]) &
                (df_full['longitude'] >= z['lon'][0]) & (df_full['longitude'] <= z['lon'][1])
            )
            filtros.append(condicion)
        
        df_santiago = df_full[pd.concat(filtros, axis=1).any(axis=1)].copy() 
        print(f"Dataset listo. Edificios en zonas de Santiago: {len(df_santiago)}")
        return df_santiago
    except Exception as e:
        print(f"Error cargando dataset: {e}")
        return pd.DataFrame()

df = cargar_y_filtrar_dataset() 

def calcular_densidad(zona_id, conf_min):
    limites = ZONAS_SANTIAGO.get(zona_id)
    mask = (
        (df['latitude'] >= limites['lat'][0]) & (df['latitude'] <= limites['lat'][1]) &
        (df['longitude'] >= limites['lon'][0]) & (df['longitude'] <= limites['lon'][1]) &
        (df['confidence'] >= conf_min)
    )
    conteo = len(df[mask]) 
    area_km2 = AREAS_KM2.get(zona_id, 1.0)
    return conteo / area_km2 

@app.route('/procesar', methods=['POST'])
def procesar_consulta():
    data = request.json
    tipo = data.get('tipo')
    zona_id = data.get('zona_id')
    conf_min = data.get('params', {}).get('confidence_min', 0.0)
    
    limites = ZONAS_SANTIAGO.get(zona_id)
    if not limites and tipo != "Q4":
        return jsonify({"error": "Zona no válida"}), 400

    mask = (
        (df['latitude'] >= limites['lat'][0]) & (df['latitude'] <= limites['lat'][1]) &
        (df['longitude'] >= limites['lon'][0]) & (df['longitude'] <= limites['lon'][1]) &
        (df['confidence'] >= conf_min)
    ) if limites else None
    
    df_zona = df[mask] if mask is not None else None

    if tipo == "Q1":
        resultado = {"count": int(len(df_zona))}

    elif tipo == "Q2":
        resultado = {
            "avg_area": float(df_zona['area_in_meters'].mean()) if not df_zona.empty else 0,
            "total_area": float(df_zona['area_in_meters'].sum()),
            "n": int(len(df_zona))
        }

    elif tipo == "Q3":
        densidad = calcular_densidad(zona_id, conf_min)
        resultado = {"density": densidad}

    elif tipo == "Q4":
        zona_b = data.get('zona_id_b')
        dens_a = calcular_densidad(zona_id, conf_min) 
        dens_b = calcular_densidad(zona_b, conf_min) 
        resultado = {
            "zone_a": dens_a, "zone_b": dens_b,
            "winner": zona_id if dens_a > dens_b else zona_b 
        }

    elif tipo == "Q5":
        bins = data.get('params', {}).get('bins', 5) 
        scores = df_zona['confidence'].tolist() 
        counts, edges = np.histogram(scores, bins=bins, range=(0, 1)) 
        
        dist = []
        for i in range(len(counts)):
            dist.append({
                "bucket": i, "min": float(edges[i]),
                "max": float(edges[i+1]), "count": int(counts[i])
            }) 
        resultado = {"distribution": dist}

    pesos = {
        "Q1": 50000,    # 50 KB
        "Q2": 100000,   # 100 KB
        "Q3": 100000,   # 100 KB
        "Q4": 50000,    # 50 KB
        "Q5": 300000    # 300 KB 
    }

    peso_adicional = pesos.get(tipo, 100)
    
    resultado_con_peso = {
        "resultado": resultado,
        "padding": generar_padding(peso_adicional) 
    }
    
    return jsonify(resultado_con_peso)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)