# farma_mapping.py
import os
from typing import Dict, Tuple
import pandas as pd

FARMA_MAPEO_PATH = os.getenv(
    "FARMA_MAPEO_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapeo_biggie_farma.xlsx")
)

def load_farma_mapping(path: str) -> Dict[str, Tuple[str, str]]:
    if not os.path.exists(path):
        print(f"[FARMA] Archivo de mapeo no encontrado: {path}. Se omite el reemplazo.")
        return {}
    df = pd.read_excel(path, dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    mapping: Dict[str, Tuple[str, str]] = {}
    for _, row in df.iterrows():
        nuevo  = str(row.get("id_sucursal_nuevo", "") or "").strip()
        viejo  = str(row.get("id_sucursal_viejo", "") or "").strip()
        nombre = str(row.get("nombre", "") or "").strip()
        if nuevo and viejo:
            mapping[nuevo] = (viejo, nombre)
    print(f"[FARMA] Mapeo cargado: {len(mapping)} sucursales FARMA reconocidas.")
    return mapping

def apply_farma_mapping(df: pd.DataFrame, mapping: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    if not mapping:
        return df
    df = df.copy()
    id_col     = "id_sucursal"
    nombre_col = next((c for c in df.columns if "nombre" in c.lower() and "local" in c.lower()), None)
    df[id_col] = df[id_col].astype(str).str.strip()
    reemplazados = df[id_col].isin(mapping).sum()
    if reemplazados:
        print(f"[FARMA] Reemplazando {reemplazados} filas con IDs FARMA → IDs históricos.")
    df[id_col] = df[id_col].apply(lambda v: mapping[v][0] if v in mapping else v)
    if nombre_col:
        df[nombre_col] = df.apply(
            lambda row: mapping[str(row[id_col]).strip()][1]
            if str(row[id_col]).strip() in mapping else row[nombre_col],
            axis=1
        )
    return df

FARMA_MAPPING: Dict[str, Tuple[str, str]] = load_farma_mapping(FARMA_MAPEO_PATH)