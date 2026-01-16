import pathlib
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

JOIN_CONFIG = {
    "productos": {
        "dim_table": "siso.dim_biggie_productos",
        "fact_key":  "codigo de barras",         # columna en el CSV/Hecho
        "dim_key":   "ean",                     # columna en la dimensión
        "pad_to":    13,   # EAN-13 (opcional)
    },
    "sucursales": {
        "dim_table": "siso.dim_biggie_locales",  # ajustá al nombre real
        "fact_key":  "id_sucursal",             # columna en el CSV/Hecho
        "dim_key":   "id_sucursal_bg",          # columna en la dimensión
        "pad_to":    None,
    },
}


def read_fact_csv(path: pathlib.Path,
                  encodings=("utf-8", "utf-8-sig", "latin-1", "cp1252"),
                  required_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Lee el CSV:
    - Prueba pocas codificaciones comunes.
    - Autodetecta separador (sep=None + engine='python').
    - Devuelve todo como string.
    - Normaliza nombres de columnas (lower + strip) y recorta espacios en celdas.
    """
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc, sep=None, engine="python")
            break
        except UnicodeDecodeError as e:
            last_err = e
    else:
        raise UnicodeDecodeError("read_fact_csv", b"", 0, 0,
                                 f"No se pudo leer {path} con {encodings}") from last_err

    # normalizaciones mínimas
    df.columns = df.columns.str.lower().str.strip()
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    if required_cols:
        faltantes = [c for c in required_cols if c not in df.columns]
        if faltantes:
            raise KeyError(f"Faltan columnas requeridas: {faltantes}")

    return df


def read_dim_productos(engine) -> pd.DataFrame:
    config = JOIN_CONFIG["productos"]
    dim_table = config["dim_table"]
    # trae solo columnas relevantes; ajusta si necesitas más
    sql = f"""
    SELECT ean, gramaje, presentacion
    FROM {dim_table}
    """
    return pd.read_sql(sql, engine)

def read_dim_sucursales(engine) -> pd.DataFrame:
    config = JOIN_CONFIG["sucursales"]
    dim_table = config["dim_table"]
    # trae solo columnas relevantes; ajusta si necesitas más
    sql = f"""
    SELECT id_sucursal_bg
    FROM {dim_table}
    """
    return pd.read_sql(sql, engine)

def join_validate(
    df_fact,
    df_dim,
    dimension: str,
    how: str = "inner",               # ← default: solo matched (inner join)
    dedup_dim_on_key: bool = True,    # evita explosión si la dim tiene duplicados de clave
    suffixes: tuple[str, str] = ("", "_dim")
):
    """
    Retorna:
      - result_df: merge según 'how' (inner/left/right/outer)
      - unmatched_left: filas del FACT cuya clave no aparece en la DIM (left-anti)
    """
    # 1) elegir config
    if dimension not in JOIN_CONFIG:
        raise ValueError("No pude inferir la dimensión. Usá dimension='productos' o 'sucursales'.")
    cfg = JOIN_CONFIG[dimension]
    left_key, right_key = cfg["fact_key"], cfg["dim_key"]

    if left_key not in df_fact.columns:
        raise KeyError(f"Falta {left_key!r} en fact")
    if right_key not in df_dim.columns:
        raise KeyError(f"Falta {right_key!r} en dim")

    # 2) normalización (mantengo tu criterio)
    fact = df_fact.copy()
    dim  = df_dim.copy()
    # fact[left_key] = fact[left_key].astype(int).astype(str)
    # dim[right_key] = dim[right_key].astype(int).astype(str)

    if dimension == "productos":
        # EAN: mantiene tu criterio (int → str) para resolver notación científica
        fact[left_key] = fact[left_key].astype(int).astype(str)
        dim[right_key] = dim[right_key].astype(int).astype(str)
    else:  # "sucursales"
        # puede haber alfanuméricos (p. ej., E292) → NO int
        fact[left_key] = fact[left_key].astype(str).str.strip()
        dim[right_key] = dim[right_key].astype(str).str.strip()
    # 3) deduplicar dimensión por clave (opcional)
    if dedup_dim_on_key:
        dim = dim.drop_duplicates(subset=[right_key])

    # 4) validar how
    allowed = {"inner", "left", "right", "outer"}
    if how not in allowed:
        raise ValueError(f"'how' debe ser uno de {allowed}. Recibí: {how!r}")

    print(f"JOIN {dimension} [{how}]: {left_key} -> {right_key}")

    # 5) merge principal según 'how'
    result_df = fact.merge(
        dim,
        how=how,
        left_on=left_key,
        right_on=right_key,
        indicator=True,
        suffixes=suffixes
    )

    # 6) cálculo de no matcheados del FACT (left-anti), independiente de 'how'
    #    (usamos las columnas ya normalizadas a str)
    dim_keys = set(dim[right_key])
    unmatched_left = (
        fact[~fact[left_key].isin(dim_keys)]
        .drop_duplicates(subset=[left_key])
        .copy()
    )

    return result_df, unmatched_left


def delete_month_from_table(engine, table: str, start_date: date, end_date: date, date_col: str):
    sql = text(f"""
        DELETE FROM {table}
        WHERE {date_col} >= :start AND {date_col} < :end
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"start": start_date, "end": end_date})

# def insert_dataframe(engine, table: str, df: pd.DataFrame):
#     # Inserción por to_sql (requiere que columnas hagan match con la tabla)
#     df.to_sql(name=table.split('.')[-1], con=engine, if_exists="append", index=False, schema=table.split('.')[0], method="multi")

def insert_dataframe(engine, table: str, df: pd.DataFrame, chunksize: int = 20000):
    """
    Inserta df en SQL Server de forma rápida y simple usando executemany
    (pyodbc + fast_executemany=True). Sin method="multi".
    """
    # NaN -> None para que viajen como NULL
    df = df.where(pd.notna(df), None)

    schema, name = table.split('.', 1)
    df.to_sql(
        name=name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=chunksize,   # 10k–50k suele ir muy bien
        # method=None  # <- default (executemany con fast_executemany activo)
    )