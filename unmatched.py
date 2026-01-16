import pandas as pd

# Columnas oficiales de dim_productos
DIM_PRODUCTOS_COLS = [
    "producto", "proveedor", "categoria", "marca",
    "clasificacion", "presentacion", "variedad", "talle", "segmento",
    "promocion", "ean", "gramaje", "sub_segmento", "sub_marca",
    "sabor", "especialidad", "sub_categoria", "para_escritura"
]

def build_unmatched_prod(unmatched_df: pd.DataFrame, categoria: str) -> pd.DataFrame:
    """
    Construye un DataFrame con las mismas columnas que siso.dim_biggie_productos,
    usando los datos disponibles del CSV y rellenando el resto con NA.
    """
    # Mapeo CSV → dim_productos
    mapping = {
        "descripcion": "producto",
        "proveedor": "proveedor",
        "familia": "categoria",   # familia en CSV = categoria en dim
        "marca": "marca",
        "clasificacion": "clasificacion",
        "codigo de barras": "ean",
        "gramaje": "gramaje"
    }

    # Crear DataFrame con las columnas esperadas
    prod_df = pd.DataFrame(columns=DIM_PRODUCTOS_COLS)

    # Cargar valores conocidos
    for csv_col, dim_col in mapping.items():
        if csv_col in unmatched_df.columns:
            prod_df[dim_col] = unmatched_df[csv_col]
    
    # Rellenar categoría fija si corresponde
    prod_df["categoria"] = categoria.upper()

    # Asegurar que existan todas las columnas
    for col in DIM_PRODUCTOS_COLS:
        if col not in prod_df.columns:
            prod_df[col] = pd.NA

    # Quitar duplicados por EAN
    prod_df = prod_df.drop_duplicates(subset="ean")

    prod_df["para_escritura"] = False  # dtype bool
    prod_df["para_escritura"] = prod_df["para_escritura"].astype(bool)

    return prod_df[DIM_PRODUCTOS_COLS]  # orden correcto

# Columnas oficiales de dim_biggie_locales
DIM_LOCALES_COLS = [
    "id_sucursal_bg", "nombre_local", "latitud", "longitud", "ciudad",
    "cod_cliente_palermo", "autocompra", "compra_balanceado",
    "AC_yerba_exhibidores", "ac_pod_cigarreras", "para_escritura"
]

def build_unmatched_loc(unmatched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye un DataFrame con la misma estructura que dbo.dim_biggie_locales,
    usando los datos disponibles del CSV y rellenando el resto con NA.
    """
    # Mapeo CSV → dim_locales
    mapping = {
        "id_sucursal": "id_sucursal_bg",
        "sucursal": "nombre_local"
    }

    loc_df = pd.DataFrame(columns=DIM_LOCALES_COLS)

    # Cargar valores que vienen en CSV
    for csv_col, dim_col in mapping.items():
        if csv_col in unmatched_df.columns:
            loc_df[dim_col] = unmatched_df[csv_col]

    # Rellenar con NA las columnas que no vienen en CSV
    for col in DIM_LOCALES_COLS:
        if col not in loc_df.columns:
            loc_df[col] = pd.NA

    # Quitar duplicados por ID sucursal
    loc_df = loc_df.drop_duplicates(subset="id_sucursal_bg")

    loc_df["para_escritura"] = False
    loc_df["para_escritura"] = loc_df["para_escritura"].astype(bool)

    return loc_df[DIM_LOCALES_COLS]  # asegurar orden correcto
