
import os, io
import re
from decimal import Decimal, InvalidOperation
import math
import pathlib
import pandas as pd
from datetime import date, timedelta
from typing import Dict, Optional, List, Tuple

from datetime import datetime as dt
from datetime import timedelta
import base64

from sqlalchemy import create_engine, text
import urllib.parse

from dotenv import load_dotenv, find_dotenv
# carga .env (busca automáticamente hacia arriba)
load_dotenv(find_dotenv(), override=True)

from auxiliar_date import month_name_es, month_file_candidates, month_boundaries, find_month_csv_for_category
from notifications import get_drive_service_sa, send_email
from upload_to_drive import drive_upload_or_update_xlsx
from in_out_joins import read_fact_csv, read_dim_productos, read_dim_sucursales, join_validate, delete_month_from_table, insert_dataframe
from unmatched import build_unmatched_prod, build_unmatched_loc

# ============ CONFIGURACIÓN ============

def str_to_bool(value: str) -> bool:
    "Transforma un string a booleano"
    return value.lower() in ("true", "1", "yes", "on")


# Carpeta raíz local donde el script de descarga guarda los CSVs por categoría
PRODUCCION_STR   = os.getenv("PRODUCCION", False)
PRODUCCION = str_to_bool(PRODUCCION_STR)
BASE_DOWNLOAD_DIR = r"C:\Users\adrian.garcia\Documents\descargas_drive\biggie"
if PRODUCCION:
    BASE_DOWNLOAD_DIR = os.getenv("BASE_DOWNLOAD_DIR", "/opt/airflow/data/descargas_drive/biggie")

# Mapeo categoría -> tabla destino
CATEGORY_CONFIG = {
    "YERBA":        {"table": "siso.fact_biggie_yerbamate",   "dir": "YERBA"},
    "VAPES":         {"table": "siso.fact_biggie_vapeadores",  "dir": "VAPES"},
    "PANALES":      {"table": "siso.fact_biggie_panales",     "dir": "PANALES"},
    # "CARAMELOS":    {"table": "siso.fact_biggie_golosinas",   "dir": "CARAMELOS"},
    "BALANCEADOS":  {"table": "siso.fact_biggie_balanceados", "dir": "BALANCEADOS"},
    "CIGARRILLOS":  {"table": "siso.fact_biggie_cigarrillos", "dir": "CIGARRILLOS"},
}

# Columna fecha en el CSV/tabla fact (ajusta si tu campo se llama diferente)
FACT_DATE_COLUMN = "fecha"

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

# Si TRUE, no ejecuta DELETE/INSERT; solo muestra lo que haría
DRY_RUN = False

# Si TRUE, inserta únicamente filas que hicieron match con la dimensión; si FALSE, inserta todo
INSERT_ONLY_MATCHED = True

# Nombre de la carpeta donde se guardarán los "no mapeados"
UNMATCHED_SUBFOLDER = "_no_mapeados"

# Conexión a SQL Server por ODBC (rellena o usa variables de entorno)
SQL_SERVER   = os.getenv("SERVER", "")
SQL_DATABASE = os.getenv("DATABASE", "")
SQL_DRIVER   = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
ENCRYPT      = os.getenv("ODBC_ENCRYPT", "yes")             # yes|no
TRUST_CERT   = os.getenv("ODBC_TRUST_SERVER_CERT", "yes")   # yes|no
TIMEOUT      = os.getenv("ODBC_TIMEOUT")                    # ej. "30"

SQL_UID = os.getenv("SQL_USERNAME")
SQL_PWD = os.getenv("SQL_PASSWORD")

SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER   = os.getenv("SMTP_USER")
SMTP_PASS   = os.getenv("SMTP_PASS")
EMAIL_TO    = [addr.strip() for addr in os.getenv("EMAIL_TO", "").split(",") if addr.strip()]

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
DELEGATED_USER       = os.getenv("GOOGLE_DELEGATED_USER")
SCOPES               = ["https://www.googleapis.com/auth/gmail.send"]
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]  # acceso completo a Drive

# FOLDER_ID_NO_MAPEADOS = "1O7YO67At_am2hMwKBkzdbM5Qq3TNHRO6"
FOLDER_ID_NO_MAPEADOS = "1qswVfDCZHlq3XGw383ZHe_Onl_U4hHcl"

# Columnas oficiales de dim_productos
DIM_PRODUCTOS_COLS = [
    "producto", "proveedor", "categoria", "marca",
    "clasificacion", "presentacion", "variedad", "talle", "segmento",
    "promocion", "ean", "gramaje", "sub_segmento", "sub_marca",
    "sabor", "especialidad", "sub_categoria", "para_escritura"
]

# Columnas oficiales de dim_biggie_locales
DIM_LOCALES_COLS = [
    "id_sucursal_bg", "nombre_local", "latitud", "longitud", "ciudad",
    "cod_cliente_palermo", "autocompra", "compra_balanceado",
    "AC_yerba_exhibidores", "ac_pod_cigarreras", "para_escritura"
]

def build_engine():
    parts = [
        f"DRIVER={{{SQL_DRIVER}}}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}",
        f"Encrypt={ENCRYPT}",
        f"TrustServerCertificate={TRUST_CERT}",
    ]

    if SQL_UID and SQL_PWD:
        # SQL Authentication
        parts += [f"UID={SQL_UID}", f"PWD={SQL_PWD}"]
    else:
        # Windows Integrated Security
        parts.append("Trusted_Connection=yes")  # o Integrated Security=SSPI

    if TIMEOUT:
        parts.append(f"Connection Timeout={TIMEOUT}")

    odbc_str = ";".join(parts) + ";"
    params = urllib.parse.quote_plus(odbc_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
    )

def normalize_barcode(value) -> str | None:
    """
    Devuelve solo dígitos del código de barras como string estable,
    manejando floats ('.0') y notación científica (e.g., 7.84007800047E+12).
    No elimina ceros a la izquierda. Si no hay dígitos, devuelve None.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()

    # 1) Notación científica -> entero exacto vía Decimal
    if re.match(r'^\d+(\.\d+)?[eE][+-]?\d+$', s):
        try:
            return str(int(Decimal(s)))
        except (InvalidOperation, ValueError):
            pass

    # 2) Quitar sufijo típico de float ".0"
    s = re.sub(r'\.0$', '', s)

    # 3) Conservar solo dígitos
    digits = re.sub(r'\D+', '', s)

    return digits if digits else None


def process_category(category: str, engine, today: Optional[date]=None,
                     productos_no_match: Optional[pd.DataFrame]=None,
                     locales_no_match: Optional[pd.DataFrame]=None):
    """
    Procesa una categoría y ACUMULA los productos/locales no matcheados
    en productos_no_match / locales_no_match (ambos con estructura fija de DIM_*_COLS).
    Devuelve (df_to_insert, productos_no_match, locales_no_match).
    """
    print(f"se recibe de today: {today}")
    today = today or date.today()
    start_month, end_month = month_boundaries(today)
    cfg = CATEGORY_CONFIG[category]
    table = cfg["table"]
    print(f"today es: {today}")

    # asegurar acumuladores
    if productos_no_match is None:
        productos_no_match = pd.DataFrame(columns=DIM_PRODUCTOS_COLS)
    if locales_no_match is None:
        locales_no_match = pd.DataFrame(columns=DIM_LOCALES_COLS)

    # Localizar archivo
    csv_path = find_month_csv_for_category(cfg["dir"], BASE_DOWNLOAD_DIR, today=today)
    print(f"[{category}] CSV encontrado: {csv_path}")

    # Leer
    df_fact = read_fact_csv(csv_path)
    print(f"[{category}] Filas en CSV: {len(df_fact):,}")

    #4.2 Se pasa a int las columnas necesarios para calculos posteriores
    # if category == "BALANCEADOS":
    #     df_fact["unidades vendidas"]     = df_fact["unidades vendidas"].astype(float)
    # else:
    #     df_fact["unidades vendidas"]     = df_fact["unidades vendidas"].astype(int)
    df_fact["unidades vendidas"]     = df_fact["unidades vendidas"].astype(float)
    df_fact["total facturado"]       = df_fact["total facturado"].astype(float)

    # Validar contra dimensión productos
    df_dim_productos = read_dim_productos(engine)
    # df_dim_productos["ean"] = df_dim_productos["ean"].astype(int).astype(str)
    df_dim_productos["ean"]      = df_dim_productos["ean"].apply(normalize_barcode)
    matched_prod, unmatched_prod = join_validate(df_fact, df_dim_productos, "productos", how="inner")
    print(f"[{category}] Matched prod: {len(matched_prod):,} | Unmatched prod: {len(unmatched_prod):,}")

    # print(f"Las columnas del df son: {matched_prod.columns}")
    datos_ok_prod_df = matched_prod.drop(columns=["_merge", "ean"])
    # Validar contra dimensión sucursales
    df_dim_sucursales = read_dim_sucursales(engine)
    matched_suc, unmatched_suc = join_validate(datos_ok_prod_df, df_dim_sucursales, "sucursales", how="inner")
    print(f"[{category}] Matched suc: {len(matched_suc):,} | Unmatched suc: {len(unmatched_suc):,}")

    # DataFrame FINAL que se cargará —incluye todo—
    datos_ok_df = matched_suc.drop(columns=["_merge", "id_sucursal_bg"])

    print("Contenido de prod. no match:")
    unmatched_prod.head(3)
    print("Contenido de suc. no match:")
    unmatched_suc.head(3)

    # REMOVIDO: escritura por categoría de no matcheados
    # --------- EN SU LUGAR: acumular y deduplicar globalmente ---------
    prod_excel_df = build_unmatched_prod(unmatched_prod, category)
    loc_excel_df  = build_unmatched_loc(unmatched_suc)

    productos_no_match = pd.concat([productos_no_match, prod_excel_df], ignore_index=True)
    locales_no_match  = pd.concat([locales_no_match,  loc_excel_df],  ignore_index=True)

    mensaje = ""
    tipo_correo="notificacion"
    # send_email(mensaje, tipo_correo, category, unmatched_prod, unmatched_suc)
    # send_email(mensaje, tipo_correo, category, unmatched_prod, None)
    if len(productos_no_match) or len(locales_no_match):
        send_email(
            subject="[SCANN MARKET] Nuevos registros no mapeados",
            body=(f"Productos sin mapear: {len(productos_no_match)}\n"
                f"Locales sin mapear: {len(locales_no_match)}\n"
                "Revisar los Excel en la carpeta no_mapeados / Drive.")
        )

    datos_ok_df["fecha_dt"] = pd.to_datetime(
        datos_ok_df["fecha"].astype(str).str.strip(),
        dayfirst=True,          # dd/mm/yyyy
        format="%d/%m/%Y",      # fuerza formato y acelera el parseo
        errors="raise",         # si algo no coincide, lanza excepción
    )

    # Agregar columnas faltantes y castear tipos
    datos_ok_df["index"] = range(1, len(datos_ok_df) + 1)
    datos_ok_df["precio_prom_gs"] = pd.NA
    
    datos_ok_df["fecha_dt"] = pd.to_datetime(datos_ok_df["fecha_dt"])
    datos_ok_df["id_sucursal"] = datos_ok_df["id_sucursal"].astype(int).astype(str)
    datos_ok_df["codigo de barras"] = datos_ok_df["codigo de barras"].astype(str)

    # Agrega columnas faltantes
    #CASO ESPECIAL columna kilos
    if category == "PANALES":
        # datos_ok_df["fardos"] = datos_ok_df["gramaje"] * datos_ok_df["unidades vendidas"] * (30/1000000/0.00579)
        datos_ok_df["fardos"] = datos_ok_df["gramaje"] * datos_ok_df["unidades vendidas"] * (30/1000000/0.0058)
        datos_ok_df["unidades"] = datos_ok_df["gramaje"]*datos_ok_df["unidades vendidas"]
    elif category == "CIGARRILLOS":
        datos_ok_df["presentacion"] = datos_ok_df["presentacion"].astype(float)
        datos_ok_df["cajas"] = (datos_ok_df["presentacion"] * datos_ok_df["unidades vendidas"])/10000
    else:
        datos_ok_df["kilos"] = datos_ok_df["gramaje"]/1000 * datos_ok_df["unidades vendidas"]
    # 6.2 Total facturado en dolares
    
    datos_ok_df["total facturado dolares"] = datos_ok_df["total facturado"] / 7900
    datos_ok_df["total facturado dolares"] = datos_ok_df["total facturado dolares"].round(decimals=3)

    # Elimina la 'fecha' original del CSV para evitar duplicados
    if "fecha" in datos_ok_df.columns:
        datos_ok_df = datos_ok_df.drop(columns=["fecha"])

    if category != "PANALES":
        datos_ok_df = datos_ok_df.rename(columns={
            "fecha_dt": "fecha",
            "id_sucursal": "id_sucursal_bg",
            "unidades vendidas": "unidades",
            "total facturado": "facturacion_gs",
            "total facturado dolares": "facturacion_dolar",
            "codigo de barras": "sku",
        })
    else:
        datos_ok_df = datos_ok_df.rename(columns={
            "unidades vendidas": "paquetes",
            "fecha_dt": "fecha",
            "id_sucursal": "id_sucursal_bg",
            "total facturado": "facturacion_gs",
            "total facturado dolares": "facturacion_dolar",
            "codigo de barras": "sku",
        })
    if category == "BALANCEADOS":
        datos_ok_df = datos_ok_df.rename(columns={
            "familia": "tipo_animal",
        })

    print(f"Cant de datos antes de insertar: {len(datos_ok_df)}")

    # DataFrame FINAL que se cargará —incluye TODO el CSV—
    # datos_ok_df = df_fact.copy()

    # Elegir qué insertar
    # df_to_insert = matched if INSERT_ONLY_MATCHED else df_fact
    if category == "YERBA":
        df_to_insert = datos_ok_df.loc[:, [
            "index", "fecha", "id_sucursal_bg", "unidades",
            "kilos", "facturacion_gs", "facturacion_dolar", "precio_prom_gs", "sku"
        ]].where(lambda df: pd.notna(df), None)
    elif category == "PANALES":
        df_to_insert = datos_ok_df.loc[:, [
            "index", "fecha", "id_sucursal_bg", "unidades", "paquetes",
            "fardos", "facturacion_gs", "facturacion_dolar", "precio_prom_gs", "sku"
        ]].where(lambda df: pd.notna(df), None)
    elif category == "BALANCEADOS":
        df_to_insert = datos_ok_df.loc[:, [
            "index", "fecha", "id_sucursal_bg", "unidades",
            "kilos", "facturacion_gs", "facturacion_dolar", "precio_prom_gs", "sku", "tipo_animal"
        ]].where(lambda df: pd.notna(df), None)
    elif category == "CIGARRILLOS":
        df_to_insert = datos_ok_df.loc[:, [
            "index", "fecha", "id_sucursal_bg", "unidades",
            "cajas", "facturacion_gs", "facturacion_dolar", "precio_prom_gs", "sku"
        ]].where(lambda df: pd.notna(df), None)
    elif category == "VAPES":
        df_to_insert = datos_ok_df.loc[:, [
            "index", "fecha", "id_sucursal_bg", "unidades",
            "facturacion_gs", "facturacion_dolar", "precio_prom_gs", "sku"
        ]].where(lambda df: pd.notna(df), None)
    # df_to_insert = datos_ok_df

    print(f"\n2)[{category}] Las columnas son:\n")
    df_to_insert.info()

    # Borrar mes y cargar
    if DRY_RUN:
        print(f"[{category}] DRY_RUN=TRUE → no se ejecuta DELETE/INSERT en {table}")
    else:
        delete_month_from_table(engine, table, start_month, end_month, FACT_DATE_COLUMN)
        insert_dataframe(engine, table, df_to_insert)
        print(f"[{category}] Insertadas {len(df_to_insert):,} filas en {table}")

    # Devolver DF a insertar + acumuladores actualizados
    return df_to_insert, productos_no_match, locales_no_match

def main():
    # Construir engine y probar conexión
    engine = build_engine()
    with engine.connect() as conn:
        test = conn.execute(text("SELECT 1")).scalar()
    print(f"Conexión OK. SELECT 1 -> {test}")

    GLOBAL_UNMATCHED_DIR = pathlib.Path(BASE_DOWNLOAD_DIR) / "no_mapeados"
    GLOBAL_UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

    # NUEVO: acumuladores globales (vacíos con estructura correcta)
    unmatched_prod_global = pd.DataFrame(columns=DIM_PRODUCTOS_COLS)
    unmatched_loc_global  = pd.DataFrame(columns=DIM_LOCALES_COLS)

    # Ejecutar para todas las categorías
    fecha_actual = date.today()
    # today = datetime.date.today()
    if fecha_actual.day == 1:
            target_date = fecha_actual.replace(day=1) - timedelta(days=1)
    else:
        target_date = fecha_actual

    for cat in CATEGORY_CONFIG:
        try:
            # process_category(cat, engine, today=fecha_actual)
            _, unmatched_prod_global, unmatched_loc_global = process_category(
                cat, engine, today=target_date,
                productos_no_match=unmatched_prod_global,
                locales_no_match=unmatched_loc_global
            )
        except Exception as e:
            print(f"[{cat}] ❌ Error: {e}")
            mensaje = ""
            tipo_correo="error"
            # send_email(mensaje=mensaje, tipo=tipo_correo, category=cat)
            send_email(
                subject=f"[ERROR] {cat} – ETL",
                body=f"Se ha generado el siguiente error:\n{e}\n\nOrigen: escritura_biggie.py"
            )

    # Deduplicación final por seguridad
    if len(unmatched_prod_global):
        unmatched_prod_global["ean"] = unmatched_prod_global["ean"].astype(str)
        unmatched_prod_global = unmatched_prod_global.drop_duplicates(subset="ean")
    if len(unmatched_loc_global):
        unmatched_loc_global["id_sucursal_bg"] = unmatched_loc_global["id_sucursal_bg"].astype(str)
        unmatched_loc_global = unmatched_loc_global.drop_duplicates(subset="id_sucursal_bg")

    mensaje = ""
    tipo_correo="notificacion"
    # send_email(mensaje, tipo_correo, None, None, unmatched_loc_global)
    send_email(
        subject="[BIGGIE] Nuevos registros no mapeados",
        body=(f"Productos sin mapear: {len(unmatched_prod_global)}\n"
            f"Locales sin mapear: {len(unmatched_loc_global)}\n"
            "Revisar los Excel en no_mapeados / Drive.")
    )

    # Escritura final (un Excel por tipo) en carpeta GLOBAL_UNMATCHED_DIR
    out_prod_xlsx = GLOBAL_UNMATCHED_DIR / f"PRODUCTOS_no_mapeados.xlsx"
    out_loc_xlsx  = GLOBAL_UNMATCHED_DIR / f"LOCALES_no_mapeados.xlsx"

    with pd.ExcelWriter(out_prod_xlsx, engine="xlsxwriter") as xlw:
        unmatched_prod_global.to_excel(xlw, index=False, sheet_name="unmatched")

    with pd.ExcelWriter(out_loc_xlsx, engine="xlsxwriter") as xlw:
        unmatched_loc_global.to_excel(xlw, index=False, sheet_name="unmatched")

    print(f"✅ No matcheados de PRODUCTOS: {len(unmatched_prod_global)} filas → {out_prod_xlsx}")
    print(f"✅ No matcheados de LOCALES:   {len(unmatched_loc_global)} filas → {out_loc_xlsx}")

    prod_filename = f"PRODUCTOS_no_mapeados.xlsx"
    loc_filename  = f"LOCALES_no_mapeados.xlsx"

    # Serializar a bytes en memoria
    prod_buf = io.BytesIO()
    with pd.ExcelWriter(prod_buf, engine="xlsxwriter") as xlw:
        unmatched_prod_global.to_excel(xlw, index=False, sheet_name="unmatched")
    prod_buf.seek(0)

    loc_buf = io.BytesIO()
    with pd.ExcelWriter(loc_buf, engine="xlsxwriter") as xlw:
        unmatched_loc_global.to_excel(xlw, index=False, sheet_name="unmatched")
    loc_buf.seek(0)

    # Variante A: desde archivo en disco
    service = get_drive_service_sa(service_account_json_path="service_account.json", scopes=DRIVE_SCOPES)


    prod_id, prod_action = drive_upload_or_update_xlsx(service, FOLDER_ID_NO_MAPEADOS, prod_filename, prod_buf.getvalue())
    loc_id,  loc_action  = drive_upload_or_update_xlsx(service, FOLDER_ID_NO_MAPEADOS,  loc_filename,  loc_buf.getvalue())

    print(f"✅ PRODUCTOS: {prod_action} (fileId={prod_id}) en carpeta {FOLDER_ID_NO_MAPEADOS}")
    print(f"✅ LOCALES:   {loc_action} (fileId={loc_id}) en carpeta {FOLDER_ID_NO_MAPEADOS}")

if __name__ == "__main__":
    main()