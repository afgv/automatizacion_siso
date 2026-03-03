# -*- coding: utf-8 -*-
"""
SCANN MARKET – Escritura de registros no mapeados
--------------------------------------------------
Lee los archivos Excel de productos y locales no mapeados desde la carpeta
de Google Drive de Scannmarket, inserta en SQL Server las filas con
para_escritura=TRUE y reescribe el archivo dejando solo las filas con
para_escritura=FALSE.

Archivos esperados en Drive (hoja "unmatched"):
  SCANN_PRODUCTOS_no_mapeados.xlsx  → tabla: siso.dim_scannmarket_producto
  SCANN_LOCALES_no_mapeados.xlsx    → tabla: siso.dim_scannmarket_pdv
"""
import os
import io
import urllib
from typing import List, Tuple

from dotenv import load_dotenv, find_dotenv
import pandas as pd
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account

load_dotenv(find_dotenv(), override=True)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

# ID de la carpeta de Drive donde se leen los archivos de no mapeados
FOLDER_ID_SHARED = os.getenv("FOLDER_ID_SCANN_NO_MAPEADOS", "1tYfITo5Up-Rdp87GNKczWNttrFUtiAjC")

# Ruta del JSON de la Service Account
SERVICE_ACCOUNT_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "service_account.json")

# Tablas destino
SQL_TARGET_SCHEMA         = "siso"
SQL_TARGET_TABLE_PRODUCTS = "dim_scannmarket_producto"
SQL_TARGET_TABLE_LOCALES  = "dim_scannmarket_pdv"

# Nombre de hoja en los .xlsx
SHEET_NAME = "unmatched"

# Prefijos de archivo esperados
PRODUCT_PREFIX = "SCANN_PRODUCTOS_no_mapeados"
LOCALES_PREFIX = "SCANN_LOCALES_no_mapeados"

# Columnas de la dimensión de productos (sin para_escritura)
DIM_PRODUCTOS_COLS = [
    "Id_Producto",
    "Descripcion_del_Producto",
    "Marca",
    "Categoria",
    "Segmento",
    "Sub_Segmento",
    "Variedades",
    "Presentacion_Producto",
    "Proveedor",
    "Palermo_Otros",
    "especialidad",
    "talle",
    "unidades",
    "Gramos",
    "es_promo",
    "Empaque",
    "clasificacion",
]

# Columnas de la dimensión de locales / PDV (sin para_escritura)
DIM_LOCALES_COLS = [
    "Código único PDV",
    "Pdv Clasif 3",
    "Pdv RUC",
    "PDV_RAZON_SOCIAL",
    "Nombre PDV",
    "Estado PDV",
    "Localidad PDV",
    "Dirección PDV",
]

# ---------------------------------------------------------------------
# DB ENGINE
# ---------------------------------------------------------------------

SQL_SERVER   = os.getenv("SERVER", "")
SQL_DATABASE = os.getenv("DATABASE", "")
SQL_DRIVER   = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
ENCRYPT      = os.getenv("ODBC_ENCRYPT", "yes")
TRUST_CERT   = os.getenv("ODBC_TRUST_SERVER_CERT", "yes")
TIMEOUT      = os.getenv("ODBC_TIMEOUT")

SQL_UID = os.getenv("SQL_USERNAME")
SQL_PWD = os.getenv("SQL_PASSWORD")

XLSX_MIME   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"


def build_engine():
    parts = [
        f"DRIVER={{{SQL_DRIVER}}}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}",
        f"Encrypt={ENCRYPT}",
        f"TrustServerCertificate={TRUST_CERT}",
    ]
    if SQL_UID and SQL_PWD:
        parts += [f"UID={SQL_UID}", f"PWD={SQL_PWD}"]
    else:
        parts.append("Trusted_Connection=yes")
    if TIMEOUT:
        parts.append(f"Connection Timeout={TIMEOUT}")
    odbc_str = ";".join(parts) + ";"
    params = urllib.parse.quote_plus(odbc_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
    )


# ---------------------------------------------------------------------
# DRIVE HELPERS (Service Account)
# ---------------------------------------------------------------------

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_drive_service_sa(service_account_json_path: str):
    creds = service_account.Credentials.from_service_account_file(
        service_account_json_path,
        scopes=DRIVE_SCOPES,
    )
    return build("drive", "v3", credentials=creds)


def list_files_in_folder(service, folder_id: str, page_size: int = 1000) -> List[dict]:
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces="drive",
            fields="nextPageToken, files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=page_size,
            pageToken=page_token,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def download_file_bytes(service, file_id: str) -> bytes:
    buf = io.BytesIO()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def update_file_bytes(service, file_id: str, data: bytes, mimetype: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    updated = service.files().update(
        fileId=file_id,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return updated["id"]


def show_file_meta(service, file_id: str):
    meta = service.files().get(
        fileId=file_id,
        fields="id,name,modifiedTime,owners(emailAddress,displayName),driveId",
        supportsAllDrives=True,
    ).execute()
    print("META:", meta)


# ---------------------------------------------------------------------
# DEBUG
# ---------------------------------------------------------------------

VERBOSE = True


def dbg(msg="", *args):
    if VERBOSE:
        print(msg.format(*args))


def dump_df(df: pd.DataFrame, name: str, max_rows: int = 5):
    dbg("\n--- [{}] shape={} ---", name, df.shape)
    dbg("[{}] columnas: {}", name, list(df.columns))
    if "para_escritura" in df.columns:
        vc = df["para_escritura"].value_counts(dropna=False)
        dbg("[{}] para_escritura value_counts:\n{}", name, vc)
    dbg("[{}] head({}):\n{}", name, max_rows, df.head(max_rows))


# ---------------------------------------------------------------------
# UTILIDADES DATAFRAME
# ---------------------------------------------------------------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas: strip + lower + espacios → _."""
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


def _to_bool(x) -> bool:
    if isinstance(x, bool):
        return x
    if pd.isna(x):
        return False
    if isinstance(x, (int, float)):
        return bool(x)
    s = str(x).strip().lower()
    return s in ("true", "1", "si", "sí", "yes", "verdadero")


def normalize_para_escritura_col(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza (o crea) la columna para_escritura como booleana."""
    df = df.copy()
    # Buscar la columna independientemente del casing/espacios
    col_map = {c.lower().replace(" ", "_"): c for c in df.columns}
    raw_col = col_map.get("para_escritura")
    if raw_col is None:
        candidates = [c for c in df.columns if "para" in c.lower() and "escritura" in c.lower()]
        raw_col = candidates[0] if candidates else None

    if raw_col and raw_col != "para_escritura":
        df = df.rename(columns={raw_col: "para_escritura"})
    if "para_escritura" not in df.columns:
        df["para_escritura"] = False

    df["para_escritura"] = df["para_escritura"].apply(_to_bool)
    return df


def ensure_cols(df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    """Añade columnas faltantes como None y reordena."""
    df = df.copy()
    for c in required_cols:
        if c not in df.columns:
            df[c] = None
    return df[required_cols]


def to_varchar_safe(df: pd.DataFrame, cols_as_str: List[str]) -> pd.DataFrame:
    """Fuerza columnas a string limpio (sin '.0', ni NaN)."""
    out = df.copy()
    for c in cols_as_str:
        if c in out.columns:
            out[c] = out[c].astype(object).where(out[c].notna(), None)
            out[c] = out[c].apply(lambda v: None if v is None else str(v).strip())
    return out


# ---------------------------------------------------------------------
# LÓGICA DE PROCESO
# ---------------------------------------------------------------------

def process_product_file(engine, service, file_id: str, file_name: str) -> Tuple[int, int]:
    """
    Inserta filas con para_escritura=TRUE en siso.dim_scannmarket_producto
    y reescribe el archivo dejando sólo las filas con para_escritura=FALSE.
    Devuelve (#insertados, #restantes_en_archivo).
    """
    raw = download_file_bytes(service, file_id)
    df = pd.read_excel(io.BytesIO(raw), sheet_name=SHEET_NAME, engine="openpyxl")

    dbg("\n===== PRODUCTOS SCANN: {} =====", file_name)
    dump_df(df, "df_original")

    df = normalize_para_escritura_col(df)
    dump_df(df, "df_normalizado")

    df_true  = df[df["para_escritura"] == True].copy()
    df_false = df[df["para_escritura"] == False].copy()
    dbg("[split] TRUE={} | FALSE={}", len(df_true), len(df_false))

    inserted = 0
    if not df_true.empty:
        df_to_insert = ensure_cols(df_true, DIM_PRODUCTOS_COLS)
        df_to_insert = to_varchar_safe(df_to_insert, [
            "Id_Producto", "Descripcion_del_Producto", "Marca", "Categoria",
            "Segmento", "Sub_Segmento", "Variedades", "Presentacion_Producto",
            "Proveedor", "Palermo_Otros", "especialidad", "talle",
            "Empaque", "clasificacion", "es_promo",
        ])
        with engine.begin() as conn:
            df_to_insert[DIM_PRODUCTOS_COLS].to_sql(
                SQL_TARGET_TABLE_PRODUCTS,
                con=conn,
                schema=SQL_TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
        inserted = len(df_to_insert)
        dbg("[PRODUCTOS insert] OK → {} filas", inserted)
    else:
        dbg("[PRODUCTOS insert] No hay filas con para_escritura=TRUE → no se inserta nada.")

    # Reescribir el archivo dejando solo FALSE
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xlw:
        df_false.to_excel(xlw, index=False, sheet_name=SHEET_NAME)
    buf.seek(0)
    update_file_bytes(service, file_id, buf.getvalue(), XLSX_MIME)

    print(f"PRODUCTOS SCANN → {file_name}: insertados {inserted}, quedan {len(df_false)} filas")
    return inserted, len(df_false)


def process_locales_file(engine, service, file_id: str, file_name: str) -> Tuple[int, int]:
    """
    Inserta filas con para_escritura=TRUE en siso.dim_scannmarket_pdv
    y reescribe el archivo dejando sólo las filas con para_escritura=FALSE.
    Devuelve (#insertados, #restantes_en_archivo).
    """
    raw = download_file_bytes(service, file_id)
    df = pd.read_excel(
        io.BytesIO(raw),
        sheet_name=SHEET_NAME,
        engine="openpyxl",
        dtype={"para_escritura": object},
    )

    dbg("\n===== LOCALES SCANN: {} =====", file_name)
    dump_df(df, "df_locales_original")

    df = normalize_para_escritura_col(df)

    if "para_escritura" in df.columns:
        vals = df["para_escritura"].astype(str).unique().tolist()
        print("Valores crudos únicos en para_escritura:", [repr(v) for v in vals])
    dump_df(df, "df_locales_normalizado")

    df_true  = df[df["para_escritura"] == True].copy()
    df_false = df[df["para_escritura"] == False].copy()
    dbg("[LOCALES split] TRUE={} | FALSE={}", len(df_true), len(df_false))

    inserted = 0
    if not df_true.empty:
        df_to_insert = ensure_cols(df_true, DIM_LOCALES_COLS)
        df_to_insert = to_varchar_safe(df_to_insert, [
            "Código único PDV", "Pdv Clasif 3", "Pdv RUC",
            "PDV_RAZON_SOCIAL", "Nombre PDV", "Estado PDV",
            "Localidad PDV", "Dirección PDV",
        ])
        with engine.begin() as conn:
            df_to_insert[DIM_LOCALES_COLS].to_sql(
                SQL_TARGET_TABLE_LOCALES,
                con=conn,
                schema=SQL_TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
        inserted = len(df_to_insert)
        dbg("[LOCALES insert] OK → {} filas", inserted)
    else:
        dbg("[LOCALES insert] No hay filas con para_escritura=TRUE → no se inserta nada.")

    # Reescribir el archivo dejando solo FALSE
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xlw:
        df_false.to_excel(xlw, index=False, sheet_name=SHEET_NAME)
    buf.seek(0)
    update_file_bytes(service, file_id, buf.getvalue(), XLSX_MIME)
    dbg("[LOCALES update] Archivo {} actualizado con {} filas (solo FALSE)", file_name, len(df_false))

    print(f"LOCALES SCANN → {file_name}: insertados {inserted}, quedan {len(df_false)} filas")
    return inserted, len(df_false)


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    if not FOLDER_ID_SHARED:
        raise SystemExit("Configura FOLDER_ID_SCANN_NO_MAPEADOS con el ID de la carpeta en Drive.")

    if not os.path.exists(SERVICE_ACCOUNT_JSON_PATH):
        raise SystemExit(f"No se encontró SERVICE_ACCOUNT_JSON_PATH={SERVICE_ACCOUNT_JSON_PATH}")

    service = get_drive_service_sa(SERVICE_ACCOUNT_JSON_PATH)
    engine  = build_engine()

    files = list_files_in_folder(service, FOLDER_ID_SHARED)
    print("Archivos en carpeta:", [(f["name"], f["mimeType"]) for f in files])
    if not files:
        print("No se encontraron archivos en la carpeta.")
        return

    product_files = [
        f for f in files
        if f["name"].startswith(PRODUCT_PREFIX) and f["mimeType"] in (XLSX_MIME, GSHEET_MIME)
    ]
    locales_files = [
        f for f in files
        if f["name"].startswith(LOCALES_PREFIX) and f["mimeType"] in (XLSX_MIME, GSHEET_MIME)
    ]

    print(f"Encontrados PRODUCTOS: {len(product_files)} | LOCALES: {len(locales_files)}")
    print("PRODUCTOS:", [f["name"] for f in product_files])
    print("LOCALES:",   [f["name"] for f in locales_files])

    total_inserted_productos = 0
    for f in sorted(product_files, key=lambda x: x["name"]):
        try:
            show_file_meta(service, f["id"])
            ins, _ = process_product_file(engine, service, f["id"], f["name"])
            total_inserted_productos += ins
        except Exception as e:
            print(f"❌ Error procesando PRODUCTOS {f['name']}: {e}")

    total_inserted_locales = 0
    for f in sorted(locales_files, key=lambda x: x["name"]):
        try:
            show_file_meta(service, f["id"])
            ins, _ = process_locales_file(engine, service, f["id"], f["name"])
            total_inserted_locales += ins
        except Exception as e:
            print(f"❌ Error procesando LOCALES {f['name']}: {e}")

    print(f"- Productos SCANN insertados: {total_inserted_productos}")
    print(f"- Locales SCANN insertados:   {total_inserted_locales}")


if __name__ == "__main__":
    main()