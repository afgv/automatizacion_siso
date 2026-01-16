# -*- coding: utf-8 -*-
import os
import io
import json
from typing import List, Tuple

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account

load_dotenv(dotenv_path=".env")
# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
# ⬅⬅⬅ PONÉ AQUÍ EL ID DE LA CARPETA (en Unidad compartida) "Registros no mapeados"
FOLDER_ID_SHARED = os.getenv("FOLDER_ID_SHARED", "")

# Ruta del JSON de la Service Account (o usá variable de entorno para pasarlo inline)
SERVICE_ACCOUNT_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "service_account.json")

# Tabla destino
SQL_TARGET_SCHEMA = "dbo"
SQL_TARGET_TABLE_PRODUCTS  = "dim_biggie_productos"  # inserta solo productos
SQL_TARGET_TABLE_LOCALS  = "dim_biggie_locales"  # inserta solo locales

# Nombre de hoja en los .xlsx
SHEET_NAME = "unmatched"

# Prefijos de archivo esperados
PRODUCT_PREFIX = "PRODUCTOS_no_mapeados"
LOCALES_PREFIX = "LOCALES_no_mapeados"

# Columnas esperadas para productos (estructura de la dimensión)
DIM_PRODUCTOS_COLS = [
    "producto", "proveedor", "categoria", "marca",
    "clasificacion", "presentacion", "variedad", "talle", "segmento",
    "promocion", "ean", "gramaje", "sub_segmento", "sub_marca",
    "sabor", "especialidad", "sub_categoria"
]

# Columnas esperadas para locales (no insertamos a DB en este script; solo limpiamos el archivo)
DIM_LOCALES_COLS = [
    "id_sucursal_bg", "nombre_local", "latitud", "longitud", "ciudad",
    "cod_cliente_palermo", "autocompra", "compra_balanceado",
    "AC_yerba_exhibidores", "ac_pod_cigarreras"
]

# ---------------------------------------------------------------------
# DB ENGINE (usa tu builder actual; si ya lo tenés en otro módulo, importalo)
# ---------------------------------------------------------------------
import urllib

SQL_SERVER   = os.getenv("SERVER", "")
SQL_DATABASE = os.getenv("DATABASE", "")
SQL_DRIVER   = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
ENCRYPT      = os.getenv("ODBC_ENCRYPT", "yes")             # yes|no
TRUST_CERT   = os.getenv("ODBC_TRUST_SERVER_CERT", "yes")   # yes|no
TIMEOUT      = os.getenv("ODBC_TIMEOUT")                    # ej. "30"

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

# ---------------------------------------------------------------------
# DRIVE HELPERS (Service Account - Unidad compartida)
# ---------------------------------------------------------------------
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_drive_service_sa(service_account_json_path: str) -> any:
    creds = service_account.Credentials.from_service_account_file(
        service_account_json_path,
        scopes=DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=creds)

def list_files_in_folder(service, folder_id: str, page_size: int = 1000) -> List[dict]:
    """
    Lista todos los archivos (no borrados) dentro de la carpeta dada.
    """
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
            pageToken=page_token
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

# ===== DEBUG =====
VERBOSE = True

def dbg(msg="", *args):
    if VERBOSE:
        print(msg.format(*args))

def dump_df(df: pd.DataFrame, name: str, max_rows: int = 5):
    dbg("\n--- [{}] shape={} ---", name, df.shape)
    dbg("[{}] columnas: {}", name, list(df.columns))
    try:
        # info() imprime a stdout; lo forzamos
        import sys
        buf = io.StringIO()
        df.info(buf=buf)
        dbg("[{}] info:\n{}", name, buf.getvalue())
    except Exception:
        print("Hubo una excepcion en el dump")
    if "para_escritura" in df.columns:
        # muestrá los valores brutos y ya normalizados si existe
        vc = df["para_escritura"].value_counts(dropna=False)
        dbg("[{}] para_escritura value_counts:\n{}", name, vc)
        # también versiones 'raw' si existiera una columna original
    dbg("[{}] head({}):\n{}", name, max_rows, df.head(max_rows))
# ===== /DEBUG =====

def download_file_bytes(service, file_id: str) -> bytes:
    buf = io.BytesIO()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        # opcional: print(f"Descargando {int(status.progress()*100)}%")
    return buf.getvalue()

def update_file_bytes(service, file_id: str, data: bytes, mimetype: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    updated = service.files().update(
        fileId=file_id,
        media_body=media,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return updated["id"]

# ---------------------------------------------------------------------
# UTILIDADES DATAFRAME
# ---------------------------------------------------------------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    return df

def ensure_para_escritura(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    if "para_escritura" not in df.columns:
        # busca alias tipo "para escritura", "para_escritura " (con espacios), etc.
        candidates = [c for c in df.columns if "para" in c and "escritura" in c]
        if candidates:
            df = df.rename(columns={candidates[0]: "para_escritura"})
        else:
            df["para_escritura"] = False

    def _to_bool(x):
        if isinstance(x, bool):
            return x
        if pd.isna(x):
            return False
        if isinstance(x, (int, float)):
            return bool(int(x))
        s = str(x).strip().lower()
        return s in {"true", "t", "1", "yes", "y", "si", "sí", "x", "verdadero"}
    df["para_escritura"] = df["para_escritura"].map(_to_bool).astype(bool)
    return df

def normalize_para_escritura_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura la columna booleana 'para_escritura' aceptando variantes:
    TRUE/FALSE, 1/0, yes/no, si/sí, 'x', y además remueve espacios invisibles.
    """
    if "para_escritura" not in df.columns:
        df["para_escritura"] = False

    def _to_bool(x):
        if isinstance(x, bool):
            return x
        if pd.isna(x):
            return False
        if isinstance(x, (int, float)):
            try:
                return bool(int(x))
            except Exception:
                return False
        s = str(x)

        # remover espacios invisibles comunes en Excel/Drive
        s = (s.replace("\u00A0", "")   # NBSP
               .replace("\u202F", "")  # NNBSP
               .replace("\u2007", "")) # FIGURE SPACE

        # normalizar
        s = "".join(ch for ch in s if not ch.isspace())
        s = s.strip().casefold()

        true_set  = {"true", "1", "yes", "y", "si", "sí", "x", "t", "verdadero"}
        false_set = {"false", "0", "no", "n", "f", "falso"}

        if s in true_set:
            return True
        if s in false_set:
            return False
        # fallback: contiene la palabra 'true' o 'verdadero'
        return ("true" in s) or ("verdadero" in s)

    df["para_escritura"] = df["para_escritura"].map(_to_bool).astype(bool)
    return df


def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Garantiza columnas y orden; agrega faltantes como None.
    """
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols + [c for c in out.columns if c not in cols]]  # mantiene extras (p.ej. para_escritura) al final

def to_varchar_safe(df: pd.DataFrame, cols_as_str: List[str]) -> pd.DataFrame:
    """
    Fuerza cols a string (sin .0, ni NaN) para evitar sorpresas de tipos.
    """
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
    Inserta filas con para_escritura=TRUE en dbo.dim_biggie_productos
    y reescribe el archivo dejando sólo para_escritura=FALSE.
    Devuelve (#insertados, #restantes_en_archivo).
    """
    raw = download_file_bytes(service, file_id)
    df = pd.read_excel(io.BytesIO(raw), sheet_name=SHEET_NAME, engine="openpyxl")

    dbg("\n===== PRODUCTOS: {} =====", file_name)
    dump_df(df, "df_original", max_rows=8)

    df = normalize_para_escritura_col(df)

    dump_df(df, "df_normalizado", max_rows=8)

    # Separar TRUE / FALSE
    df_true  = df[df["para_escritura"] == True].copy()
    df_false = df[df["para_escritura"] == False].copy()
    dbg("[split] TRUE={} | FALSE={}", len(df_true), len(df_false))

    inserted = 0
    if not df_true.empty:
        # Asegurar estructura de la dimensión y tipos amigables
        df_to_insert = ensure_cols(df_true, DIM_PRODUCTOS_COLS)
        # tipificar columnas típicamente textuales (incluye ean)
        df_to_insert = to_varchar_safe(df_to_insert, [
            "producto", "proveedor", "categoria", "marca",
            "clasificacion", "presentacion", "variedad", "talle", "segmento",
            "promocion", "ean", "gramaje", "sub_segmento", "sub_marca",
            "sabor", "especialidad", "sub_categoria"
        ])

        # Insertar a SQL
        with engine.begin() as conn:
            df_to_insert[DIM_PRODUCTOS_COLS].to_sql(
                SQL_TARGET_TABLE_PRODUCTS,
                con=conn,
                schema=SQL_TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000
            )
        inserted = len(df_to_insert)

    # Reescribir el archivo con sólo FALSE
    # (conservamos todas las columnas originales para no romper tu flujo)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xlw:
        df_false.to_excel(xlw, index=False, sheet_name=SHEET_NAME)
    buf.seek(0)
    update_file_bytes(service, file_id, buf.getvalue(),
                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    print(f"PRODUCTOS → {file_name}: insertados {inserted}, quedan {len(df_false)} filas")
    return inserted, len(df_false)

def process_locales_file(engine, service, file_id: str, file_name: str) -> int:
    """
    Inserta filas con para_escritura=TRUE en dbo.dim_biggie_locales
    y reescribe el archivo dejando sólo para_escritura=FALSE.
    Devuelve #restantes_en_archivo.
    """
    # 1) Leer forzando la columna como texto para que tu normalizador la detecte bien
    raw = download_file_bytes(service, file_id)
    df = pd.read_excel(
        io.BytesIO(raw),
        sheet_name=SHEET_NAME,
        engine="openpyxl",
        dtype={"para_escritura": object}  # <- clave
    )

    dbg("\n===== LOCALES: {} =====", file_name)
    dump_df(df, "df_locales_original", max_rows=8)

    vals = df["para_escritura"].astype(str).unique().tolist()
    print("Valores crudos únicos en para_escritura:", [repr(v) for v in vals])

    # 2) Normalizar booleana como en productos
    df = normalize_para_escritura_col(df)
    dump_df(df, "df_locales_normalizado", max_rows=8)

    # 3) Split TRUE / FALSE
    df_true  = df[df["para_escritura"] == True].copy()
    df_false = df[df["para_escritura"] == False].copy()
    dbg("[LOCALES split] TRUE={} | FALSE={}", len(df_true), len(df_false))

    # 4) INSERT a SQL (solo si hay TRUE)
    inserted = 0
    if not df_true.empty:
        # columnas exactas de la dimensión
        df_to_insert = ensure_cols(df_true, DIM_LOCALES_COLS)

        # sanear campos propensos a quedar '1037911.0' o NaN en texto
        df_to_insert = to_varchar_safe(df_to_insert, [
            "id_sucursal_bg", "nombre_local", "ciudad", "cod_cliente_palermo"
        ])

        with engine.begin() as conn:
            df_to_insert[DIM_LOCALES_COLS].to_sql(
                SQL_TARGET_TABLE_LOCALS,
                con=conn,
                schema=SQL_TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000
            )
        inserted = len(df_to_insert)
        dbg("[LOCALES insert] OK → {} filas", inserted)
    else:
        dbg("[LOCALES insert] No hay filas con para_escritura=TRUE → no se inserta nada.")

    # 5) Reescribir archivo con solo FALSE (igual que productos)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xlw:
        df_false.to_excel(xlw, index=False, sheet_name=SHEET_NAME)
    buf.seek(0)
    update_file_bytes(
        service, file_id, buf.getvalue(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    dbg("[LOCALES update] Archivo {} actualizado con {} filas (solo FALSE)", file_name, len(df_false))

    print(f"LOCALES → {file_name}: insertados {inserted}, quedan {len(df_false)} filas")
    return inserted, len(df_false)

def show_file_meta(service, file_id):
    meta = service.files().get(
        fileId=file_id,
        fields="id,name,modifiedTime,owners(emailAddress,displayName),driveId",
        supportsAllDrives=True
    ).execute()
    print("META:", meta)


def main():
    # Validaciones básicas
    if not FOLDER_ID_SHARED or FOLDER_ID_SHARED.startswith("<REEMPLAZAR"):
        raise SystemExit("Configura FOLDER_ID_SHARED con el ID de la carpeta en la Unidad compartida.")

    if not os.path.exists(SERVICE_ACCOUNT_JSON_PATH):
        raise SystemExit(f"No se encontró SERVICE_ACCOUNT_JSON_PATH={SERVICE_ACCOUNT_JSON_PATH}")

    # Servicios
    service = get_drive_service_sa(SERVICE_ACCOUNT_JSON_PATH)
    engine = build_engine()

    # Listar archivos en carpeta
    files = list_files_in_folder(service, FOLDER_ID_SHARED)
    print("Archivos en carpeta:", [(f["name"], f["mimeType"]) for f in files])
    if not files:
        print("No se encontraron archivos en la carpeta.")
        return

    # Filtrar por prefijo (procesa todos los meses que haya)
    product_files = [f for f in files
                    if f["name"].startswith(PRODUCT_PREFIX) and f["mimeType"] in (XLSX_MIME, GSHEET_MIME)]
    locales_files = [f for f in files
                    if f["name"].startswith(LOCALES_PREFIX) and f["mimeType"] in (XLSX_MIME, GSHEET_MIME)]

    print(f"Encontrados PRODUCTOS: {len(product_files)} | LOCALES: {len(locales_files)}")
    print("PRODUCTOS:", [f["name"] for f in product_files])
    print("LOCALES:", [f["name"] for f in locales_files])
    # Procesar PRODUCTOS
    total_inserted_productos = 0
    for f in sorted(product_files, key=lambda x: x["name"]):
        try:
            show_file_meta(service, f["id"])
            ins, _remain = process_product_file(engine, service, f["id"], f["name"])
            total_inserted_productos += ins
        except Exception as e:
            print(f"❌ Error procesando PRODUCTOS {f['name']}: {e}")

    total_inserted_locales = 0
    # Procesar LOCALES (solo limpiar)
    for f in sorted(locales_files, key=lambda x: x["name"]):
        try:
            show_file_meta(service, f["id"])
            ins, _remain = process_locales_file(engine, service, f["id"], f["name"])
            total_inserted_locales += ins
        except Exception as e:
            print(f"❌ Error procesando LOCALES {f['name']}: {e}")

    print(f"-Productos insertados: {total_inserted_productos}")
    print(f"-Locales insertados: {total_inserted_locales}")

if __name__ == "__main__":
    main()
