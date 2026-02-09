#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCANN MARKET - ETL (.env + mes anterior + Excel)
-------------------------------------------------
- Detecta automáticamente el Excel del **mes ANTERIOR** dentro de BASE_DOWNLOAD_DIR
  con nombres del estilo:
      base_scann_<mes>.xlsx
      base_scann_<mes>_<yy>.xlsx
      base_scann_<mes>_<yyyy>.xlsx
  Acepta alias "septiembre"/"setiembre" y guiones/underscores.
- Normaliza nombres de columnas (sin acentos, minúsculas).
- Aplica mapeo de "nombre pdv" desde JSON fijo (pdv_mapeo_scann.json).
- Carga/actualiza DIMs y FACT en SQL Server.
- Conexión a BD **solo por .env** (sin argparse).
"""
from __future__ import annotations

import os
import re
import io
import pathlib
import json
import urllib.parse
import unicodedata
from typing import Dict, List, Optional, Tuple
from datetime import date
import logging

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from dotenv import load_dotenv, find_dotenv

from notifications import get_drive_service_sa, send_email
from upload_to_drive import drive_upload_or_update_xlsx


# carga .env (busca automáticamente hacia arriba)
load_dotenv(find_dotenv(), override=True)

# --------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------

# Carpeta raíz donde se descargan los Excels de Scann Market
PRODUCCION   = os.getenv("PRODUCCION", False)
BASE_DOWNLOAD_DIR = r"C:\Users\adrian.garcia\Documents\descargas_drive\scannmarket"
if PRODUCCION:
    BASE_DOWNLOAD_DIR = os.getenv("BASE_DOWNLOAD_DIR_SM", "/opt/airflow/data/descargas_drive/scannmarket")

GLOBAL_UNMATCHED_DIR = pathlib.Path(BASE_DOWNLOAD_DIR) / "no_mapeados_scann"
GLOBAL_UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

FOLDER_ID_NO_MAPEADOS = '1tYfITo5Up-Rdp87GNKczWNttrFUtiAjC'

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
DELEGATED_USER       = os.getenv("GOOGLE_DELEGATED_USER")
SCOPES               = ["https://www.googleapis.com/auth/gmail.send"]
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]  # acceso completo a Drive

# Hoja por defecto
DEFAULT_SHEET_NAME = "Base"

# Carpeta de salida para errores
OUTDIR = os.getenv("SCANN_ERROR_DIR", "./errores_scann")

# Mapeo de mes (texto) -> número (para construir fecha)
MONTH_MAP: Dict[str, str] = {
    "ENERO": "1", "FEBRERO": "2", "MARZO": "3", "ABRIL": "4", "MAYO": "5", "JUNIO": "6",
    "JULIO": "7", "AGOSTO": "8", "SETIEMBRE": "9", "SEPTIEMBRE": "9",
    "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12",
}

# Hotfix observado en KNIME
PDV_HOTFIX_REPLACE: Dict[str, str] = {"360845043": "359545043"}

# JSON de mapeo de PDV (debe estar junto al script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PDV_MAP_JSON = os.path.join(SCRIPT_DIR, "pdv_mapeo_scann.json")

# Columnas a limpiar (tras normalizar nombres a minúsculas sin acentos)
CLEAN_COLS: List[str] = [
    "nombre pdv", "estado pdv", "localidad pdv", "direccion pdv", "pdv clasif 3",
    "categoria sku", "prod clasif 5", "prod clasif 6", "prod clasif 7",
    "prod clasif 8", "prod clasif 9", "prod clasif 10", "escencia", "sabor",
]

# Columnas de salida requeridas (en orden) + bandera booleana
# Columnas de salida requeridas (en orden) + bandera booleana
UNMATCHED_PROD_OUTPUT_COLS = [
    "para_escritura",
    # === mismas columnas que dbo.dim_scannmarket_producto ===
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


UNMATCHED_LOC_OUTPUT_COLS = [
    "para_escritura",
    "Código único PDV",
    "Pdv Clasif 3",
    "Pdv RUC",
    "PDV_RAZON_SOCIAL",
    "Nombre PDV",
    "Estado PDV",
    "Localidad PDV",
    "Dirección PDV",
]

SCHEMA = 'siso'

# --------------------------------------------------------------------
# Conexión a SQL Server por ODBC (usa variables de entorno del .env)
# --------------------------------------------------------------------
SQL_SERVER   = os.getenv("SERVER", "")
SQL_DATABASE = os.getenv("DATABASE", "")
SQL_DRIVER   = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
ENCRYPT      = os.getenv("ODBC_ENCRYPT", "yes")           # yes|no
TRUST_CERT   = os.getenv("ODBC_TRUST_SERVER_CERT", "yes") # yes|no
TIMEOUT      = os.getenv("ODBC_TIMEOUT")                  # ej. "30"

SQL_UID = os.getenv("SQL_USERNAME")
SQL_PWD = os.getenv("SQL_PASSWORD")

def build_engine() -> Engine:
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
        parts.append("Trusted_Connection=yes")  # o Integrated Security=SSPI
    if TIMEOUT:
        parts.append(f"Connection Timeout={TIMEOUT}")
    odbc_str = ";".join(parts) + ";"
    params = urllib.parse.quote_plus(odbc_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,
        echo=False,
        hide_parameters=True,   # <— oculta valores en logs y tracebacks
    )

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# --------------------------------------------------------------------
# Selección del archivo del MES ANTERIOR
# --------------------------------------------------------------------

# Meses en español (incluye alias "setiembre")
MONTH_ALIASES: Dict[int, List[str]] = {
    1: ["enero"],
    2: ["febrero"],
    3: ["marzo"],
    4: ["abril"],
    5: ["mayo"],
    6: ["junio"],
    7: ["julio"],
    8: ["agosto"],
    9: ["septiembre", "setiembre"],
    10: ["octubre"],
    11: ["noviembre"],
    12: ["diciembre"],
}

def prev_month(d: Optional[date] = None) -> date:
    """Primer día del mes anterior a `d` (o a hoy)."""
    d = d or date.today()
    if d.month == 1:
        return date(d.year - 1, 12, 1)
    return date(d.year, d.month - 1, 1)

def prev_month(d: Optional[date] = None) -> date:
    d = d or date.today()
    return date(d.year - 1, 12, 1) if d.month == 1 else date(d.year, d.month - 1, 1)

def find_prev_month_csv(base_dir: str, today: Optional[date] = None) -> Tuple[str, str]:
    """
    Busca recursivamente el CSV del **mes anterior** bajo base_dir.

    Nombres aceptados (case-insensitive):
      - base_<mes><yyyy>.csv    ej: Base_octubre2025.csv
      - base-<mes><yyyy>.csv
      - base_<mes>_<yyyy>.csv   (robusto a subrayado)
      - base-<mes>-<yyyy>.csv   (robusto a guion)

    <mes> admite 'septiembre' o 'setiembre'. Si hay varios matches,
    devuelve el más reciente por fecha de modificación.
    """
    target = prev_month(today)
    aliases = MONTH_ALIASES[target.month]     # p.ej. ["septiembre", "setiembre"]
    exts = ["csv"]
    ext_group = "(?:" + "|".join(exts) + ")"

    # Patrones: Base[ _-]?<mes>[ _-]?<yyyy>.csv    (ignora may/min y acentos en el nombre real del archivo)
    patterns = [
        re.compile(
            rf"^base[ _-]?({alias})[ _-]?{target.year}\.{ext_group}$",
            re.IGNORECASE
        )
        for alias in aliases
    ]

    matches: List[Tuple[float, str]] = []
    for root, _, files in os.walk(base_dir):
        for fn in files:
            low = fn.lower()
            if not low.endswith(".csv"):
                continue
            if any(pat.match(fn) for pat in patterns):
                full = os.path.join(root, fn)
                try:
                    mtime = os.path.getmtime(full)
                except Exception:
                    mtime = 0.0
                matches.append((mtime, full))

    if not matches:
        # Ayuda para depurar
        probe = []
        for root, _, files in os.walk(base_dir):
            for fn in files:
                if fn.lower().startswith("base") and fn.lower().endswith(".csv"):
                    probe.append(fn)
                    if len(probe) >= 30:
                        break
            if len(probe) >= 30:
                break
        raise FileNotFoundError(
            "No se encontró CSV del mes anterior en '"
            + base_dir + "'.\n"
            + "Buscado por alias: " + ", ".join(aliases) + "\n"
            + "Ejemplos: Base_" + aliases[0] + f"{target.year}.csv o Base-{aliases[0]}{target.year}.csv\n"
            + "Algunos existentes: " + ", ".join(sorted(probe))
        )

    best = max(matches, key=lambda t: t[0])[1]
    print(f"[INFO] Archivo del mes anterior seleccionado: {best}")
    return best, ""   # sheet_name no aplica a CSV

# --------------------------------------------------------------------
# Normalización y limpiezas
# --------------------------------------------------------------------

def _strip_accents(s: str) -> str:
    if s is None:
        return s
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Quita acentos, pasa a minúsculas y colapsa espacios en TODOS los nombres de columnas."""
    new_cols = []
    for c in df.columns:
        base = _strip_accents(c).lower()
        base = re.sub(r"\s+", " ", base).strip()
        new_cols.append(base)
    df.columns = new_cols
    return df

def _to_number_series(s: pd.Series) -> pd.Series:
    # quita separadores de miles "." y cambia coma decimal por punto
    return pd.to_numeric(
        s.astype(str)
         .str.replace(r"\.", "", regex=True)
         .str.replace(",", ".", regex=False)
         .str.replace(r"\s+", "", regex=True),
        errors="coerce"
    )

def _norm_col(c: str) -> str:
    # normaliza para comparar nombres de columnas sin acentos / case
    return _strip_accents(c).strip().lower()

def _map_cols_case_insensitive(df: pd.DataFrame, desired_cols: list) -> dict:
    """
    Retorna un mapeo {desired_name -> existing_name_en_df} por coincidencia
    case-insensitive y sin acentos. Si no existe, mapea a None.
    """
    existing = { _norm_col(c): c for c in df.columns }
    mapping = {}
    for want in desired_cols:
        key = _norm_col(want)
        mapping[want] = existing.get(key)  # puede ser None si no existe
    return mapping

def _ensure_and_order(df: pd.DataFrame, ordered_cols: list) -> pd.DataFrame:
    # Crea columnas faltantes y reordena
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[ordered_cols]

def _pick_first(df: pd.DataFrame, *candidates) -> Optional[str]:
    """Devuelve el primer nombre de columna existente en df según el orden de candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _derive_presentacion_from_grams(v, categoria_str: str) -> str:
    """
    Construye la Presentacion_Producto a partir del contenido (gr/ml).
    Heurística básica: si la categoría contiene 'BEBID' o 'CAÑ', usamos 'ML.'; si no, 'GR.'.
    """
    if pd.isna(v) or str(v).strip() == "":
        return "-"
    try:
        num = int(float(str(v).replace(",", ".").strip()))
    except Exception:
        return str(v).strip()  # deja tal cual si no se puede parsear
    cat = (categoria_str or "").upper()
    if ("BEBID" in cat) or ("CAÑ" in cat) or ("VINO" in cat) or ("CERVE" in cat):
        return f"{num} ML."
    return f"{num} GR."


def build_unmatched_dfs_strict(df_src: pd.DataFrame,
                               dim_prod_db: pd.DataFrame,
                               dim_pdv_db: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Genera dataframes de PRODUCTOS y LOCALES no matcheados
    con EXACTAMENTE las columnas pedidas + 'para_escritura' al inicio.
    """
    # ================== PRODUCTOS ==================
    prod_keys = set(dim_prod_db["Id_Producto"].astype(str)) if "Id_Producto" in dim_prod_db.columns else set()

    # nombres reales (ya normalizaste a minúsculas antes en el pipeline)
    col_codigo  = _map_cols_case_insensitive(df_src, ["Código Barras SKU"]).get("Código Barras SKU")
    col_nombre  = _map_cols_case_insensitive(df_src, ["Nombre SKU"]).get("Nombre SKU")
    col_marca   = _map_cols_case_insensitive(df_src, ["Marca SKU"]).get("Marca SKU")
    col_cat     = _map_cols_case_insensitive(df_src, ["Categoría SKU"]).get("Categoría SKU")
    col_prov    = _map_cols_case_insensitive(df_src, ["Proveedor SKU"]).get("Proveedor SKU")
    col_cont    = _map_cols_case_insensitive(df_src, ["Cant Contenido SKU"]).get("Cant Contenido SKU")

    # Clasificaciones jerárquicas (tomamos una jerarquía razonable)
    col_p3  = _map_cols_case_insensitive(df_src, ["Prod Clasif 3"]).get("Prod Clasif 3")   # Segmento
    col_p4  = _map_cols_case_insensitive(df_src, ["Prod Clasif 4"]).get("Prod Clasif 4")   # Sub_Segmento
    col_p5  = _map_cols_case_insensitive(df_src, ["Prod Clasif 5"]).get("Prod Clasif 5")   # Variedades
    col_p6  = _map_cols_case_insensitive(df_src, ["Prod Clasif 6"]).get("Prod Clasif 6")   # especialidad
    col_p7  = _map_cols_case_insensitive(df_src, ["Prod Clasif 7"]).get("Prod Clasif 7")   # clasificacion
    col_p10 = _map_cols_case_insensitive(df_src, ["Prod Clasif 10"]).get("Prod Clasif 10") # Empaque

    if col_codigo is None:
        raise KeyError("No se encontró la columna 'Código Barras SKU' en la fuente para construir no matcheados de productos.")

    # filas cuyo Id_Producto NO existe en la DIM
    mask_prod_unmatched = ~df_src[col_codigo].astype(str).isin(prod_keys)
    prod_base = df_src.loc[mask_prod_unmatched].copy()

    # dedup por código de barras
    prod_base = prod_base.sort_index().drop_duplicates(subset=[col_codigo])

    # construir salidas con nombres EXACTOS de la DIM
    prod_out = pd.DataFrame(index=prod_base.index)
    prod_out["Id_Producto"]              = prod_base[col_codigo] if col_codigo else pd.NA
    prod_out["Descripcion_del_Producto"] = prod_base[col_nombre] if col_nombre else pd.NA
    prod_out["Marca"]                    = prod_base[col_marca]  if col_marca  else pd.NA
    prod_out["Categoria"]                = prod_base[col_cat]    if col_cat    else pd.NA

    # jerarquías (elige la mejor disponible)
    prod_out["Segmento"]       = prod_base[col_p3]  if col_p3  else pd.NA
    prod_out["Sub_Segmento"]   = prod_base[col_p4]  if col_p4  else pd.NA
    prod_out["Variedades"]     = prod_base[col_p5]  if col_p5  else pd.NA
    prod_out["especialidad"]   = prod_base[col_p6]  if col_p6  else pd.NA
    prod_out["clasificacion"]  = prod_base[col_p7]  if col_p7  else pd.NA
    prod_out["Empaque"]        = prod_base[col_p10] if col_p10 else pd.NA

    # proveedor
    prod_out["Proveedor"] = prod_base[col_prov] if col_prov else pd.NA

    # contenido → Gramos (numérico) + Presentacion_Producto (texto)
    if col_cont:
        grams = pd.to_numeric(
            prod_base[col_cont].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce"
        )
        prod_out["Gramos"] = grams
    else:
        prod_out["Gramos"] = pd.NA

    # Presentacion_Producto (ej. "85 GR." / "900 ML.")
    if col_cont:
        cat_series = prod_out["Categoria"].astype(str)
        prod_out["Presentacion_Producto"] = [
            _derive_presentacion_from_grams(v, c) for v, c in zip(prod_base[col_cont], cat_series)
        ]
    else:
        prod_out["Presentacion_Producto"] = "-"

    # defaults que no vienen en SCANN (alineados a ejemplos de la DIM)
    prod_out["Palermo_Otros"] = "OTROS"
    prod_out["talle"]         = "-"
    prod_out["unidades"]      = 0
    prod_out["es_promo"]      = "NO"

    # bandera al inicio
    prod_out.insert(0, "para_escritura", False)

    # asegurar orden exacto
    prod_out = _ensure_and_order(prod_out, UNMATCHED_PROD_OUTPUT_COLS)
    prod_out = prod_out.reset_index(drop=True)

    # ================== LOCALES ===================
    loc_keys = set(dim_pdv_db["Id_Comercial"].astype(str)) if "Id_Comercial" in dim_pdv_db.columns else set()

    loc_map = _map_cols_case_insensitive(df_src, ["Código único PDV"])
    codigo_unico_col = loc_map["Código único PDV"]
    if codigo_unico_col is None:
        raise KeyError("No se encontró la columna 'Código único PDV' en la fuente para construir no matcheados de locales.")

    mask_loc_unmatched = ~df_src[codigo_unico_col].astype(str).isin(loc_keys)
    loc_base = df_src.loc[mask_loc_unmatched].copy()

    # deduplicar por código único
    loc_base = loc_base.sort_index()
    loc_base = loc_base.drop_duplicates(subset=[codigo_unico_col])

    loc_cols_map = _map_cols_case_insensitive(loc_base, UNMATCHED_LOC_OUTPUT_COLS[1:])
    loc_out = pd.DataFrame(index=loc_base.index)
    for desired, existing in loc_cols_map.items():
        loc_out[desired] = loc_base[existing] if existing else pd.NA

    loc_out.insert(0, "para_escritura", False)
    loc_out = _ensure_and_order(loc_out, UNMATCHED_LOC_OUTPUT_COLS)

    return prod_out.reset_index(drop=True), loc_out.reset_index(drop=True)

def push_unmatched_to_drive(unmatched_prod: pd.DataFrame, unmatched_loc: pd.DataFrame):
    import io, pathlib
    GLOBAL_UNMATCHED_DIR = pathlib.Path(BASE_DOWNLOAD_DIR) / "no_mapeados_scann"
    GLOBAL_UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

    out_prod_xlsx = GLOBAL_UNMATCHED_DIR / "SCANN_PRODUCTOS_no_mapeados.xlsx"
    out_loc_xlsx  = GLOBAL_UNMATCHED_DIR / "SCANN_LOCALES_no_mapeados.xlsx"

    with pd.ExcelWriter(out_prod_xlsx, engine="xlsxwriter") as xlw:
        unmatched_prod.to_excel(xlw, index=False, sheet_name="unmatched")
    with pd.ExcelWriter(out_loc_xlsx, engine="xlsxwriter") as xlw:
        unmatched_loc.to_excel(xlw, index=False, sheet_name="unmatched")

    print(f"✅ No matcheados SCANN – PRODUCTOS: {len(unmatched_prod)} filas → {out_prod_xlsx}")
    print(f"✅ No matcheados SCANN – LOCALES:   {len(unmatched_loc)} filas → {out_loc_xlsx}")

    prod_buf = io.BytesIO()
    with pd.ExcelWriter(prod_buf, engine="xlsxwriter") as xlw:
        unmatched_prod.to_excel(xlw, index=False, sheet_name="unmatched")
    prod_buf.seek(0)

    loc_buf = io.BytesIO()
    with pd.ExcelWriter(loc_buf, engine="xlsxwriter") as xlw:
        unmatched_loc.to_excel(xlw, index=False, sheet_name="unmatched")
    loc_buf.seek(0)

    service = get_drive_service_sa(service_account_json_path="service_account.json", scopes=DRIVE_SCOPES)
    prod_id, prod_action = drive_upload_or_update_xlsx(service, FOLDER_ID_NO_MAPEADOS, "SCANN_PRODUCTOS_no_mapeados.xlsx", prod_buf.getvalue())
    loc_id,  loc_action  = drive_upload_or_update_xlsx(service, FOLDER_ID_NO_MAPEADOS,  "SCANN_LOCALES_no_mapeados.xlsx",  loc_buf.getvalue())

    print(f"☁️  SCANN PRODUCTOS: {prod_action} (fileId={prod_id}) en carpeta {FOLDER_ID_NO_MAPEADOS}")
    print(f"☁️  SCANN LOCALES:   {loc_action} (fileId={loc_id}) en carpeta {FOLDER_ID_NO_MAPEADOS}")

    notifs = []
    if not unmatched_prod.empty:
        notifs.append(f"Hay {len(unmatched_prod)} productos nuevos sin mapear.")
    if not unmatched_loc.empty:
        notifs.append(f"Hay {len(unmatched_loc)} sucursales nuevas sin mapear.")

    if notifs:  # solo envía si hay algo que avisar
        send_email(
            subject="[SCANN MARKET] Nuevos registros no mapeados",
            body="\n".join(notifs) + "\n\nRevisar los Excel en no_mapeados / Drive."
        )


def read_source(path: str, sheet: str = "") -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(path, sheet_name=(sheet or "Base (1)"), dtype=str)

    elif ext == ".csv":
        # Inspeccionar los primeros bytes para detectar BOM/UTF-16 y separador
        with open(path, "rb") as f:
            head = f.read(4096)

        # ¿UTF-16? (BOM o bytes nulos intercalados)
        is_utf16 = head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff") or (b"\x00" in head)

        if is_utf16:
            # TSV típico exportado desde Windows en UTF-16
            df = pd.read_csv(path, sep="\t", dtype=str, encoding="utf-16")
        else:
            # ¿Tab o coma?
            sep = "\t" if (b"\t" in head) else ","
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig")
            except UnicodeDecodeError:
                # Fallback para CSVs en ANSI/Windows-1252
                df = pd.read_csv(path, sep=sep, dtype=str, encoding="cp1252")

        # Limpiar posible BOM en nombres de columnas y descartar 'index' si vino incluida
        df.columns = [str(c).lstrip("\ufeff").strip() for c in df.columns]
        if "index" in df.columns:
            df = df.drop(columns=["index"])

        return df

    else:
        raise ValueError(f"Extensión no soportada: {ext}")


def upper_trim(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.upper()
    return df

def map_month_name_to_number(df: pd.DataFrame) -> pd.DataFrame:
    col = "mes de fecha"
    if col in df.columns:
        df[col] = df[col].map(lambda x: MONTH_MAP.get(str(x).strip().upper(), str(x)))
    return df

def build_fecha(df: pd.DataFrame) -> pd.DataFrame:
    ycol, mcol, dcol = "ano de fecha", "mes de fecha", "dia de fecha"
    def to_date(row):
        try:
            y = int(str(row.get(ycol, "")).strip())
            m = int(str(row.get(mcol, "")).strip())
            d = int(str(row.get(dcol, "")).strip())
            return pd.Timestamp(year=y, month=m, day=d).date()
        except Exception:
            return pd.NaT
    df["fecha"] = df.apply(to_date, axis=1)
    return df

def fix_codigo_unico(df: pd.DataFrame) -> pd.DataFrame:
    col = "codigo unico pdv"
    if col in df.columns:
        df[col] = df[col].replace(PDV_HOTFIX_REPLACE)
    return df

def derive_tipo_de_comercio(df: pd.DataFrame) -> pd.DataFrame:
    col = "pdv clasif 3"
    def classify(x: str) -> str:
        s = str(x or "").upper()
        if "SUPERMERCADO" in s: return "SUPERMERCADO"
        if "TIENDA" in s: return "TIENDA"
        if "DESPENSA" in s: return "DESPENSA"
        return ""
    if col in df.columns:
        df["tipo_de_comercio"] = df[col].map(classify)
    return df

def _get_col(df: pd.DataFrame, *names):
    """Devuelve la serie del df para el primer nombre (case/acentos-insensible) que exista."""
    if not hasattr(df, "__colmap__"):
        df.__colmap__ = { _norm_col(c): c for c in df.columns }
    for n in names:
        real = df.__colmap__.get(_norm_col(n))
        if real is not None:
            return df[real]
    # si no existe, devolvemos serie de NA del largo del df
    return pd.Series([pd.NA]*len(df), index=df.index)

_num_pattern = re.compile(r"[^0-9\.\-]")

def _num_es(x) -> float:
    """'56.000,75' -> 56000.75 ; '1.500' -> 1500.0 ; inválidos -> 0.0"""
    if pd.isna(x): 
        return 0.0
    s = str(x).strip().replace("\u00A0", "").replace(" ", "")
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    s = _num_pattern.sub("", s)
    try:
        return float(s)
    except Exception:
        return 0.0

def compute_aux_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Replica el cálculo de KNIME: kilos, cajas (cigarrillos) y fardos (pañales)."""
    out = df.copy()

    # Columnas fuente (tolerante a variaciones)
    categoria   = _get_col(out, "Categoría SKU", "categoria sku").astype(str).str.upper()
    prod10      = _get_col(out, "Prod Clasif 10", "prod clasif 10").astype(str).str.upper()
    vol_src     = _get_col(out, "Ventas en volumen", "ventas en volumen").map(_num_es)
    cant        = _get_col(out, "Cantidad de venta", "cantidad de venta").map(_num_es)
    contenido   = _get_col(out, "Cant Contenido SKU", "cant contenido sku").map(_num_es)

    print("volumne maximo es: ",vol_src.describe())

    # === kilos (KNIME: renombrar ventas en volumen a kilos) ===
    # si ventas en volumen viene vacía/0, usamos cantidad*contenido como fallback
    vol = vol_src.where(vol_src > 0, cant * contenido)
    out["kilos"] = vol

    # === cajas (solo CIGARRILLOS) ===
    mask_cig = (categoria == "CIGARRILLOS")
    cajas = pd.Series(0.0, index=out.index)

    # CAJETILLAS: ventas_en_volumen / 10000
    mask_caj = mask_cig & (prod10 == "CAJETILLAS")
    cajas.loc[mask_caj] = vol.loc[mask_caj] / 10000.0

    # GRUESAS: ((contenido * (10 si 20, si no 20)) * cantidad) / 10000
    mask_gru = mask_cig & (prod10 == "GRUESAS")
    factor = np.where(contenido == 20, 10.0, 20.0)
    cajas.loc[mask_gru] = ((contenido.loc[mask_gru] * factor[mask_gru]) * cant.loc[mask_gru]) / 10000.0

    out["cajas"] = cajas.fillna(0.0)

    # === fardos (solo PAÑALES) ===
    mask_pn = (categoria == "PAÑALES")
    fardos = pd.Series(0.0, index=out.index)
    fardos.loc[mask_pn] = ((vol.loc[mask_pn] * 30.0) / 1_000_000.0) / 0.00579
    out["fardos"] = fardos.fillna(0.0)

    # Sanitizar negativos y NaN por si acaso
    for col in ("kilos", "cajas", "fardos"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
        out.loc[out[col] < 0, col] = 0.0

    return out

# --------------------------------------------------------------------
# PDV mapping desde JSON fijo
# --------------------------------------------------------------------

def apply_pdv_mapeo_json(df: pd.DataFrame, json_path: Optional[str] = None) -> pd.DataFrame:
    """
    Corrige 'nombre pdv' desde JSON con:
    { "Nombre PDV": "...", "PDV_RAZON_SOCIAL": "...", "Nombre PDV Correcto": "..." }
    Coincide por (Nombre PDV + PDV_RAZON_SOCIAL) si ambas están; si no, por Nombre PDV.
    DataFrame esperado con columnas normalizadas: 'nombre pdv' y opcional 'pdv_razon_social'.
    """
    col_name = "nombre pdv"
    if col_name not in df.columns:
        return df

    path = json_path or PDV_MAP_JSON
    if not path or not os.path.exists(path):
        print(f"ADVERTENCIA: no se encontró el mapeo PDV en {path!r}; se omite.")
        return df

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        dic = pd.DataFrame(data)
    except Exception as e:
        print(f"ADVERTENCIA: no se pudo leer {path!r} como JSON: {e}; se omite.")
        return df

    expected = {"Nombre PDV", "Nombre PDV Correcto"}
    if not expected.issubset(set(dic.columns)):
        print("ADVERTENCIA: columnas requeridas ausentes en JSON; se omite.")
        return df

    def norm(x): return str(x).strip().upper()

    df2 = df.copy()
    df2["__pdv_norm__"] = df2[col_name].map(norm)

    has_razon_df = "pdv_razon_social" in df2.columns
    has_razon_map = "PDV_RAZON_SOCIAL" in dic.columns

    if has_razon_df and has_razon_map:
        df2["__razon_norm__"] = df2["pdv_razon_social"].map(norm)
        dic["__pdv_norm__"] = dic["Nombre PDV"].map(norm)
        dic["__razon_norm__"] = dic["PDV_RAZON_SOCIAL"].map(norm)
        map_df = dic[["__pdv_norm__", "__razon_norm__", "Nombre PDV Correcto"]].dropna()
        df2 = df2.merge(
            map_df.rename(columns={"Nombre PDV Correcto": "__pdv_corr__"}),
            on=["__pdv_norm__", "__razon_norm__"], how="left"
        )
    else:
        dic["__pdv_norm__"] = dic["Nombre PDV"].map(norm)
        map_df = dic[["__pdv_norm__", "Nombre PDV Correcto"]].dropna()
        df2 = df2.merge(
            map_df.rename(columns={"Nombre PDV Correcto": "__pdv_corr__"}),
            on=["__pdv_norm__"], how="left"
        )

    if "__pdv_corr__" in df2.columns:
        df2[col_name] = df2["__pdv_corr__"].fillna(df2[col_name]).map(lambda s: str(s).strip())
        df2 = df2.drop(columns=[c for c in ["__pdv_norm__", "__razon_norm__", "__pdv_corr__"] if c in df2.columns])

    return df2

# --------------------------------------------------------------------
# DIMs
# --------------------------------------------------------------------

def build_dim_producto(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "codigo barras sku": "Id_Producto",
        "categoria sku": "Categoria_SKU",
        "cant contenido sku": "Cant_Contenido_SKU",
        "escencia": "Escencia",
        "sabor": "Sabor",
    }
    prod_cols = [c for c in df.columns if c.startswith("prod clasif ")]
    keep = [c for c in ["codigo barras sku", "categoria sku", "cant contenido sku", "escencia", "sabor"] if c in df.columns]
    keep += prod_cols
    dim = df[keep].drop_duplicates().rename(columns=rename)
    if "Cant_Contenido_SKU" in dim.columns:
        dim["Cant_Contenido_SKU"] = pd.to_numeric(dim["Cant_Contenido_SKU"], errors="coerce").astype("Int64")
    dim = dim[dim["Id_Producto"].notna() & (dim["Id_Producto"].astype(str) != "")]
    return dim

def build_dim_pdv(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "codigo unico pdv": "Id_Comercial",
        "nombre pdv": "Nombre_PDV",
        "estado pdv": "Estado_PDV",
        "localidad pdv": "Localidad_PDV",
        "direccion pdv": "Direccion_PDV",
        "pdv ruc": "RUC_PDV",
        "pdv_razon_social": "Razon_Social_PDV",
        "pdv razon social": "Razon_Social_PDV",
        "pdv clasif 3": "Pdv_Clasif_3",
        "tipo_de_comercio": "Tipo_de_Comercio",
    }
    cols_presentes = [c for c in rename.keys() if c in df.columns]
    dim = df[cols_presentes].drop_duplicates().rename(columns=rename)
    dim = dim[dim["Id_Comercial"].notna() & (dim["Id_Comercial"].astype(str) != "")]
    return dim

def upsert_dim(engine: Engine, table: str, key_col: str, df_new: pd.DataFrame):
    if df_new.empty:
        return 0, 0
    try:
        existing = pd.read_sql(f"SELECT {key_col} FROM {table}", engine)
    except Exception:
        df_new.to_sql(table.split('.')[-1], con=engine, schema=table.split('.')[0], if_exists="append", index=False)
        return len(df_new), 0
    exist_keys = set(existing[key_col].astype(str))
    df_ins = df_new[~df_new[key_col].astype(str).isin(exist_keys)]
    if df_ins.empty:
        return 0, len(exist_keys)
    schema, tbl = table.split('.')
    df_ins.to_sql(tbl, con=engine, schema=schema, if_exists="append", index=False)
    return len(df_ins), len(exist_keys)

# --------------------------------------------------------------------
# FACT
# --------------------------------------------------------------------

def add_row_ids(df: pd.DataFrame) -> pd.DataFrame:
    now = pd.Timestamp.now().strftime("%m%d%H%M%S%f")
    df = df.copy()
    df["Id"] = [f"{i}_{now}" for i in range(len(df))]
    return df

def rename_columns_for_fact(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "codigo unico pdv": "Id_Comercial",
        "codigo barras sku": "Id_Producto",
        "precio por unidad": "Precio_Unitario",
        "cantidad de venta": "Cantidad",
        "ventas en valor": "Facturacion_Gs",
        "fecha": "Fecha",
    }
    return df.rename(columns=rename_map)


def reorder_fact_columns(df: pd.DataFrame) -> pd.DataFrame:
    wanted = ["Id", "Fecha", "Id_Comercial", "Id_Producto",
              "Cantidad", "Precio_Unitario", "Facturacion_Gs",
              "cajas", "fardos", "kilos"]
    cols = [c for c in wanted if c in df.columns]
    rest = [c for c in df.columns if c not in cols]
    out = df[cols + rest].copy()

    # Normaliza numéricos que pueden venir con miles '.' y decimal ','
    for c in ["Cantidad", "Precio_Unitario", "Facturacion_Gs", "cajas", "fardos", "kilos",
              "Ventas en volumen", "Cantidad de venta"]:
        if c in out.columns:
            out[c] = _to_number_series(out[c])

    if "Fecha" in out.columns:
        out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce").dt.date
    return out

def validate_keys(df: pd.DataFrame, dim_pdv: pd.DataFrame, dim_prod: pd.DataFrame):
    left = df.copy()
    if "Id_Producto" in left.columns and "Id_Producto" in dim_prod.columns:
        prod = dim_prod[["Id_Producto"]].drop_duplicates()
        left = left.merge(prod.assign(_join_ok_prod=1), on="Id_Producto", how="left")
    else:
        left["_join_ok_prod"] = 1
    if "Id_Comercial" in left.columns and "Id_Comercial" in dim_pdv.columns:
        pdv = dim_pdv[["Id_Comercial"]].drop_duplicates()
        left = left.merge(pdv.assign(_join_ok_pdv=1), on="Id_Comercial", how="left")
    else:
        left["_join_ok_pdv"] = 1
    ok_mask = left["_join_ok_prod"].notna() & left["_join_ok_pdv"].notna()
    return left[ok_mask].drop(columns=["_join_ok_prod","_join_ok_pdv"]), left[~ok_mask]

def write_errors(df_bad: pd.DataFrame, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    bad_prod = df_bad[df_bad.get("_join_ok_prod").isna()] if "_join_ok_prod" in df_bad.columns else df_bad.iloc[0:0]
    bad_pdv = df_bad[df_bad.get("_join_ok_pdv").isna()] if "_join_ok_pdv" in df_bad.columns else df_bad.iloc[0:0]
    if not bad_prod.empty:
        bad_prod.to_excel(os.path.join(outdir, "ERROR_PRODUCTOS.xlsx"), index=False, sheet_name="PRODUCTOS")
    if not bad_pdv.empty:
        bad_pdv.to_excel(os.path.join(outdir, "ERROR_PDV.xlsx"), index=False, sheet_name="PDV")
    if not df_bad.empty and bad_prod.empty and bad_pdv.empty:
        df_bad.to_excel(os.path.join(outdir, "ERROR_FACT_VALIDACIONES.xlsx"), index=False, sheet_name="PRODUCTOS")

def _periods_from_df(df: pd.DataFrame):
    """Devuelve lista ordenada de (year, month) presentes en df['Fecha']."""
    if "Fecha" not in df.columns:
        return []
    fechas = pd.to_datetime(df["Fecha"], errors="coerce")
    fechas = fechas.dropna()
    # Sin FutureWarning: usamos Period('M')
    periods = fechas.dt.to_period("M")
    return sorted({(p.year, p.month) for p in periods})

def insert_fact(engine, df_fact: pd.DataFrame):
    if df_fact.empty:
        print("No hay filas válidas para insertar en FACT.")
        return

    # por defecto, unidades = Cantidad si no viene
    if "unidades" not in df_fact.columns and "Cantidad" in df_fact.columns:
        df_fact["unidades"] = pd.to_numeric(df_fact["Cantidad"], errors="coerce").astype("Int64")

    # sanitizar numéricos para evitar NaN/Inf/overflow
    for col in ("kilos","cajas","fardos","Cantidad","Precio_Unitario","Facturacion_Gs"):
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors="coerce")
    for col in ("kilos","cajas","fardos"):
        if col in df_fact.columns:
            df_fact[col] = df_fact[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            df_fact[col] = df_fact[col].clip(lower=0, upper=1_000_000)

    cols = ["Id","Fecha","Id_Comercial","Id_Producto","Cantidad",
            "Precio_Unitario","Facturacion_Gs","unidades","cajas","fardos","kilos"]
    cols = [c for c in cols if c in df_fact.columns]

    periods = _periods_from_df(df_fact)
    if not periods:
        raise RuntimeError("No hay fechas válidas en df_fact para determinar período a reemplazar.")

    period_str = ", ".join([f"{y}-{m:02d}" for (y,m) in periods])
    print(f"Reemplazando FACT para períodos: {period_str}")

    try:
        with engine.begin() as conn:
            # 1) borrar cada (año, mes)
            for (y, m) in periods:
                del_sql = text(f"DELETE FROM {SCHEMA}.fact_scannmarket WHERE YEAR(Fecha)=:y AND MONTH(Fecha)=:m;")
                res = conn.execute(del_sql, {"y": int(y), "m": int(m)})
                rc = getattr(res, "rowcount", None)
                if rc is not None and rc >= 0:
                    print(f" - Borradas {rc} filas de {y}-{m:02d}")

            # 2) insertar nuevas filas
            print(f"Insertando {len(df_fact):,} filas en {SCHEMA}.fact_scannmarket (columnas: {cols})")
            df_fact[cols].to_sql(
                "fact_scannmarket",
                con=conn,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                method=None,      # importante para SQL Server
                chunksize=5000
            )
    except DBAPIError as e:
        msg = str(getattr(e, "orig", e))
        print("❌ Error SQL al reemplazar FACT:")
        print("Detalle:", msg)
        raise

# --------------------------------------------------------------------
# MAIN (sin argparse)
# --------------------------------------------------------------------

def main():
    # 1) Seleccionar archivo fuente (MES ANTERIOR) - CSV/TSV
    source_path, _ = find_prev_month_csv(BASE_DOWNLOAD_DIR)  # sheet_name no aplica a CSV
    print(f"source_path: {source_path}")

    # 2) Leer y normalizar columnas
    df = read_source(source_path, "")   # ignora segundo parámetro si es CSV
    df = normalize_column_names(df)
    print(f"Columnas: {df.columns}")

    print(df.head())

    # 3) Limpieza / derivadas
    df = upper_trim(df, CLEAN_COLS)
    df = map_month_name_to_number(df)
    df = build_fecha(df)
    df = fix_codigo_unico(df)
    df = derive_tipo_de_comercio(df)
    df = compute_aux_metrics(df)

    # 4) Correcciones de Nombre PDV desde JSON
    df = apply_pdv_mapeo_json(df, PDV_MAP_JSON)

    # 5) FACT: renombrar, agregar Id y preparar
    df_fact = rename_columns_for_fact(df)
    df_fact = add_row_ids(df_fact)
    df_fact = reorder_fact_columns(df_fact)

    # 6) Conexión a SQL (desde .env)
    engine = build_engine()

    # 7) Leer DIMs (SIN upsert) para validación y para armar no-matcheados
    dim_pdv_db  = pd.read_sql(f"SELECT * FROM {SCHEMA}.dim_scannmarket_pdv", engine)
    dim_prod_db = pd.read_sql(f"SELECT * FROM {SCHEMA}.dim_scannmarket_producto", engine)

    # 8) Validar claves contra DIMs (para insertar solo lo que matchea)
    df_ok, df_bad = validate_keys(
        df_fact,
        dim_pdv_db[["Id_Comercial"]],
        dim_prod_db[["Id_Producto"]],
    )
    write_errors(df_bad, OUTDIR)

    # 9) “No matcheados” EXACTOS (con 'para_escritura') + subida a Drive y mail
    unmatched_prod_out, unmatched_loc_out = build_unmatched_dfs_strict(df, dim_prod_db, dim_pdv_db)
    push_unmatched_to_drive(unmatched_prod_out, unmatched_loc_out)

    # 10) Insertar FACT (append)
    insert_fact(engine, df_ok)
    print("[OK] Proceso completado.")


if __name__ == "__main__":
    main()
