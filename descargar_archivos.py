#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
descargar_archivos.py
---------------------------------------------------
Descarga el archivo CSV del mes vigente desde cuatro carpetas
compartidas de Google Drive.  Pensado para ejecutarse sin
intervención humana (por ejemplo, en el Task Scheduler).

Downloads the current month’s CSV from four shared Google Drive
folders.  Designed for fully head-less execution (e.g., Windows
Task Scheduler).
"""

from __future__ import annotations

import datetime
import os, io
import pathlib
from typing import List, Dict

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# ---------------------------------------------------------------------------
# Configuración / Configuration
# ---------------------------------------------------------------------------

def str_to_bool(value: str) -> bool:
    "Transforma un string a booleano"
    return str(value).lower() in ("true", "1", "yes", "on")

PRODUCCION_STR   = os.getenv("PRODUCCION", False)
PRODUCCION = str_to_bool(PRODUCCION_STR)

print(f"PRODUCCION_STR: {PRODUCCION_STR}")
print(f"PRODUCCION: {PRODUCCION}")

# 1. Ruta local donde se guardarán los archivos descargados
#    Local folder where reports will be saved
DOWNLOAD_ROOT: pathlib.Path = pathlib.Path.home() / "Documents" / "descargas_drive" / "biggie"
if PRODUCCION:
    DOWNLOAD_ROOT = "/opt/airflow/data/descargas_drive/biggie"
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

DOWNLOAD_ROOT_SM = pathlib.Path.home() / "Documents" / "descargas_drive" / "scannmarket"
if PRODUCCION:
    DOWNLOAD_ROOT_SM = "/opt/airflow/data/descargas_drive/scannmarket"
DOWNLOAD_ROOT_SM.mkdir(parents=True, exist_ok=True)

SCANNMARKET_FOLDER_NAME = "SCANNMARKET"   # o pone SCANNMARKET_FOLDER_ID si lo tenés
SCANNMARKET_FOLDER_ID = '12pIx4NlpsqakB7joAGVmTzYV11Gj5VST'


# 2. Archivos de credenciales
SERVICE_ACCOUNT_FILE = "service_account.json"   # clave de cuenta de servicio / service-account key
OAUTH_CLIENT_FILE    = "credentials.json"       # cliente de escritorio (fallback interactivo)
TOKEN_FILE           = "token_drive.pickle"     # se genera tras el primer login OAuth

# 3. Scopes mínimos para lectura
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# 4. ID de la carpeta raíz "PALERMO_DATA_SET"
ROOT_FOLDER_ID = "1xzVYBeBFvyWysx27JiHEsaq6MLaKraa8"

# 5. Meses en español (fijos → independiente de la locale)
MONTH_NAMES_ES = {
     1: "Enero",  2: "Febrero",  3: "Marzo",
     4: "Abril",  5: "Mayo",     6: "Junio",
     7: "Julio",  8: "Agosto",   9: "Septiembre",
    10: "Octubre",11: "Noviembre",12: "Diciembre",
}

# 5. Categorías esperadas (corresponden a nombres de subcarpetas)
CATEGORIES = ["YERBA", "VAPES", "PANALES", "CARAMELOS", "BALANCEADOS", "CIGARRILLOS"]

MONTH_ALIASES = {
    "Septiembre": ["Septiembre", "Setiembre"],
    # si querés, podés agregar otros alias aquí
}

# ---------------------------------------------------------------------------
# Funciones de autenticación / Authentication helpers
# ---------------------------------------------------------------------------

def build_drive_service():
    """
    Devuelve un objeto service autenticado.  Intenta primero la cuenta
    de servicio; si no existe, recurre al flujo OAuth interactivo
    (sólo la primera vez).

    Returns an authenticated Drive API service.  Tries service-account
    credentials first; if absent, falls back to interactive OAuth
    (only on the first run).
    """
    credentials = None

    # --- 1) service account -------------------------------------------------
    sa_path = pathlib.Path(SERVICE_ACCOUNT_FILE)
    if sa_path.exists():
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        return build("drive", "v3", credentials=credentials)

    # --- 2) OAuth fallback --------------------------------------------------
    token_path = pathlib.Path(TOKEN_FILE)
    if token_path.exists():
        with token_path.open("rb") as f:
            credentials = pickle.load(f)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_FILE, SCOPES)
            credentials = flow.run_local_server(port=0)
        with token_path.open("wb") as f:
            pickle.dump(credentials, f)

    return build("drive", "v3", credentials=credentials)

# ---------------------------------------------------------------------------
# Funciones de Drive / Drive helpers
# ---------------------------------------------------------------------------

def query_children(
    service,
    parent_id: str,
    *,
    must_be_folder: bool,
    name_equals: str | None = None,
) -> List[dict]:
    """
    Devuelve los hijos directos de `parent_id` que cumplan los filtros.

    Returns the direct children of `parent_id` matching the filters.
    """
    mime_filter = (
        "mimeType='application/vnd.google-apps.folder'"
        if must_be_folder
        else "mimeType!='application/vnd.google-apps.folder'"
    )

    query_parts = [f"'{parent_id}' in parents", mime_filter, "trashed=false"]
    if name_equals:
        query_parts.append(f"name='{name_equals}'")
    query = " and ".join(query_parts)

    response = (
        service.files()
        .list(
            q=query,
            fields="files(id, name)",
            pageSize=1000,
            # corpora="user",             # incluye Mi unidad + Compartidos :contentReference[oaicite:2]{index=2}
            corpora="allDrives",           
            spaces="drive",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    return response.get("files", [])


def download_drive_file(service, file_id: str, destination: pathlib.Path) -> None:
    """
    Descarga un archivo de Drive a `destination` en chunks de 5 MB
    (valor por defecto de MediaIoBaseDownload).

    Download a Drive file to `destination` in 5 MB chunks.
    """
    request = service.files().get_media(
        fileId=file_id, supportsAllDrives=True
    )
    with io.FileIO(destination, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def debug_list_root_contents(service):
    print("\n Listando carpetas dentro de PALERMO_DATA_SET:")
    items = query_children(service, ROOT_FOLDER_ID, must_be_folder=True)
    for item in items:
        print(f"  - {item['name']} (ID: {item['id']})")

def debug_list_all_visible_in_root(service):
    print("\n Explorando todo lo que se ve dentro de PALERMO_DATA_SET:")
    response = service.files().list(
        q=f"'{ROOT_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name, mimeType)",
        pageSize=1000,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    
    items = response.get("files", [])
    if not items:
        print("  (vacío)")
    else:
        for item in items:
            tipo = " Carpeta" if item["mimeType"] == "application/vnd.google-apps.folder" else " Archivo"
            print(f"  {tipo}: {item['name']} (ID: {item['id']})")
# ---------------------------------------------------------------------------
# Lógica principal / Main logic
# ---------------------------------------------------------------------------

def fetch_and_download_monthly_reports() -> None:
    """Descarga el archivo del mes desde cada carpeta de categoría."""
    today = datetime.date.today()

    # Si es día 1, tomamos el último día del mes anterior; si no, usamos hoy
    if today.day == 1:
        target_date = today.replace(day=1) - datetime.timedelta(days=1)
    else:
        target_date = today

    year_str = str(target_date.year)
    month_es = MONTH_NAMES_ES[target_date.month]
    target_name = f"{year_str}-{month_es}.csv"

    # candidatos tolerantes: Septiembre/Setiembre + dos órdenes
    aliases = MONTH_ALIASES.get(month_es, [month_es])
    candidates = []
    for a in aliases:
        candidates += [f"{year_str}-{a}.csv", f"{a}-{year_str}.csv"]

    service = build_drive_service()

    debug_list_root_contents(service)

    debug_list_all_visible_in_root(service)

    for category in CATEGORIES:
        print(f"\n▶ {category}")

        # 1. Buscar carpeta de la categoría dentro de PALERMO_DATA_SET
        cat_folders = query_children(
            service,
            ROOT_FOLDER_ID,
            must_be_folder=True,
            name_equals=category,
        )
        if not cat_folders:
            print(f"  ❌ Carpeta {category} no encontrada")
            continue
        cat_folder_id = cat_folders[0]["id"]

        # 2. Buscar subcarpeta del año
        year_folders = query_children(
            service,
            cat_folder_id,
            must_be_folder=True,
            name_equals=year_str,
        )
        if not year_folders:
            print(f"  ❌ Carpeta del año {year_str} no encontrada en {category}")
            continue
        year_folder_id = year_folders[0]["id"]

        # 3. Buscar archivo del mes dentro del año
        file_candidates = query_children(
            service,
            year_folder_id,
            must_be_folder=False,
        )
        # match exacto (case-insensitive)
        cand_lower = {c.lower() for c in candidates}
        # file_match = next(
        #     (f for f in file_candidates if f["name"] == target_name),
        #     None,
        # )
        file_match = next(
            (f for f in file_candidates if f["name"].lower() in cand_lower),
            None,
        )
        if not file_match:
            file_match = next(
                (f for f in file_candidates
                 if f["name"].lower().endswith(".csv")
                 and year_str in f["name"]
                 and any(a.lower() in f["name"].lower() for a in aliases)),
                None,
            )
        if not file_match:
            nombres = ", ".join(sorted(f['name'] for f in file_candidates))
            print(f"  ⚠️ No se encontró {candidates} en {category}. En {year_str} hay: {nombres}")
            continue

        # 4. Descargar
        target_dir = DOWNLOAD_ROOT / category
        target_dir.mkdir(parents=True, exist_ok=True)
        destination_path = target_dir / target_name
        download_drive_file(service, file_match["id"], destination_path)
        print(f"  ✅ Guardado en {destination_path}")

def fetch_and_download_scannmarket() -> None:
    """Descarga el CSV del mes ANTERIOR de la carpeta SCANNMARKET (archivo: Base_MesAño*.csv)."""
    today = datetime.date.today()
    prev_month = today.replace(day=1) - datetime.timedelta(days=1)

    year_str = str(prev_month.year)
    month_es = MONTH_NAMES_ES[prev_month.month]
    aliases  = MONTH_ALIASES.get(month_es, [month_es])


    # patrones típicos: Base_MesAño.csv, Base_Mes-Año.csv, Base_Mes_Año.csv
    candidates = []
    for a in aliases:
        base = f"base_{a}".lower()
        candidates += [
            f"{base}{year_str}.csv",
            f"{base}-{year_str}.csv",
            f"{base}_{year_str}.csv",
        ]

    service = build_drive_service()

    # localizar carpeta SCANNMARKET
    try:
        scann_folder_id = SCANNMARKET_FOLDER_ID  # si definiste el ID arriba
    except NameError:
        folders = query_children(service, ROOT_FOLDER_ID, must_be_folder=True, name_equals=SCANNMARKET_FOLDER_NAME)
        if not folders:
            print(f"❌ Carpeta {SCANNMARKET_FOLDER_NAME} no encontrada en la raíz.")
            return
        scann_folder_id = folders[0]["id"]

    # listar archivos (no hay subcarpetas por año)
    files = query_children(service, scann_folder_id, must_be_folder=False)

    # match case-insensitive por candidatos o por regla “empieza con Base_ y contiene Mes/Año”
    names_lower = {f["name"].lower(): f for f in files}
    file_match = next((names_lower[c] for c in candidates if c in names_lower), None)

    if not file_match:
        file_match = next(
            (f for f in files
             if f["name"].lower().startswith("base_")
             and any(a.lower() in f["name"].lower() for a in aliases)
             and year_str in f["name"]),
            None,
        )

    if not file_match:
        disponibles = ", ".join(sorted(f['name'] for f in files))
        print(f"⚠️ No encontré archivo Base_{month_es}{year_str}. En la carpeta hay: {disponibles}")
        return

    # descargar
    dest = DOWNLOAD_ROOT_SM / file_match["name"]
    download_drive_file(service, file_match["id"], dest)
    print(f"✅ Scannmarket guardado en {dest}")


if __name__ == "__main__":
    # Biggie (carpetas por categoría/año)
    fetch_and_download_monthly_reports()

    # Scannmarket (único archivo por mes, directo en la carpeta)
    fetch_and_download_scannmarket()
