from datetime import date, timedelta
from typing import List, Tuple, Optional
import pathlib
import re

MONTH_NAMES_ES = {
     1: "Enero",  2: "Febrero",  3: "Marzo",
     4: "Abril",  5: "Mayo",     6: "Junio",
     7: "Julio",  8: "Agosto",   9: "Septiembre",
    10: "Octubre",11: "Noviembre",12: "Diciembre",
}

def month_name_es(dt: date) -> str:
    return MONTH_NAMES_ES[dt.month]

def month_file_candidates(dt: date) -> List[str]:
    year = dt.year
    mes  = month_name_es(dt)
    # soportar ambos formatos: "2025-Mayo.csv" y "Mayo-2025.csv"
    return [f"{year}-{mes}.csv", f"{mes}-{year}.csv"]

def month_boundaries(dt: date) -> Tuple[date, date]:
    start = dt.replace(day=1)
    # primer día del mes siguiente
    if start.month == 12:
        end = start.replace(year=start.year+1, month=1, day=1)
    else:
        end = start.replace(month=start.month+1, day=1)
    return start, end

def find_month_csv_for_category(category_dir: str, base_dir: str, today: Optional[date]=None) -> pathlib.Path:
    """
    Busca el CSV del mes actual directamente dentro de:
        <base_dir>/<category_dir>/
    Acepta dos patrones de nombre:
        YYYY-Mes.csv   p.ej. 2025-Agosto.csv
        Mes-YYYY.csv   p.ej. Agosto-2025.csv
    """
    today = today or date.today()
    cat_dir = pathlib.Path(base_dir) / category_dir
    if not cat_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta de categoría {category_dir}: {cat_dir}")

    candidates = month_file_candidates(today)  # ["2025-Agosto.csv", "Agosto-2025.csv"]
    # 1) Coincidencia exacta
    for cand in candidates:
        p = cat_dir / cand
        if p.exists():
            return p

    # 2) Búsqueda por regex (tolerante a may/min)
    mes = month_name_es(today)
    pattern = re.compile(rf"^((?:{today.year}-{mes})|(?:{mes}-{today.year}))\\.csv$", re.IGNORECASE)
    matches = [p for p in cat_dir.glob("*.csv") if pattern.match(p.name)]
    if matches:
        # si hay más de uno, quedate con el más reciente
        return max(matches, key=lambda x: x.stat().st_mtime)

    # 3) Ayuda para depurar: listar lo que hay
    existentes = sorted([p.name for p in cat_dir.glob("*.csv")])
    raise FileNotFoundError(
        f"No se encontró CSV del mes en {cat_dir}.\n"
        f"Buscado: {candidates}\n"
        f"Archivos existentes: {existentes[:20]}"
    )