#!/usr/bin/env python3
"""
run_sp.py — Ejecutar SQL / Stored Procedures en SQL Server desde Linux (Airflow-friendly)

¿Para qué sirve este script?
----------------------------
Este script está pensado para que Apache Airflow (o una ejecución manual en Debian)
pueda ejecutar comandos SQL en SQL Server, típicamente Stored Procedures, usando ODBC
(Microsoft ODBC Driver 18 for SQL Server) a través de pyodbc.

Es útil cuando:
- Tenés pipelines con tareas Python que cargan CSVs/ETLs.
- Luego necesitás ejecutar procedimientos almacenados (SPs) para actualizar dimensiones/facts.
- Querés un ejecutor simple, confiable y reproducible en un entorno no interactivo (Airflow).

Características
---------------
- Carga automática de variables desde un archivo `.env` ubicado en el mismo directorio del script.
- `override=True`: el `.env` tiene prioridad sobre variables ya existentes en el entorno
  (evita que un servicio/systemd “pise” valores con configuraciones viejas).
- Valida variables requeridas y falla con mensaje claro.
- Soporta tanto:
    - `python run_sp.py "EXEC schema.sp_name;"`  (Stored Procedures)
    - `python run_sp.py "SELECT 1"`            (consultas simples de prueba)
- Controla opciones típicas del driver 18:
    - Encrypt / TrustServerCertificate / Timeout
- No imprime la contraseña en logs.

Uso
---
1) Ubicar `.env` en el mismo directorio:
   /opt/airflow/scripts/automatizacion_siso/.env

   Ejemplo mínimo:
     SERVER=172.16.12.89,1433
     DATABASE=KNIME
     SQL_USERNAME=sellout_knime
     SQL_PASSWORD=Sellout26*
     ODBC_DRIVER=ODBC Driver 18 for SQL Server

2) Probar manual:
   (venv-scripts) $ python run_sp.py "SELECT 1"
   (venv-scripts) $ python run_sp.py "EXEC siso.sp_dim_clientes;"

3) Usar desde Airflow (BashOperator):
   /opt/airflow/venv-scripts/bin/python run_sp.py "EXEC siso.sp_dim_clientes;"

Notas importantes
-----------------
- Para instancias nombradas tipo 172.16.12.89\\DES en Linux suele ser más estable usar puerto:
  SERVER=172.16.12.89,1433  (coma) en vez de backslash.
- Este script usa autocommit=True para SPs (típico en tareas de carga/refresh).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyodbc

try:
    from dotenv import load_dotenv
except ImportError as e:
    raise SystemExit(
        "Falta dependencia: python-dotenv. Instalá con:\n"
        "  pip install python-dotenv\n"
    ) from e


SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"


@dataclass(frozen=True)
class SqlServerConfig:
    """Configuración de conexión a SQL Server tomada de variables de entorno."""
    server: str
    database: str
    username: str
    password: str

    odbc_driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: str = "yes"
    trust_server_certificate: str = "yes"
    timeout: int = 30


def _load_env(env_path: Path) -> None:
    """
    Carga variables desde .env.

    override=True: si el proceso ya tiene variables (por ejemplo systemd),
    el .env las sobrescribe para asegurar consistencia.
    """
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        # No cortamos acá: puede que el entorno venga seteado por otro mecanismo.
        print(f"[WARN] No existe {env_path}. Se intentará usar variables del entorno.", file=sys.stderr)


def _get_required(name: str) -> str:
    """Obtiene una variable requerida o falla con mensaje claro."""
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Falta variable requerida: {name}")
    return value


def _get_int(name: str, default: int) -> int:
    """Obtiene una variable entera con default."""
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"Variable {name} debe ser int. Valor actual: {raw!r}")


def read_config() -> SqlServerConfig:
    """Lee configuración desde entorno (preferentemente ya cargado desde .env)."""
    server = _get_required("SERVER")
    database = _get_required("DATABASE")
    username = _get_required("SQL_USERNAME")
    password = _get_required("SQL_PASSWORD")

    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    encrypt = os.getenv("ODBC_ENCRYPT", "yes")
    trust = os.getenv("ODBC_TRUST_SERVER_CERT", "yes")
    timeout = _get_int("ODBC_TIMEOUT", 30)

    return SqlServerConfig(
        server=server,
        database=database,
        username=username,
        password=password,
        odbc_driver=odbc_driver,
        encrypt=encrypt,
        trust_server_certificate=trust,
        timeout=timeout,
    )


def build_connection_string(cfg: SqlServerConfig) -> str:
    """
    Construye el connection string ODBC para SQL Server.

    Nota:
    - ODBC Driver 18 requiere Encrypt=yes por defecto en muchos entornos.
    - TrustServerCertificate=yes permite confiar en el cert sin validar CA.
    """
    return (
        f"DRIVER={{{cfg.odbc_driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.username};"
        f"PWD={cfg.password};"
        f"Encrypt={cfg.encrypt};"
        f"TrustServerCertificate={cfg.trust_server_certificate};"
    )


def safe_conn_summary(cfg: SqlServerConfig) -> str:
    """Resumen seguro (sin password) para logs."""
    return (
        f"SERVER={cfg.server};DATABASE={cfg.database};UID={cfg.username};"
        f"DRIVER={cfg.odbc_driver};Encrypt={cfg.encrypt};TrustServerCertificate={cfg.trust_server_certificate};"
        f"Timeout={cfg.timeout}"
    )


def execute_sql(sql: str, cfg: SqlServerConfig) -> None:
    """
    Ejecuta SQL en SQL Server.

    - Usa autocommit=True (recomendado para SPs de mantenimiento/carga).
    - Si el SQL devuelve filas (por ejemplo SELECT), imprime la primera fila.
      (esto ayuda para pruebas como SELECT 1).
    """
    cs = build_connection_string(cfg)

    # Conectar
    cn = pyodbc.connect(cs, timeout=cfg.timeout)
    cn.autocommit = True

    try:
        cur = cn.cursor()
        cur.execute(sql)

        # Si hay resultados (SELECT), intentamos traer 1 fila para debug
        row: Optional[tuple] = None
        try:
            row = cur.fetchone()
        except pyodbc.ProgrammingError:
            # No results (común en EXEC)
            row = None

        if row is not None:
            print(f"OK: {sql} -> {row}")
        else:
            print(f"OK: {sql}")
    finally:
        cn.close()


def main() -> None:
    if len(sys.argv) < 2:
        print('Uso: python run_sp.py "EXEC schema.proc;"', file=sys.stderr)
        print('Ej:  python run_sp.py "SELECT 1"', file=sys.stderr)
        sys.exit(2)

    sql = sys.argv[1].strip()
    if not sql:
        raise SystemExit("SQL vacío. Pasá un string con el comando SQL.")

    # 1) Cargar .env (si existe)
    _load_env(ENV_PATH)

    # 2) Leer config desde env
    cfg = read_config()

    # Log seguro
    print("[INFO] Ejecutando SQL en SQL Server con:")
    print("[INFO]", safe_conn_summary(cfg))

    # 3) Ejecutar
    execute_sql(sql, cfg)


if __name__ == "__main__":
    main()
