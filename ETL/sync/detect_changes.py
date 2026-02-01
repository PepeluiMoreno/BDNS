# ETL/sync/detect_changes.py
"""
Detecta cambios en BDNS respecto a datos locales.

Uso:
    python -m ETL.sync.detect_changes --meses 48
    python -m ETL.sync.detect_changes --meses 24 --dry-run

Este script:
1. Calcula la ventana temporal (hoy - N meses hasta hoy)
2. Extrae IDs de concesiones de API BDNS en esa ventana
3. Compara con IDs de concesiones locales en esa ventana
4. Detecta:
   - NUEVAS: ids_api - ids_local
   - ELIMINADAS: ids_local - ids_api
   - MODIFICADAS: ids_api & ids_local con diferencias en campos clave
5. Genera archivo de cambios para apply_changes.py
"""

import csv
import json
import hashlib
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from app.db.session import get_session
from app.db.models import SyncControl
from ETL.etl_utils import get_or_create_dir

MODULO = "detect_changes"

# Rutas
RUTA_SYNC = get_or_create_dir("sync")
RUTA_LOGS = get_or_create_dir("logs")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)


def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")


def calcular_hash(concesion: dict) -> str:
    """Calcula hash de campos clave para detectar modificaciones."""
    campos = [
        str(concesion.get("idConcesion", "")),
        str(concesion.get("codigoBDNS", "")),
        str(concesion.get("idPersona", "")),
        str(concesion.get("importe", "")),
        str(concesion.get("fechaConcesion", ""))[:10],
    ]
    return hashlib.md5("|".join(campos).encode()).hexdigest()


def obtener_concesiones_api(fecha_desde: date, fecha_hasta: date,
                            max_workers: int = 5) -> dict[str, dict]:
    """
    Obtiene concesiones de la API BDNS en la ventana temporal.

    Returns:
        Dict {id_concesion: {datos, hash}}
    """
    # API de BDNS para concesiones por rango de fechas
    # NOTA: La API real puede tener paginacion, ajustar segun documentacion
    base_url = "https://www.infosubvenciones.es/bdnstrans/api/concesiones"

    concesiones = {}
    current = fecha_desde

    while current <= fecha_hasta:
        year = current.year
        month = current.month

        url = f"{base_url}?ejercicio={year}&mes={month:02d}"
        log(f"Consultando API: {url}")

        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            for c in data if isinstance(data, list) else []:
                id_c = c.get("idConcesion")
                if id_c:
                    concesiones[id_c] = {
                        "data": c,
                        "hash": calcular_hash(c),
                    }
        except Exception as e:
            log(f"Error consultando API para {year}-{month:02d}: {e}", "ERROR")

        # Siguiente mes
        current = (current.replace(day=1) + relativedelta(months=1))

    return concesiones


def obtener_concesiones_local(fecha_desde: date, fecha_hasta: date) -> dict[str, str]:
    """
    Obtiene IDs y hashes de concesiones locales en la ventana.

    Returns:
        Dict {id_concesion: hash}
    """
    sql = """
    SELECT
        id,
        MD5(
            id || '|' ||
            COALESCE(codigo_bdns::TEXT, '') || '|' ||
            COALESCE(id_beneficiario::TEXT, '') || '|' ||
            COALESCE(importe::TEXT, '') || '|' ||
            COALESCE(TO_CHAR(fecha_concesion, 'YYYY-MM-DD'), '')
        ) as hash
    FROM concesion
    WHERE fecha_concesion >= :fecha_desde
      AND fecha_concesion <= :fecha_hasta
    """

    with get_session() as session:
        rows = session.execute(text(sql), {
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta
        }).fetchall()

    return {row[0]: row[1] for row in rows}


def detectar_cambios(concesiones_api: dict, concesiones_local: dict) -> dict:
    """
    Compara concesiones de API vs local y detecta cambios.

    Returns:
        Dict con listas de inserts, updates, deletes
    """
    ids_api = set(concesiones_api.keys())
    ids_local = set(concesiones_local.keys())

    # Nuevas: en API pero no en local
    inserts = ids_api - ids_local

    # Eliminadas: en local pero no en API
    deletes = ids_local - ids_api

    # Posibles modificadas: en ambos
    updates = set()
    for id_c in ids_api & ids_local:
        hash_api = concesiones_api[id_c]["hash"]
        hash_local = concesiones_local[id_c]
        if hash_api != hash_local:
            updates.add(id_c)

    return {
        "inserts": list(inserts),
        "updates": list(updates),
        "deletes": list(deletes),
        "insert_data": {id_c: concesiones_api[id_c]["data"] for id_c in inserts},
        "update_data": {id_c: concesiones_api[id_c]["data"] for id_c in updates},
    }


def guardar_cambios(cambios: dict, fecha_desde: date, fecha_hasta: date) -> Path:
    """Guarda los cambios detectados en un archivo JSON."""
    out_path = RUTA_SYNC / f"sync_changes_{datetime.now():%Y%m%d_%H%M%S}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "fecha_desde": fecha_desde.isoformat(),
            "fecha_hasta": fecha_hasta.isoformat(),
            "fecha_deteccion": datetime.now().isoformat(),
            "resumen": {
                "inserts": len(cambios["inserts"]),
                "updates": len(cambios["updates"]),
                "deletes": len(cambios["deletes"]),
            },
            "cambios": cambios,
        }, f, ensure_ascii=False, indent=2)

    return out_path


def registrar_sync(ventana_meses: int, fecha_desde: date, fecha_hasta: date,
                   total_api: int, total_local: int, cambios: dict,
                   estado: str = "detected", error: str = None):
    """Registra la ejecucion en la tabla sync_control."""
    with get_session() as session:
        sync = SyncControl(
            fecha_ejecucion=datetime.now(),
            ventana_meses=ventana_meses,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            total_api=total_api,
            total_local=total_local,
            inserts_detectados=len(cambios.get("inserts", [])),
            updates_detectados=len(cambios.get("updates", [])),
            deletes_detectados=len(cambios.get("deletes", [])),
            estado=estado,
            error=error,
        )
        session.add(sync)
        session.commit()
        return sync.id


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Detecta cambios en BDNS.")
    parser.add_argument("--meses", type=int, default=48,
                        help="Ventana de meses hacia atras (default: 48)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo mostrar resumen, no guardar")
    parser.add_argument("--workers", type=int, default=5,
                        help="Workers paralelos para API (default: 5)")
    args = parser.parse_args()

    ventana_meses = args.meses
    fecha_hasta = date.today()
    fecha_desde = fecha_hasta - relativedelta(months=ventana_meses)

    log(f"Detectando cambios en ventana de {ventana_meses} meses")
    log(f"Rango: {fecha_desde} a {fecha_hasta}")

    try:
        # Obtener datos de API
        log("Obteniendo concesiones de API BDNS...")
        concesiones_api = obtener_concesiones_api(fecha_desde, fecha_hasta, args.workers)
        log(f"Total en API: {len(concesiones_api)}")

        # Obtener datos locales
        log("Obteniendo concesiones locales...")
        concesiones_local = obtener_concesiones_local(fecha_desde, fecha_hasta)
        log(f"Total local: {len(concesiones_local)}")

        # Detectar cambios
        log("Comparando...")
        cambios = detectar_cambios(concesiones_api, concesiones_local)

        log(f"Cambios detectados:")
        log(f"  - NUEVAS: {len(cambios['inserts'])}")
        log(f"  - MODIFICADAS: {len(cambios['updates'])}")
        log(f"  - ELIMINADAS: {len(cambios['deletes'])}")

        if args.dry_run:
            log("Modo dry-run: no se guardan cambios")
            return 0

        # Guardar cambios
        out_path = guardar_cambios(cambios, fecha_desde, fecha_hasta)
        log(f"Cambios guardados en: {out_path}")

        # Registrar en BD
        sync_id = registrar_sync(
            ventana_meses=ventana_meses,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            total_api=len(concesiones_api),
            total_local=len(concesiones_local),
            cambios=cambios,
            estado="detected"
        )
        log(f"Registrado en sync_control con ID: {sync_id}")

        return 0

    except Exception as e:
        log(f"Error: {e}", "ERROR")
        registrar_sync(
            ventana_meses=ventana_meses,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            total_api=0,
            total_local=0,
            cambios={},
            estado="failed",
            error=str(e)
        )
        return 1


if __name__ == "__main__":
    exit(main())
