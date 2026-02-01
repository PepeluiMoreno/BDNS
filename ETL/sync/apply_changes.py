# ETL/sync/apply_changes.py
"""
Aplica cambios detectados a la BD local.

Uso:
    python -m ETL.sync.apply_changes --archivo sync_changes_20260201_120000.json
    python -m ETL.sync.apply_changes --archivo sync_changes_20260201_120000.json --dry-run

Este script:
1. Lee el archivo de cambios generado por detect_changes.py
2. Aplica los cambios a la BD:
   - INSERT: nuevas concesiones
   - UPDATE: concesiones modificadas
   - DELETE: concesiones eliminadas
3. Los triggers PostgreSQL actualizan automaticamente las estadisticas

NOTA: Este script modifica la BD, usar --dry-run para verificar primero.
"""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, date
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.session import get_session
from app.db.models import Concesion, SyncControl
from ETL.etl_utils import get_or_create_dir

MODULO = "apply_changes"

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


def preparar_concesion(data: dict) -> dict:
    """Prepara datos de concesion para INSERT/UPDATE."""
    fecha = data.get("fechaConcesion")
    if fecha and len(fecha) >= 10:
        fecha = fecha[:10]
    else:
        fecha = None

    importe = data.get("importe")
    try:
        importe = float(importe) if importe else None
    except (ValueError, TypeError):
        importe = None

    ayuda_eq = data.get("ayudaEquivalente")
    try:
        ayuda_eq = float(ayuda_eq) if ayuda_eq else None
    except (ValueError, TypeError):
        ayuda_eq = None

    return {
        "id": data.get("idConcesion"),
        "codigo_bdns": data.get("codigoBDNS"),
        "id_beneficiario": data.get("idPersona"),
        "fecha_concesion": fecha,
        "importe": importe,
        "ayuda_equivalente": ayuda_eq,
        "url_br": data.get("urlBR"),
        "tiene_proyecto": data.get("tieneProyecto", "").lower() in ("true", "1", "s"),
        "id_instrumento": data.get("instrumento") or data.get("idInstrumento"),
        "updated_by": "sync_system",
    }


def aplicar_inserts(session, insert_data: dict, batch_size: int = 1000) -> int:
    """Aplica INSERTs de nuevas concesiones."""
    if not insert_data:
        return 0

    concesiones = [preparar_concesion(data) for data in insert_data.values()]
    total = 0

    for i in range(0, len(concesiones), batch_size):
        batch = concesiones[i:i + batch_size]
        stmt = pg_insert(Concesion).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
        result = session.execute(stmt)
        total += result.rowcount
        log(f"Inserted batch {i // batch_size + 1}: {result.rowcount} rows")

    return total


def aplicar_updates(session, update_data: dict) -> int:
    """Aplica UPDATEs de concesiones modificadas."""
    if not update_data:
        return 0

    total = 0
    for id_c, data in update_data.items():
        prepared = preparar_concesion(data)
        prepared["updated_at"] = datetime.utcnow()

        stmt = text("""
        UPDATE concesion SET
            codigo_bdns = :codigo_bdns,
            id_beneficiario = :id_beneficiario,
            fecha_concesion = :fecha_concesion,
            importe = :importe,
            ayuda_equivalente = :ayuda_equivalente,
            url_br = :url_br,
            tiene_proyecto = :tiene_proyecto,
            id_instrumento = :id_instrumento,
            updated_at = :updated_at,
            updated_by = :updated_by
        WHERE id = :id
        """)

        result = session.execute(stmt, prepared)
        total += result.rowcount

    log(f"Updated {total} rows")
    return total


def aplicar_deletes(session, deletes: list) -> int:
    """Aplica DELETEs de concesiones eliminadas."""
    if not deletes:
        return 0

    # Eliminar en batches para evitar queries muy grandes
    batch_size = 1000
    total = 0

    for i in range(0, len(deletes), batch_size):
        batch = deletes[i:i + batch_size]
        placeholders = ", ".join([f":id_{j}" for j in range(len(batch))])
        params = {f"id_{j}": id_c for j, id_c in enumerate(batch)}

        stmt = text(f"DELETE FROM concesion WHERE id IN ({placeholders})")
        result = session.execute(stmt, params)
        total += result.rowcount
        log(f"Deleted batch {i // batch_size + 1}: {result.rowcount} rows")

    return total


def actualizar_sync_control(sync_id: int, inserts: int, updates: int, deletes: int,
                            estado: str, error: str = None):
    """Actualiza el registro de sync_control con los resultados."""
    with get_session() as session:
        session.execute(text("""
        UPDATE sync_control SET
            inserts_aplicados = :inserts,
            updates_aplicados = :updates,
            deletes_aplicados = :deletes,
            estado = :estado,
            error = :error,
            updated_at = NOW()
        WHERE id = :sync_id
        """), {
            "sync_id": sync_id,
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes,
            "estado": estado,
            "error": error,
        })
        session.commit()


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Aplica cambios a la BD.")
    parser.add_argument("--archivo", type=str, required=True,
                        help="Archivo de cambios (JSON)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo mostrar resumen, no aplicar")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="Tamano de batch para inserts (default: 1000)")
    args = parser.parse_args()

    # Buscar archivo
    archivo_path = Path(args.archivo)
    if not archivo_path.exists():
        archivo_path = RUTA_SYNC / args.archivo
        if not archivo_path.exists():
            log(f"ERROR: Archivo no encontrado: {args.archivo}", "ERROR")
            return 1

    log(f"Leyendo cambios de: {archivo_path}")

    with open(archivo_path, encoding="utf-8") as f:
        data = json.load(f)

    cambios = data.get("cambios", {})
    resumen = data.get("resumen", {})

    log(f"Cambios a aplicar:")
    log(f"  - INSERTS: {resumen.get('inserts', 0)}")
    log(f"  - UPDATES: {resumen.get('updates', 0)}")
    log(f"  - DELETES: {resumen.get('deletes', 0)}")

    if args.dry_run:
        log("Modo dry-run: no se aplican cambios")
        return 0

    # Buscar sync_control asociado (por fechas)
    sync_id = None
    with get_session() as session:
        result = session.execute(text("""
        SELECT id FROM sync_control
        WHERE estado = 'detected'
        ORDER BY fecha_ejecucion DESC
        LIMIT 1
        """)).fetchone()
        if result:
            sync_id = result[0]

    try:
        with get_session() as session:
            # Aplicar cambios
            inserts = aplicar_inserts(
                session, cambios.get("insert_data", {}), args.batch_size
            )
            updates = aplicar_updates(session, cambios.get("update_data", {}))
            deletes = aplicar_deletes(session, cambios.get("deletes", []))

            session.commit()

            log(f"Aplicados: {inserts} inserts, {updates} updates, {deletes} deletes")

            # Actualizar sync_control
            if sync_id:
                actualizar_sync_control(
                    sync_id, inserts, updates, deletes, "completed"
                )
                log(f"Actualizado sync_control ID: {sync_id}")

            return 0

    except Exception as e:
        log(f"ERROR: {e}", "ERROR")
        if sync_id:
            actualizar_sync_control(sync_id, 0, 0, 0, "failed", str(e))
        return 1


if __name__ == "__main__":
    exit(main())
