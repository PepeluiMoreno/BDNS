#!/usr/bin/env python
"""
Carga de concesiones a la base de datos.

Este script carga concesiones desde CSVs generados por extract_concesiones.py.
Optimizado para volumen alto (~1M registros/ano) usando:
- Procesamiento por lotes (batches)
- Bulk inserts con executemany
- Manejo de concurrencia

Uso:
    python -m ETL.concesiones.load.load_concesiones --year 2024
"""

import csv
import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import sys

# Agregar el directorio raiz al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from bdns_core.db.session import get_session
from bdns_core.db.models import Concesion, Instrumento, Beneficiario
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Configuracion
BATCH_SIZE = 5000  # Registros por batch
MAX_WORKERS = 4    # Threads para procesamiento paralelo
ETL_USER = "etl_system"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def safe_float(val: str) -> float | None:
    """Convierte string a float de forma segura."""
    if not val or val.strip() == '':
        return None
    try:
        return float(val.replace(',', '.'))
    except (ValueError, AttributeError):
        return None


def safe_int(val: str) -> int | None:
    """Convierte string a int de forma segura."""
    if not val or val.strip() == '':
        return None
    try:
        return int(val)
    except (ValueError, AttributeError):
        return None


def safe_str(val: str) -> str | None:
    """Limpia string de forma segura."""
    if not val:
        return None
    cleaned = val.strip()
    return cleaned if cleaned else None


def safe_date(val: str) -> datetime.date | None:
    """Convierte string a date de forma segura."""
    if not val or val.strip() == '':
        return None
    try:
        # Formato esperado: YYYY-MM-DD o DD/MM/YYYY
        if '/' in val:
            return datetime.strptime(val.strip(), '%d/%m/%Y').date()
        return datetime.strptime(val.strip(), '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return None


def load_csv_rows(csv_path: Path) -> List[Dict[str, str]]:
    """Carga filas del CSV."""
    with open(csv_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def prepare_concesion_dict(row: Dict[str, str], now: datetime) -> Dict[str, Any]:
    """
    Prepara un diccionario de concesion para insercion.

    El CSV debe tener estos campos (del extract_concesiones.py):
    - id: identificador unico de la concesion
    - codigo_bdns: codigo de la convocatoria
    - fecha_concesion: fecha de concesion
    - importe: importe de la concesion
    - ayuda_equivalente: ayuda equivalente
    - id_beneficiario: ID del beneficiario (ya resuelto en extract)
    - id_instrumento: ID del instrumento (opcional)
    """
    return {
        'id': safe_str(row.get('id')),
        'codigo_bdns': safe_int(row.get('codigo_bdns')),
        'fecha_concesion': safe_date(row.get('fecha_concesion')),
        'importe': safe_float(row.get('importe')),
        'ayuda_equivalente': safe_float(row.get('ayuda_equivalente')),
        'url_br': safe_str(row.get('url_br') or row.get('bases_reguladoras')),
        'tiene_proyecto': row.get('tiene_proyecto', '').lower() == 'true',
        'id_beneficiario': safe_int(row.get('id_beneficiario')),
        'id_instrumento': safe_int(row.get('id_instrumento')),
        # Campos de auditoria
        'created_at': now,
        'created_by': ETL_USER,
    }


def process_batch(batch: List[Dict[str, Any]], batch_num: int) -> Tuple[int, int, List[str]]:
    """
    Procesa un batch de concesiones.

    Returns:
        Tuple[int, int, List[str]]: (insertados, fallidos, errores)
    """
    inserted = 0
    failed = 0
    errors = []

    now = datetime.utcnow()
    prepared = []

    for row in batch:
        try:
            data = prepare_concesion_dict(row, now)
            if data['id'] and data['codigo_bdns'] and data['id_beneficiario']:
                prepared.append(data)
            else:
                failed += 1
                errors.append(f"Registro incompleto: id={data.get('id')}")
        except Exception as e:
            failed += 1
            errors.append(f"Error preparando registro: {e}")

    if not prepared:
        return inserted, failed, errors

    try:
        with get_session() as session:
            # Usar INSERT ... ON CONFLICT DO NOTHING para evitar duplicados
            stmt = pg_insert(Concesion).values(prepared)
            stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
            result = session.execute(stmt)
            session.commit()
            inserted = result.rowcount if hasattr(result, 'rowcount') else len(prepared)
            logger.info(f"Batch {batch_num}: {inserted} insertados")
    except IntegrityError as e:
        failed += len(prepared)
        errors.append(f"IntegrityError en batch {batch_num}: {e}")
        logger.error(f"IntegrityError en batch {batch_num}: {e}")
    except SQLAlchemyError as e:
        failed += len(prepared)
        errors.append(f"SQLAlchemyError en batch {batch_num}: {e}")
        logger.error(f"SQLAlchemyError en batch {batch_num}: {e}")

    return inserted, failed, errors


def main():
    parser = argparse.ArgumentParser(description='Cargar concesiones a la BD')
    parser.add_argument('--year', type=int, required=True, help='Ano de las concesiones')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Tamano del batch')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help='Numero de workers')
    parser.add_argument('--csv-path', type=str, help='Ruta al CSV (opcional)')
    args = parser.parse_args()

    # Determinar ruta del CSV
    if args.csv_path:
        csv_path = Path(args.csv_path)
    else:
        # Buscar en ubicaciones estandar
        possible_paths = [
            Path(__file__).parent.parent.parent.parent / "ETL" / "control" / f"concesiones_{args.year}.csv",
            Path(__file__).parent.parent.parent.parent / "data" / "populate" / f"concesiones_{args.year}.csv",
        ]
        csv_path = None
        for p in possible_paths:
            if p.exists():
                csv_path = p
                break

        if not csv_path:
            logger.error(f"No se encontro CSV para el ano {args.year}")
            logger.error(f"Rutas buscadas: {[str(p) for p in possible_paths]}")
            sys.exit(1)

    logger.info(f"Cargando concesiones desde: {csv_path}")
    logger.info(f"Batch size: {args.batch_size}, Workers: {args.workers}")

    # Cargar filas
    start_time = datetime.now()
    rows = load_csv_rows(csv_path)
    total = len(rows)
    logger.info(f"Total registros a procesar: {total:,}")

    # Dividir en batches
    batches = [rows[i:i + args.batch_size] for i in range(0, len(rows), args.batch_size)]
    logger.info(f"Total batches: {len(batches)}")

    # Procesar batches
    total_inserted = 0
    total_failed = 0
    all_errors = []

    # Procesamiento secuencial (mas seguro para BD)
    for i, batch in enumerate(batches, 1):
        inserted, failed, errors = process_batch(batch, i)
        total_inserted += inserted
        total_failed += failed
        all_errors.extend(errors)

        # Progreso cada 10 batches
        if i % 10 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = (i * args.batch_size) / elapsed if elapsed > 0 else 0
            logger.info(f"Progreso: {i}/{len(batches)} batches, {rate:.0f} reg/s")

    # Resumen
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"CARGA COMPLETADA - Ano {args.year}")
    logger.info("=" * 60)
    logger.info(f"Total procesados: {total:,}")
    logger.info(f"Insertados:       {total_inserted:,}")
    logger.info(f"Fallidos:         {total_failed:,}")
    logger.info(f"Tiempo:           {elapsed:.1f}s")
    logger.info(f"Velocidad:        {total / elapsed:.0f} reg/s")

    if all_errors:
        logger.warning(f"Errores ({len(all_errors)}):")
        for err in all_errors[:10]:  # Solo primeros 10
            logger.warning(f"  - {err}")

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
