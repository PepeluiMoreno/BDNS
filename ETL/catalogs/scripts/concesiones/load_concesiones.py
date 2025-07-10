import csv
import argparse
from pathlib import Path
import logging
import time
from app.db.session import get_session
from app.db.models import Concesion, Instrumento, Beneficiario  # importa los catálogos necesarios
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from filelock import FileLock

LOCK_TIMEOUT = 30  # segundos

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def safe_float(val):
    try:
        return float(val.replace(',', '.')) if val else None
    except Exception:
        return None

def safe_str(val):
    return val.strip() if val else None

def get_instrumento_id(session, desc):
    """Devuelve el id del instrumento a partir de la descripción, o None si no existe."""
    if not desc:
        return None
    obj = session.query(Instrumento).filter(Instrumento.descripcion == desc.strip()).first()
    return obj.id if obj else None

def get_beneficiario_id(session, nombre):
    """Devuelve el id del beneficiario a partir del nombre, o None si no existe."""
    if not nombre:
        return None
    obj = session.query(Beneficiario).filter(Beneficiario.nombre == nombre.strip()).first()
    return obj.id if obj else None

def process_row(row, session):
    """Procesa e inserta/actualiza una concesión."""
    try:
        concesion = session.query(Concesion).filter_by(codigo_concesion=row["codigo_concesion"]).first()
        if not concesion:
            concesion = Concesion(codigo_concesion=row["codigo_concesion"])
            session.add(concesion)

        concesion.codigo_bdns = safe_str(row["codigo_bdns"])
        concesion.fecha_concesion = safe_str(row["fecha_concesion"])
        concesion.importe = safe_float(row["importe"])
        concesion.ayuda_equivalente = safe_float(row["ayuda_equivalente"])
        concesion.url_br = safe_str(row.get("bases_reguladoras"))

        # Manejo correcto de relaciones: sólo claves foráneas
        concesion.id_instrumento = get_instrumento_id(session, row.get("instrumento"))
        concesion.id_beneficiario = get_beneficiario_id(session, row.get("beneficiario"))

        # Añadir otros campos/fks de la misma forma...
        session.commit()
        return "done", ""
    except (IntegrityError, DataError) as sqle:
        session.rollback()
        logger.error(f"[SQL ERROR] {row['codigo_concesion']}: {sqle}")
        return "error", str(sqle)
    except SQLAlchemyError as sqle:
        session.rollback()
        logger.error(f"[SQLAlchemy ERROR] {row['codigo_concesion']}: {sqle}")
        return "error", str(sqle)
    except Exception as e:
        session.rollback()
        logger.error(f"[EXCEPTION] {row['codigo_concesion']}: {e}")
        return "error", str(e)

def load_csv_rows(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows

def write_csv_rows(csv_path, rows, fieldnames):
    lock = FileLock(str(csv_path) + ".lock", timeout=LOCK_TIMEOUT)
    with lock:
        tmp_path = csv_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        tmp_path.replace(csv_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True)
    args = parser.parse_args()
    year = args.year

    csv_path = Path("data/populate") / f"concesiones_{year}.csv"
    rows = load_csv_rows(csv_path)
    fieldnames = rows[0].keys() if rows else []

    total = len(rows)
    procesadas = 0

    for i, row in enumerate(rows):
        if row["status"] == "done":
            continue
        # Reintentos para errores transitorios de SQL/conexión
        for intento in range(3):
            try:
                with get_session() as session:
                    status, error = process_row(row, session)
                row["status"] = status
                row["last_error"] = error
                row["retries"] = str(int(row.get("retries", "0")) + 1)
                if status == "done":
                    procesadas += 1
                    logger.info(f"[OK] ({i+1}/{total}) Concesión {row['codigo_concesion']} grabada.")
                else:
                    logger.error(f"[ERROR] ({i+1}/{total}) {row['codigo_concesion']}: {error}")
                break
            except Exception as e:
                logger.error(f"[RETRY {intento+1}] ({i+1}/{total}) {row['codigo_concesion']}: {e}")
                time.sleep(2 * (intento+1))  # espera incremental
        # Guardar CSV protegido tras cada fila para tolerancia a fallos/concurrencia
        try:
            write_csv_rows(csv_path, rows, fieldnames)
        except Exception as e:
            logger.error(f"[CSV ERROR] No se pudo guardar el CSV tras procesar {row['codigo_concesion']}: {e}")

    logger.info(f"Proceso terminado. {procesadas}/{total} concesiones procesadas.")

if __name__ == "__main__":
    main()
