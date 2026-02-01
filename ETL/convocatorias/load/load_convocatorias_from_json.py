# load_convocatorias_from_json.py
# Carga las convocatorias de un JSON mensual, marcando las cargadas como 'loaded' en el CSV de control.

import logging
import json
import csv
from pathlib import Path
from app.db.session import SessionLocal
from app.db.models import (
    Convocatoria, Instrumento, TipoBeneficiario, Finalidad, Objetivo, Organo,
    Reglamento, Fondo, Region, SectorActividad, SectorProducto
)
from ETL.etl_utils import get_or_create_dir

logger = logging.getLogger("load_convocatorias_from_json")
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
    )

def registrar_pendiente(nombre_catalogo, descripcion):
    """Registra un valor pendiente (sin duplicados) en ETL/convocatorias/pending/<catalogo>_pending.csv."""
    pending_dir = get_or_create_dir("ETL", "convocatorias", "pending")
    pending_file = Path(pending_dir) / f"{nombre_catalogo}_pending.csv"
    if not hasattr(registrar_pendiente, "vistos"):
        registrar_pendiente.vistos = set()
    clave = (nombre_catalogo, str(descripcion))
    if clave in registrar_pendiente.vistos:
        return
    registrar_pendiente.vistos.add(clave)
    with open(pending_file, "a", encoding="utf-8", newline="") as f:
        f.write(f"{descripcion}\n")

def load_convocatorias_from_json(json_path, csv_path):
    """
    Carga todas las convocatorias de un JSON mensual.
    Marca como 'loaded' en el CSV las que se cargan.
    Devuelve completadas, pendientes.
    """
    get_or_create_dir("ETL", "convocatorias", "pending")
    get_or_create_dir("logs")

    json_path = Path(json_path)
    if not json_path.exists():
        logger.error(f"No existe el JSON: {json_path}")
        return 0, 0

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    # Cargar CSV de control
    csv_path = Path(csv_path)
    if not csv_path.exists():
        logger.error(f"No existe el CSV de control: {csv_path}")
        return 0, len(data)

    with open(csv_path, encoding="utf-8", newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    completadas, pendientes = 0, 0
    codigos_procesados = set()

    for entrada in data:
        pendientes_entry = []
        codigo_bdns = str(entrada.get("codigo_bdns") or entrada.get("numeroConvocatoria") or entrada.get("id"))
        if not codigo_bdns:
            logger.warning("Convocatoria sin codigo_bdns, ignorada")
            continue

        def check_fk(session, modelo, nombre, valor):
            if not valor:
                registrar_pendiente(nombre, valor)
                pendientes_entry.append(nombre)
                return None
            id_val = valor.get("id") if isinstance(valor, dict) else valor
            obj = session.query(modelo).get(id_val)
            if not obj:
                registrar_pendiente(nombre, valor)
                pendientes_entry.append(nombre)
                return None
            return obj.id

        with SessionLocal() as session:
            try:
                id_organo = check_fk(session, Organo, "organo", entrada.get("organo"))
                id_reglamento = check_fk(session, Reglamento, "reglamento", entrada.get("reglamento"))
                id_finalidad = check_fk(session, Finalidad, "finalidad", entrada.get("finalidad"))
                id_objetivo = check_fk(session, Objetivo, "objetivo", entrada.get("objetivo"))
                id_instrumento = check_fk(session, Instrumento, "instrumento", entrada.get("instrumento"))
                id_tipo_beneficiario = check_fk(session, TipoBeneficiario, "tipo_beneficiario", entrada.get("tipoBeneficiario"))
                id_sector_actividad = check_fk(session, SectorActividad, "sector_actividad", entrada.get("sectorActividad"))
                id_sector_producto = check_fk(session, SectorProducto, "sector_producto", entrada.get("sectorProducto"))
                id_region = check_fk(session, Region, "region", entrada.get("region"))
                id_fondo = check_fk(session, Fondo, "fondo", entrada.get("fondo"))

                if pendientes_entry:
                    pendientes += 1
                    continue

                convocatoria = Convocatoria(
                    codigo_bdns=codigo_bdns,
                    descripcion=entrada.get("descripcion"),
                    descripcion_leng=entrada.get("descripcionLeng"),
                    fecha_recepcion=entrada.get("fechaRecepcion"),
                    mrr=entrada.get("mrr"),
                    organo_id=id_organo,
                    reglamento_id=id_reglamento,
                    finalidad_id=id_finalidad,
                    objetivo_id=id_objetivo,
                    instrumento_id=id_instrumento,
                    tipo_beneficiario_id=id_tipo_beneficiario,
                    sector_actividad_id=id_sector_actividad,
                    sector_producto_id=id_sector_producto,
                    region_id=id_region,
                    fondo_id=id_fondo,
                )

                # Relaciones N:M
                def set_n_m(attr, model, key):
                    items = entrada.get(key) or []
                    objs = [session.query(model).get(i["id"]) for i in items if i.get("id")]
                    setattr(convocatoria, attr, objs)

                set_n_m("instrumentos", Instrumento, "instrumentos")
                set_n_m("tipos_beneficiarios", TipoBeneficiario, "tiposBeneficiarios")
                set_n_m("finalidades", Finalidad, "finalidades")
                set_n_m("objetivos", Objetivo, "objetivos")
                set_n_m("reglamentos", Reglamento, "reglamentos")
                set_n_m("fondos", Fondo, "fondos")
                set_n_m("regiones", Region, "regiones")
                set_n_m("sectores_actividad", SectorActividad, "sectoresActividad")
                set_n_m("sectores_producto", SectorProducto, "sectoresProducto")

                session.merge(convocatoria)
                session.commit()
                completadas += 1
                codigos_procesados.add(codigo_bdns)
            except Exception as e:
                session.rollback()
                logger.error(f"Error cargando convocatoria {codigo_bdns}: {e}")
                pendientes += 1

    # Actualizar CSV: marcar como 'loaded' las completadas
    for row in rows:
        if row.get("codigo_bdns") in codigos_procesados:
            row["status"] = "loaded"

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Si todas las convocatorias se cargaron, elimina el JSON mensual
    if completadas and completadas + pendientes == len(data):
        try:
            json_path.unlink()
            logger.info(f"Eliminado JSON mensual {json_path} tras cargar todas las convocatorias.")
        except Exception as e:
            logger.error(f"No se pudo eliminar {json_path}: {e}")

    logger.info(f"JSON {json_path.name}: {completadas} completadas, {pendientes} pendientes.")
    print(f"{json_path.name}: {completadas} completadas, {pendientes} pendientes.")

    return completadas, pendientes

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carga convocatorias de un JSON mensual en la BD y marca el CSV de control.")
    parser.add_argument("--json", "-j", type=str, required=True, help="Ruta al JSON mensual")
    parser.add_argument("--csv", "-c", type=str, required=True, help="Ruta al CSV de control")
    args = parser.parse_args()
    load_convocatorias_from_json(args.json, args.csv)





