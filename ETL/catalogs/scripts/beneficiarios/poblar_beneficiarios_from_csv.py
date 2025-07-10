# load_beneficiarios_from_csv.py
# Script para poblar la tabla de Beneficiarios y sus Pseudónimos a partir de archivos CSV de concesiones.
import csv
import logging
import re
import sys
from pathlib import Path
from collections import defaultdict
from sqlalchemy.dialects.postgresql import insert
from app.db.models import Beneficiario, Pseudonimo
from app.db.session import SessionLocal
from app.db.utils import normalizar

CONTROL_DIR = Path("ETL/control")

def extraer_nif_y_nombre(descripcion: str) -> tuple[str, str]:
    if not descripcion:
        return "", ""
    partes = descripcion.strip().split(maxsplit=1)
    if not partes:
        return "", ""
    if len(partes) == 1:
        return partes[0].strip(":-"), ""
    return partes[0].strip(":-"), partes[1].strip()

def deducir_forma_juridica(nif: str) -> str | None:
    if not nif:
        return None
    if "*" in nif:
        return "PF"
    match = re.match(r"([A-Z])", nif, re.I)
    if match:
        return match.group(1).upper()
    return None

def limpiar_nombre(nombre: str) -> str:
    nombre = re.sub(r'^[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+', '', nombre)
    nombre = re.sub(r'[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+$', '', nombre)
    nombre = re.sub(r'[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ .-]', '', nombre)
    return nombre.strip()

def procesar_csv_concesiones(archivo_csv, beneficiarios):
    with open(archivo_csv, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_persona = row.get("idPersona")
            beneficiario_raw = row.get("beneficiario") or ""
            if not id_persona or not beneficiario_raw.strip():
                continue
            nif, nombre = extraer_nif_y_nombre(beneficiario_raw)
            nombre_limpio = limpiar_nombre(nombre)
            if not nombre_limpio:
                continue
            nombre_norm = normalizar(nombre_limpio)
            forma_juridica = deducir_forma_juridica(nif)
            beneficiarios[id_persona].append((nif, nombre, nombre_limpio, nombre_norm, forma_juridica))
    # Devuelve el path del archivo para renombrarlo después
    return archivo_csv

def load_beneficiarios_y_pseudonimos(debug_mode=False):
    archivos = list(CONTROL_DIR.glob("concesiones*.csv"))
    logger = logging.getLogger()
    logger.info(f"Encontrados {len(archivos)} archivos de concesiones en {CONTROL_DIR}")

    beneficiarios = defaultdict(list)

    # 1. Recoger todos los beneficiarios de todos los archivos
    for archivo in archivos:
        logger.info(f"Procesando {archivo}")
        procesado = procesar_csv_concesiones(archivo, beneficiarios)
        # Renombrar a done_ solo si se procesa sin error
        archivo_path = Path(procesado)
        done_path = archivo_path.parent / f"done_{archivo_path.name}"
        archivo_path.rename(done_path)
        logger.info(f"Renombrado a {done_path}")

    inserts_beneficiarios = []
    inserts_pseudonimos = []

    # 2. Por cada id_persona, guarda como principal el nombre más largo/normalizado
    for id_persona, variantes in beneficiarios.items():
        variantes = sorted(variantes, key=lambda x: (-len(x[3]), x[3]))
        nif, nombre_original, nombre_limpio, nombre_norm, forma_juridica = variantes[0]

        inserts_beneficiarios.append({
            "id": id_persona,
            "nif": nif,
            "nombre": nombre_limpio,
            "nombre_norm": nombre_norm,
            "forma_juridica": forma_juridica,
        })

        pseudonimos_vistos = set()
        pseudonimos_vistos.add(nombre_norm)
        pseudonimos_encontrados = []

        for v in variantes[1:]:
            _, alt_original, alt_limpio, alt_norm, _ = v
            if alt_norm and alt_norm not in pseudonimos_vistos:
                inserts_pseudonimos.append({
                    "beneficiario_id": id_persona,
                    "pseudonimo": alt_limpio,
                    "pseudonimo_norm": alt_norm,
                })
                pseudonimos_vistos.add(alt_norm)
                pseudonimos_encontrados.append((alt_limpio, alt_norm))

        if debug_mode:
            logger.debug(f"[id_persona={id_persona}] Variantes encontradas ({len(variantes)}):")
            for v in variantes:
                logger.debug(f"    - NIF: {v[0]}, Original: '{v[1]}', Limpio: '{v[2]}', Norm: '{v[3]}', FormaJ: {v[4]}")
            if pseudonimos_encontrados:
                logger.debug(f"[id_persona={id_persona}] Pseudónimos detectados para principal '{nombre_limpio}' [{nombre_norm}]:")
                for pl, pn in pseudonimos_encontrados:
                    logger.debug(f"    - '{pl}' | '{pn}'")

    logger.info(f"Subiendo {len(inserts_beneficiarios)} beneficiarios y {len(inserts_pseudonimos)} pseudónimos a la BD...")

    with SessionLocal() as session:
        try:
            if inserts_beneficiarios:
                session.execute(
                    insert(Beneficiario)
                    .values(inserts_beneficiarios)
                    .on_conflict_do_nothing(index_elements=['id'])
                )
            if inserts_pseudonimos:
                session.execute(
                    insert(Pseudonimo)
                    .values(inserts_pseudonimos)
                    .on_conflict_do_nothing(index_elements=['beneficiario_id', 'pseudonimo_norm'])
                )
            session.commit()
            logger.info(f"Proceso terminado: {len(inserts_beneficiarios)} beneficiarios únicos, {len(inserts_pseudonimos)} pseudónimos.")
        except Exception as e:
            session.rollback()
            logger.exception(f"[BATCH ERROR] {e}")

if __name__ == "__main__":
    # Usa --debug para activar logging DEBUG de pseudónimos y variantes
    debug_mode = "--debug" in sys.argv

    logging.basicConfig(
        level=logging.DEBUG if debug_mode else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    load_beneficiarios_y_pseudonimos(debug_mode=debug_mode)






