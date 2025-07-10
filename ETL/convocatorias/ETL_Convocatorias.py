
# ETL_Convocatorias.py
# Orquestador ETL: descarga, transforma y enriquece convocatorias BDNS, con logs y mensajes uniformes

import json
import logging
from pathlib import Path
from datetime import datetime
import requests
import csv
from multiprocessing import Process, Queue
from ETL.etl_utils import get_or_create_dir
from app.db.session import SessionLocal
from app.db.utils import normalizar, buscar_organo_id
from app.db.models import (
    SectorActividad, Instrumento, TipoBeneficiario, SectorProducto, Region,
    Finalidad, Objetivo, Reglamento, Fondo
)

MODULO = "ETL_Convocatorias"
URL_BASE = "https://www.infosubvenciones.es/bdnstrans/api"

RUTA_JSONS = get_or_create_dir("ETL", "json", "convocatorias")
RUTA_LOGS = get_or_create_dir("ETL", "logs")
RUTA_CONTROL = get_or_create_dir("ETL", "control")
FALTANTES_CSV = RUTA_CONTROL / "catalogos_faltantes.csv"

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_extract_convocatorias.log",
    level=logging.INFO,
    format="[%(asctime)s] [{}] [%(levelname)s] %(message)s".format(MODULO)
)

CATALOGOS = {
    "instrumentos": Instrumento,
    "tiposBeneficiarios": TipoBeneficiario,
    "sectores": SectorActividad,
    "sectoresProducto": SectorProducto,
    "regiones": Region,
    "finalidades": Finalidad,
    "objetivos": Objetivo,
    "reglamentos": Reglamento,
    "fondos": Fondo,
}

def print_msg(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")

def descargar_convocatorias(tipo, anio):
    json_file = RUTA_JSONS / f"convocatorias_{tipo}_{anio}.json"
    if json_file.exists():
        try:
            if json_file.stat().st_size == 0:
                logging.warning(f"{json_file} está vacío, se va a regenerar.")
                raise ValueError("Archivo vacío")
            with open(json_file, encoding="utf-8") as f:
                datos = json.load(f)
            logging.info(f"Ya existe {json_file}, contiene {len(datos)} registros")
            return len(datos) if datos else 0
        except Exception as e:
            logging.warning(f"{json_file} está vacío o malformado ({e}), se regenera...")
            json_file.unlink(missing_ok=True)

    resultados = []
    page = 0
    page_size = 10000
    total_esperado = None
    fechaDesde = f"01/01/{anio}"
    fechaHasta = f"31/12/{anio}"

    try:
        while True:
            params = {
                "page": page,
                "pageSize": page_size,
                "order": "numeroConvocatoria",
                "direccion": "asc",
                "fechaDesde": fechaDesde,
                "fechaHasta": fechaHasta,
                "tipoAdministracion": tipo
            }
            url = f"{URL_BASE}/convocatorias/busqueda"
            response = requests.get(url, params=params, timeout=180)
            response.raise_for_status()
            data = response.json()
            contenido = data.get("content", [])
            resultados.extend(contenido)
            if total_esperado is None:
                total_esperado = data.get("totalElements", 0)
            logging.info(f"Página {page} descargada ({len(contenido)} registros) para {tipo}-{anio}")
            if len(resultados) >= total_esperado:
                break
            page += 1
    except Exception as e:
        logging.error(f"Error al descargar {tipo}-{anio} página {page}: {e}")

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    logging.info(f"{len(resultados)} convocatorias extraídas para {tipo}-{anio}")
    return len(resultados)

def registrar_faltante(nombre_catalogo, descripcion, faltantes_registrados):
    clave = (nombre_catalogo, descripcion)
    if clave not in faltantes_registrados:
        with open(FALTANTES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([nombre_catalogo, descripcion])
        faltantes_registrados.add(clave)

def enriquecer_detalle(session, entrada, faltantes_registrados):
    cod = entrada.get("numeroConvocatoria")
    url = f"{URL_BASE}/convocatorias?numConv={cod}"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        detalle = r.json()
    except Exception as e:
        logging.error(f"Error recuperando detalle {cod}: {e}")
        return

    for campo, modelo in CATALOGOS.items():
        valores = detalle.get(campo)
        if not valores:
            continue
        for item in valores:
            norm = normalizar(item.get("descripcion")) if "descripcion" in item else None
            existente = None
            if norm:
                existente = session.query(modelo).filter_by(descripcion_norm=norm).first()
            elif item.get("id"):
                existente = session.query(modelo).get(item.get("id"))
            if existente:
                item["id"] = existente.id
            else:
                desc = item.get("descripcion") or str(item)
                registrar_faltante(modelo.__tablename__, desc, faltantes_registrados)

    organo = detalle.get("organo")
    organo_id = None
    if organo:
        nivel1 = organo.get("nivel1")
        nivel2 = organo.get("nivel2")
        nivel3 = organo.get("nivel3")
        organo_id = buscar_organo_id(session, nivel1, nivel2, nivel3)
        if organo_id:
            organo_actualizado = {"id": organo_id}
            organo_actualizado.update(organo)
            detalle["organo"] = organo_actualizado
        # No warning de órgano: la gestión de faltantes se hace arriba
    entrada.update(detalle)

def transform_worker(tipo, anio, mes, queue):
    json_file = RUTA_JSONS / f"convocatorias_{tipo}_{anio}.json"
    if not json_file.exists():
        msg = f"Archivo no encontrado: {json_file}"
        logging.error(msg)
        print_msg(msg, "ERROR")
        return
    with open(json_file, encoding="utf-8") as f:
        datos = json.load(f)
    datos_mes = [e for e in datos if e.get("fechaRegistro", "").startswith(f"{anio}-{mes:02d}")]
    session = SessionLocal()
    faltantes_registrados = set()
    completadas = 0
    for entrada in datos_mes:
        enriquecer_detalle(session, entrada, faltantes_registrados)
        completadas += 1
        queue.put(("progreso", tipo, mes, completadas))
    session.close()
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    queue.put(("finalizado", tipo, mes, completadas))

def monitor(queue, total_hijos):
    totales = {}
    completadas = 0
    t0 = datetime.now()
    while total_hijos:
        msg = queue.get()
        if msg[0] == "progreso":
            _, tipo, mes, count = msg
            totales[(tipo, mes)] = count
            completadas = sum(totales.values())
            elapsed = (datetime.now() - t0).total_seconds()
            print_msg(f"Progreso: {completadas} convocatorias enriquecidas. Tiempo transcurrido: {elapsed:.1f}s")
        elif msg[0] == "finalizado":
            total_hijos -= 1

def extract_and_transform_convocatorias(anio):
    queue = Queue()
    procs = []
    for tipo in ["C", "A", "L", "O"]:
        for mes in range(1, 13):
            p = Process(target=transform_worker, args=(tipo, anio, mes, queue))
            p.start()
            procs.append(p)
    monitor_proc = Process(target=monitor, args=(queue, len(procs)))
    monitor_proc.start()
    for p in procs:
        p.join()
    monitor_proc.join()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Descarga, transforma y enriquece JSONs de convocatorias de un ejercicio con concurrencia")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    print_msg(f"Extrayendo JSONs agrupados del ejercicio {year}")
    total_general = 0
    resumen = {}
    for tipo in ["C", "A", "L", "O"]:
        print_msg(f"Extrayendo tipo {tipo}...")
        subtotal = descargar_convocatorias(tipo, year)
        resumen[tipo] = subtotal
        total_general += subtotal

    print_msg("RESUMEN:")
    for tipo, subtotal in resumen.items():
        print_msg(f"{tipo}: {subtotal} convocatorias")
        logging.info(f"Subtotal {tipo}: {subtotal} convocatorias")
    print_msg(f"TOTAL GENERAL: {total_general} convocatorias")
    logging.info(f"TOTAL GENERAL: {total_general} convocatorias")

    print_msg(f"Transformando convocatorias del ejercicio {year}")
    extract_and_transform_convocatorias(year)
    print_msg("Proceso completado.")

if __name__ == "__main__":
    main()
