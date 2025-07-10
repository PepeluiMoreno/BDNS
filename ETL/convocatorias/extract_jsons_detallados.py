# extract_control_csv.py
# Genera o actualiza el CSV de control de convocatorias por ejercicio y tipo

import requests
import csv
from pathlib import Path
from datetime import datetime
import logging
import time
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

TIPOS = ["C", "A", "L", "O"]

def get_logger():
    logger = logging.getLogger("extract_control_csv")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] [extract_control_csv] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger

logger = get_logger()

@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=2, max=20))
def fetch_api(url, params):
    resp = requests.get(url, params=params, timeout=180)
    resp.raise_for_status()
    return resp.json()

def fetch_codigos_bdns(year, tipo, csv_path):
    url = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias/busqueda"
    page = 0
    page_size = 10000
    total_esperado = None

    existentes_csv = {}
    if csv_path.exists():
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cod = row.get('codigo_bdns')
                if cod:
                    existentes_csv[cod] = row

    nuevos = []
    while True:
        params = {
            "fechaDesde": f"01/01/{year}",
            "fechaHasta": f"31/12/{year}",
            "page": page,
            "pageSize": page_size,
            "order": "numeroConvocatoria",
            "direccion": "asc",
            "tipoAdministracion": tipo,
        }
        try:
            data = fetch_api(url, params)
        except Exception as e:
            logger.error(f"[{tipo}] ERROR página {page}: {e}")
            break

        contenido = data.get("content", [])
        logger.info(f"[{tipo}] Página {page}: {len(contenido)} registros")

        for item in contenido:
            codigo_bdns = str(item.get("numeroConvocatoria") or item.get("id"))
            fecha_recepcion = (item.get("fechaRecepcion", "") or "")[:10] or "sin_fecha"
            if codigo_bdns:
                if codigo_bdns not in existentes_csv:
                    nuevos.append({
                        "codigo_bdns": codigo_bdns,
                        "fecha_recepcion": fecha_recepcion,
                        "tipo_administracion": tipo,
                        "status": "pending",
                        "last_error": "",
                        "last_attempt": "",
                        "retries": "0"
                    })
                else:
                    # Si ya existe, actualizamos fecha_recepcion y tipo si han cambiado
                    row = existentes_csv[codigo_bdns]
                    if (row.get("fecha_recepcion") != fecha_recepcion) or (row.get("tipo_administracion") != tipo):
                        row["fecha_recepcion"] = fecha_recepcion
                        row["tipo_administracion"] = tipo

        if total_esperado is None:
            total_esperado = data.get("totalElements", 0)

        if not contenido or (len(existentes_csv) + len(nuevos)) >= total_esperado:
            break
        page += 1
        time.sleep(0.1)  # Pequeño sleep para evitar rate limit

    # Unimos antiguos y nuevos
    todas = list(existentes_csv.values()) + nuevos
    tmp_path = csv_path.with_suffix(".tmp")
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "codigo_bdns", "fecha_recepcion", "tipo_administracion",
                "status", "last_error", "last_attempt", "retries"
            ]
        )
        writer.writeheader()
        for row in todas:
            writer.writerow(row)
    tmp_path.replace(csv_path)

    logger.info(f"[{tipo}] Total {len(todas)} en {csv_path}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extrae lista de códigos BDNS de convocatorias de un año y crea CSV de control.')
    parser.add_argument('--year', '-y', type=int, required=True, help='Año')
    args = parser.parse_args()
    control_dir = Path(__file__).resolve().parent.parent / "control"
    control_dir.mkdir(parents=True, exist_ok=True)
    csv_path = control_dir / f"convocatoria_{args.year}.csv"

    for tipo in TIPOS:
        fetch_codigos_bdns(args.year, tipo, csv_path)

if __name__ == "__main__":
    main()

