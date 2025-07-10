
# load_convocatoria_details.py
# Carga detalles de convocatorias pendientes desde el CSV, ejecuta load_conocatorias_from_json y actualiza el CSV de control después de cada fila

# load_convocatoria_details.py
# Carga detalles de convocatorias pendientes desde el CSV,
# ejecuta load_conocatorias_from_json y actualiza el CSV de control al final.

import requests
import csv
import argparse
from datetime import datetime
from pathlib import Path
import time
import random

from .load_convocatoria import load_conocatorias_from_json, IncompleteCatalog

def main():
    parser = argparse.ArgumentParser(description='Carga detalles y actualiza el estado de cada convocatoria en la base de datos.')
    parser.add_argument('--year', '-y', type=int, required=True, help='Año')
    args = parser.parse_args()
    year = args.year

    csv_path = Path("ETL/control") / f"convocatoria_{year}.csv"
    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    processed = 0
    start_time = time.time()

    print(f"[{datetime.now():%H:%M:%S}] Iniciando carga de detalles para {total} convocatorias del ejercicio {year}")

    for i, row in enumerate(rows, 1):
        if row["status"] == "OK" or row["status"] == "done":
            continue
        convocatoria_id = row["codigoBDNS"]
        url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatorias?numConv={convocatoria_id}"
        row["last_attempt"] = datetime.now().isoformat(timespec="seconds")
        row["retries"] = str(int(row.get("retries", "0")) + 1)
        try:
            time.sleep(random.uniform(0.1, 0.5))
            r = requests.get(url, timeout=60)
            if r.status_code != 200:
                row["status"] = "error"
                row["last_error"] = f"HTTP {r.status_code}"
                print(f"[ERROR] ({i}/{total}) Convocatoria {convocatoria_id}: HTTP {r.status_code}")
                continue
            detail = r.json()
            load_conocatorias_from_json(detail)
            row["status"] = "done"
            row["last_error"] = ""
            processed += 1
            print(f"[OK] ({i}/{total}) Convocatoria {convocatoria_id} procesada y marcada como done.")
        except IncompleteCatalog as ce:
            row["status"] = "error"
            row["last_error"] = f"Falta catálogo: {ce.catalog}, '{ce.description}'"
            print(f"[CATÁLOGO INCOMPLETO] ({i}/{total}) Convocatoria {convocatoria_id}: {ce.catalog} '{ce.description}'")
        except Exception as e:
            row["status"] = "error"
            row["last_error"] = str(e)
            print(f"[ERROR] ({i}/{total}) Convocatoria {convocatoria_id}: {e}")

    # Al final, graba el CSV de control, siempre con la cabecera 'codigoBDNS'
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["codigoBDNS", "status", "last_error", "last_attempt", "retries"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    elapsed = time.time() - start_time
    print(f"[{datetime.now():%H:%M:%S}] Proceso terminado. Convocatorias con detalle procesado: {processed} / {total} en {elapsed:.1f} s.")

if __name__ == "__main__":
    main()
