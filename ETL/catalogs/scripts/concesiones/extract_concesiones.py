# Script para descargar todas las concesiones de un año
# desde la API BDNS y generar un CSV preparado para poblamiento de los modelo Concesiones y Beneficiario.

import requests
import csv
import argparse
from pathlib import Path

PAGE_SIZE = 10000
URL = "https://www.infosubvenciones.es/bdnstrans/api/concesiones/busqueda"

def limpiar_campo(txt):
    return txt.replace('\ufeff', '').strip() if txt else ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, help="Año de las concesiones a descargar (ej: 2024)")
    args = parser.parse_args()
    year = args.year

    desde = f"01/01/{year}"
    hasta = f"31/12/{year}"

    output_dir = Path("ETL/control")
    output_dir.mkdir(parents=True, exist_ok=True)
    salida = output_dir / f"concesiones_{year}.csv"

    # Campos típicos de la respuesta (revísalo en una muestra, amplía si hace falta)
    cabecera = [
        "id", "idConvocatoria", "numeroConvocatoria", "idPersona", "beneficiario", 
        "importe", "instrumento", "ayudaEquivalente", "fechaConcesion", "urlBR", "tieneProyecto"
    ]

    page = 0
    total = 0
    with open(salida, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=cabecera)
        writer.writeheader()
        while True:
            params = {
                "page": page,
                "pageSize": PAGE_SIZE,
                "fechaDesde": desde,
                "fechaHasta": hasta,
            }
            r = requests.get(URL, params=params, timeout=180)
            r.raise_for_status()
            data = r.json()
            content = data.get("content", [])
            batch = 0

            for row in content:
                writer.writerow({
                    "id": row.get("id"),
                    "idConvocatoria": row.get("idConvocatoria"),
                    "numeroConvocatoria": row.get("numeroConvocatoria"),
                    "idPersona": row.get("idPersona"),
                    "beneficiario": limpiar_campo(row.get("beneficiario")),
                    "importe": row.get("importe"),
                    "instrumento": row.get("instrumento"),
                    "ayudaEquivalente": row.get("ayudaEquivalente"),
                    "fechaConcesion": row.get("fechaConcesion"),
                    "urlBR": row.get("urlBR"),
                    "tieneProyecto": row.get("tieneProyecto"),
                })
                batch += 1
            total += batch
            print(f"[INFO] Página {page}: {batch} registros")
            if batch < PAGE_SIZE:
                break
            page += 1
    print(f"[INFO] CSV extraído a {salida} con {total} filas.")

if __name__ == "__main__":
    main()
