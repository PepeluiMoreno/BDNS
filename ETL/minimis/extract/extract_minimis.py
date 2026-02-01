# extract_minimis.py
# Script para descargar todas las concesiones de minimis de un año
# desde la API BDNS y generar un CSV preparado para poblamiento de los modelo Minimi y Beneficiario.


import requests
import csv
import argparse
from pathlib import Path

PAGE_SIZE = 10000
URL = "https://www.infosubvenciones.es/bdnstrans/api/minimis/busqueda"

def limpiar_campo(txt):
    return txt.replace('\ufeff', '').strip() if txt else ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, help="Año de los minimis a descargar (ej: 2024)")
    args = parser.parse_args()
    year = args.year

    desde = f"01/01/{year}"
    hasta = f"31/12/{year}"

    output_dir = Path("ETL/control")
    output_dir.mkdir(parents=True, exist_ok=True)
    salida = output_dir / f"minimis_{year}.csv"

    cabecera = [
        "idConcesion", "numeroConvocatoria", "idPersona", "ayudaEquivalente", "fechaConcesion", "fechaRegistro",
        "instrumento", "reglamento", "sectorActividad", "sectorProducto", "beneficiario"
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
                    "idConcesion": row.get("idConcesion"),
                    "numeroConvocatoria": limpiar_campo(row.get("numeroConvocatoria")),
                    "idPersona": row.get("idPersona"),
                    "ayudaEquivalente": row.get("ayudaEquivalente"),
                    "fechaConcesion": row.get("fechaConcesion"),
                    "fechaRegistro": row.get("fechaRegistro"),
                    "instrumento": limpiar_campo(row.get("instrumento")),
                    "reglamento": limpiar_campo(row.get("reglamento")),
                    "sectorActividad": limpiar_campo(row.get("sectorActividad")),
                    "sectorProducto": limpiar_campo(row.get("sectorProducto")),
                    "beneficiario": limpiar_campo(row.get("beneficiario")),
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