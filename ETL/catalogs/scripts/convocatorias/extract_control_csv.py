# extract_contol_csv.py

import requests
import csv
import argparse
from pathlib import Path

def fetch_codigos_bdns(year, csv_path):
    url = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias/busqueda"
    page = 0
    page_size = 10000
    total_esperado = None

    # Lee CSV existente para evitar duplicados y mantener estados previos
    existentes_csv = set()
    estados = {}
    if csv_path.exists():
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cod = row.get('codigoBDNS')
                if cod:
                    existentes_csv.add(cod)
                    estados[cod] = [
                        row.get('status', 'pending'),
                        row.get('last_error', ''),
                        row.get('last_attempt', ''),
                        row.get('retries', '0')
                    ]

    nuevos_codigos = set()
    while True:
        params = {
            "fechaDesde": f"01/01/{year}",
            "fechaHasta": f"31/12/{year}",
            "page": page,
            "pageSize": page_size,
            "order": "numeroConvocatoria",
            "direccion": "asc",
        }
        response = requests.get(url, params=params, timeout=180)
        response.raise_for_status()
        data = response.json()
        contenido = data.get("content", [])
        print(f"Página {page}: {len(contenido)} registros")

        for item in contenido:
            codigo_bdns = str(item.get("numeroConvocatoria") or item.get("id"))
            if codigo_bdns and codigo_bdns not in existentes_csv:
                nuevos_codigos.add(codigo_bdns)

        if total_esperado is None:
            total_esperado = data.get("totalElements", 0)

        if not contenido or (len(nuevos_codigos) + len(existentes_csv)) >= total_esperado:
            break
        page += 1

    # Genera el CSV de control, conservando el estado de filas previas
    todas_codigos = existentes_csv | nuevos_codigos
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["codigoBDNS", "status", "last_error", "last_attempt", "retries"])
        for codigo in sorted(todas_codigos, key=lambda x: int(x)):
            if codigo in estados:
                writer.writerow([codigo] + estados[codigo])
            else:
                writer.writerow([codigo, "pending", "", "", "0"])

    print(f"Se extrajeron y listaron {len(nuevos_codigos)} nuevas convocatorias. Total en control: {len(todas_codigos)}.")

def main():
    parser = argparse.ArgumentParser(description='Extrae lista de códigos BDNS de convocatorias de un año y crea CSV de control.')
    parser.add_argument('--year', '-y', type=int, required=True, help='Año')
    args = parser.parse_args()
    year = args.year
    output_dir = Path("ETL/control")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"convocatoria_{year}.csv"
    fetch_codigos_bdns(year, csv_path)

if __name__ == "__main__":
    main()
