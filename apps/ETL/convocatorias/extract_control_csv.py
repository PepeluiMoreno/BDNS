# extract_control_csv.py
# Genera o actualiza el CSV de control de convocatorias por ejercicio y tipo. Unifica en uno anual y borra los mensuales.

import requests
import csv
from pathlib import Path
from datetime import datetime

TIPOS = ["C", "A", "L", "O"]

def log(msg, level="INFO", modulo="extract_control_csv"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{modulo}] [{level}] {msg}")

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
        response = requests.get(url, params=params, timeout=180)
        response.raise_for_status()
        data = response.json()
        contenido = data.get("content", [])
        log(f"[{tipo}] Página {page}: {len(contenido)} registros")

        for item in contenido:
            codigo_bdns = str(item.get("numeroConvocatoria") or item.get("id"))
            fecha_recepcion = item.get("fechaRecepcion", "")[:10]
            if codigo_bdns and codigo_bdns not in existentes_csv:
                nuevos.append({
                    "codigo_bdns": codigo_bdns,
                    "fecha_recepcion": fecha_recepcion,
                    "tipo_administracion": tipo,
                    "status": "pending",
                    "last_error": "",
                    "last_attempt": "",
                    "retries": "0"
                })

        if total_esperado is None:
            total_esperado = data.get("totalElements", 0)

        if not contenido or (len(existentes_csv) + len(nuevos)) >= total_esperado:
            break
        page += 1

    todas = list(existentes_csv.values()) + nuevos
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
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

    log(f"[{tipo}] Total {len(todas)} en {csv_path}")

def merge_and_cleanup(year, control_dir):
    """Fusiona los archivos mensuales y borra los temporales."""
    anual_path = control_dir / f"convocatoria_{year}.csv"
    filas = {}
    # Acumula todas las filas de todos los archivos tipo
    for tipo in TIPOS:
        path = control_dir / f"convocatoria_{year}_{tipo}.csv"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cod = row.get("codigo_bdns")
                    if cod:
                        filas[cod] = row
            # Borra el archivo mensual
            path.unlink()
            log(f"Borrado temporal: {path}", "INFO")
    # Escribe el archivo anual único
    if filas:
        with open(anual_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "codigo_bdns", "fecha_recepcion", "tipo_administracion",
                    "status", "last_error", "last_attempt", "retries"
                ]
            )
            writer.writeheader()
            for row in filas.values():
                writer.writerow(row)
        log(f"CSV anual completado en {anual_path} ({len(filas)} convocatorias)", "INFO")
    else:
        log("No se encontró ningún archivo temporal para fusionar.", "WARNING")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Extrae lista de códigos BDNS de convocatorias de un año y crea CSV de control.')
    parser.add_argument('--year', '-y', type=int, required=True, help='Año')
    args = parser.parse_args()
    control_dir = Path(__file__).resolve().parent.parent / "control"
    control_dir.mkdir(parents=True, exist_ok=True)
    for tipo in TIPOS:
        csv_path = control_dir / f"convocatoria_{args.year}_{tipo}.csv"
        fetch_codigos_bdns(args.year, tipo, csv_path)
    merge_and_cleanup(args.year, control_dir)

if __name__ == "__main__":
    main()


