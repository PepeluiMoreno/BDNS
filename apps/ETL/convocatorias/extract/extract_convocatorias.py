# ETL/convocatorias/extract/extract_convocatorias.py


import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from ETL.etl_utils import get_or_create_dir

RUTA_RAW = get_or_create_dir("json", "convocatorias", "raw")

def fetch_convocatoria(codigo: str):
    url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatorias?numConv={codigo}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("convocatorias", [])

def main(year: int, mes: int, tipo: str, workers: int):
    # aquí normalmente sacarías los códigos desde la BD
    # placeholder mínimo:
    codigos = []  # TODO: query real

    if not codigos:
        return

    resultados = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_convocatoria, c) for c in codigos]
        for f in as_completed(futures):
            resultados.extend(f.result())

    if resultados:
        out = RUTA_RAW / f"raw_convocatorias_{tipo}_{year}_{mes:02d}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--mes", type=int, required=True)
    p.add_argument("--tipo", type=str, required=True)
    p.add_argument("--workers", type=int, default=6)
    args = p.parse_args()
    main(args.year, args.mes, args.tipo, args.workers)
