# load_convocatorias.py

from pathlib import Path
from ETL.convocatorias.load.load_convocatorias_from_json import load_convocatorias_from_json
from ETL.etl_utils import get_or_create_dir

RUTA_CONTROL = get_or_create_dir("control")
RUTA_TRANSFORMED = get_or_create_dir("json", "convocatorias", "transformed")

def main(year: int, mes: int, tipo: str):
    json_path = RUTA_TRANSFORMED / f"convocatorias_{tipo}_{year}_{mes:02d}.json"
    if not json_path.exists():
        return

    csv_path = RUTA_CONTROL / f"convocatoria_{year}.csv"
    load_convocatorias_from_json(json_path, csv_path)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--mes", type=int, required=True)
    p.add_argument("--tipo", type=str, required=True)
    args = p.parse_args()
    main(args.year, args.mes, args.tipo)
