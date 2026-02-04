# ETL/convocatorias/transform/transform_convocatorias.py

from ETL.etl_utils import get_or_create_dir
from ETL.convocatorias.load.load_convocatorias_from_json import load_convocatorias_from_json

RUTA_TRANS = get_or_create_dir("json", "convocatorias", "transformed")

def main(year: int, mes: int, tipo: str):
    path = RUTA_TRANS / f"convocatorias_{tipo}_{year}_{mes:02d}.json"
    if not path.exists():
        return
    load_convocatorias_from_json(path)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--mes", type=int, required=True)
    p.add_argument("--tipo", type=str, required=True)
    args = p.parse_args()
    main(args.year, args.mes, args.tipo)
