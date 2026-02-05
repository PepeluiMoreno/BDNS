# load_minimis.py
# Pobla la tabla Minimi a partir del CSV generado por extract_minimis_csv.py
# Inserta los valores de catálogos sobre la marcha si no existen.

import csv
from pathlib import Path
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from bdns_core.db.models import Minimi, Instrumento, Reglamento, SectorActividad, SectorProducto
from bdns_core.db.session import SessionLocal

CSV_PATH = Path("ETL/control/minimis_2024.csv")  # Modifica el año si es necesario

def parse_fecha(f):
    if not f:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(f, fmt).date()
        except Exception:
            continue
    return None

def get_id(session, Modelo, descripcion):
    if not descripcion:
        return None
    obj = session.query(Modelo).filter_by(descripcion=descripcion).first()
    if obj:
        return obj.id
    nuevo = Modelo(descripcion=descripcion)
    session.add(nuevo)
    session.commit()
    return nuevo.id

def load_minimis():
    with SessionLocal() as session, open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        registros = []
        for row in reader:
            registros.append({
                "id": row["idConcesion"],
                "fecha_concesion": parse_fecha(row["fechaConcesion"]),
                "fecha_registro": parse_fecha(row["fechaRegistro"]),
                "ayuda_equivalente": float(row["ayudaEquivalente"]) if row["ayudaEquivalente"] else None,
                "concesion_id": row["idConcesion"],  # Si tu tabla Concesion usa ese mismo id
                "codigo_bdns": int(row["numeroConvocatoria"]) if row["numeroConvocatoria"] else None,
                "id_beneficiario": int(row["idPersona"]) if row["idPersona"] else None,
                "id_instrumento": get_id(session, Instrumento, row["instrumento"]),
                "id_reglamento": get_id(session, Reglamento, row["reglamento"]),
                "id_sector_actividad": get_id(session, SectorActividad, row["sectorActividad"]),
                "id_sector_producto": get_id(session, SectorProducto, row["sectorProducto"]),
            })

        session.execute(
            insert(Minimi)
            .values(registros)
            .on_conflict_do_nothing(index_elements=['id'])
        )
        session.commit()
        print(f"Insertados {len(registros)} minimis en la base de datos.")

if __name__ == "__main__":
    load_minimis()
