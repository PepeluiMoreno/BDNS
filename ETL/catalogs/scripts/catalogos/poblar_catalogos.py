# load_catalogos.py

import requests
import logging
import csv
from app.db.session import SessionLocal, engine
from app.db.utils import normalizar
from app.db.models import (
    Base,
    Finalidad,
    Fondo,
    Instrumento,
    Objetivo,
    Region,
    Reglamento,
    SectorActividad,
    SectorProducto,
    TipoBeneficiario,
)
import sys
from sqlalchemy.dialects.postgresql import insert as pg_insert



from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configurar logging sin prefijo de módulo
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s"
)
logger = logging.getLogger()

API_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
VPD = "GE"

def load_catalogo(session, Model, endpoint, extra_params=None):
    try:
        url = f"{API_BASE}/{endpoint}"
        if extra_params:
            url += "?" + "&".join([f"{k}={v}" for k, v in extra_params.items()])
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            obj = Model(
                id=item["id"],
                descripcion=item["descripcion"],
                descripcion_norm=normalizar(item["descripcion"]),
            )
            session.merge(obj)
        session.commit()
        logger.info(f"{Model.__name__} insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar {Model.__name__}: {e}")

def load_regiones(session):
    def insertar_region(item, id_padre=None):
        region = Region(
            id=item["id"],
            descripcion=item["descripcion"],
            descripcion_norm=normalizar(item["descripcion"]),
            id_padre=id_padre,
        )
        session.merge(region)
        for hijo in item.get("children", []):
            insertar_region(hijo, id_padre=item["id"])

    try:
        url = f"{API_BASE}/regiones"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        for item in data:
            insertar_region(item)
        session.commit()
        logger.info("Regiones insertadas/actualizadas.")
    except Exception as e:
        logger.exception("Error al poblar regiones: %s", e)

def load_sector_actividad_desde_csv(session, ruta_csv):
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=":")
            items = list(reader)

        nodos = {}

        for row in items:
            id = row["CODINTEGR"].strip()
            descripcion = row["TITULO_CNAE2009"].strip()
            norm = normalizar(descripcion)

            if id not in nodos:
                nodos[id] = SectorActividad(
                    id=id,
                    descripcion=descripcion,
                    descripcion_norm=norm
                )

        for id, sector in nodos.items():
            if len(id) == 1:
                continue  # Sección (raíz)
            elif len(id) == 3:
                sector.id_padre = id[0]
            elif len(id) == 4:
                sector.id_padre = id[:3]
            elif len(id) == 5:
                sector.id_padre = id[:4]

        for id, sector in sorted(nodos.items(), key=lambda x: len(x[0])):
            session.merge(sector)

        session.commit()
        logger.info("Sectores de actividad insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar sectores de actividad desde CSV: {e}")

def load_fondo_desde_csv(session, ruta_csv):
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            items = list(reader)

        for row in items:
            id = row["id"].strip()
            descripcion = row["descripcion"].strip()
            descripcion_norm = normalizar(descripcion)
            fondo = Fondo(
                id=id,
                descripcion=descripcion,
                descripcion_norm=descripcion_norm
            )
            session.merge(fondo)
        session.commit()
        logger.info("Fondos insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar fondos desde CSV: {e}")

def load_reglamento(session):
    AMBITOS = {
        'C': 'Concesiones',
        'A': 'Ayudas de Estado',
        'M': 'de Minimis',
    }
    vistos = set()
    try:
        for ambito, nombre in AMBITOS.items():
            url = f"{API_BASE}/reglamentos?ambito={ambito}"
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            for item in data:
                if item["id"] in vistos:
                    continue
                vistos.add(item["id"])
                stmt = pg_insert(Reglamento).values(
                    id=item["id"],
                    descripcion=item["descripcion"],
                    descripcion_norm=normalizar(item["descripcion"]),
                    ambito=ambito
                ).on_conflict_do_nothing(index_elements=['id'])
                session.execute(stmt)
            logger.info(f"Reglamentos ({nombre}) insertados (sin duplicados).")
        session.commit()
    except Exception as e:
        logger.exception(f"Error al poblar Reglamentos: {e}")

def load_reglamento_desde_csv(session, ruta_csv):
    load_reglamento(session)
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                stmt = pg_insert(Reglamento).values(
                    id=row["id"].strip(),
                    descripcion=row["descripcion"].strip(),
                    descripcion_norm=normalizar(row["descripcion"].strip()),
                    ambito=row["ambito"].strip()
                ).on_conflict_do_nothing(index_elements=['id'])
                session.execute(stmt)
        session.commit()
        logger.info("Reglamentos insertados desde CSV (sin duplicados).")
    except Exception as e:
        logger.exception(f"Error al poblar reglamentos desde CSV: {e}")

        
def main():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        load_sector_actividad_desde_csv(session, "data/populate/estructura_cnae2009.csv")
        load_fondo_desde_csv(session, "data/populate/fondos_europeos.csv")
        load_reglamento_desde_csv(session, "data/populate/reglamentos.csv")
        load_catalogo(session, Instrumento, "instrumentos")
        load_catalogo(session, TipoBeneficiario, "beneficiarios", {"vpd": VPD})
        load_catalogo(session, SectorProducto, "sectores")
        load_regiones(session),
        load_catalogo(session, Finalidad, "finalidades", {"vpd": VPD})
        load_catalogo(session, Objetivo, "objetivos")
        load_catalogo(session, Reglamento, "reglamentos")

if __name__ == "__main__":
    main()
