# ETL_catalogos.py
# Poblamiento de órganos convocantes y catálogos principales de la BDNS
# requisito impresindible para la integridad referencial de la base de datos.

# Asegúrate de que los módulos necesarios están en el PYTHONPATH o en el mismo directorio

import sys
import logging
from datetime import datetime
from pathlib import Path

# Añade la carpeta "app" al sys.path
app_dir = Path(__file__).resolve().parent.parent / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
etl_dir = Path(__file__).resolve().parent
if str(etl_dir) not in sys.path:
    sys.path.insert(0, str(etl_dir))
    
# Crear la carpeta logs/ si no existe
logs_dir = Path("logs")
logs_dir.mkdir(parents=True, exist_ok=True)

# Archivo de log rotativo con fecha/hora
log_filename = f"logs/ETL_inicializacion_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()  # Si quieres seguir viendo los logs en consola
    ]
)
logger = logging.getLogger("ETL_inicializacion")


from app.db.session import SessionLocal, engine
from app.db.models import (
    Base, Organo, Instrumento, Reglamento, SectorActividad, SectorProducto,
    TipoBeneficiario, Finalidad, Fondo, Objetivo, Region
)
from app.db.enums import TipoOrganoEnum


# --- Importa las funciones de los módulos de ETL---
from load_organos import load_organos  
from load_catalogos import (
    load_catalogo,
    load_regiones,
    load_sector_actividad_desde_csv,
    load_fondo_desde_csv,
    load_reglamento_desde_csv,
)
# ---

# Definición de constantes

VPD = "GE"  # Valor por defecto para el campo VPD en algunos catálogos

CATALOGOS_ETL = [
    # Cada entrada: función y sus argumentos (session va siempre primero)
    {"func": load_sector_actividad_desde_csv, "args": ["data/populate/estructura_cnae2009.csv"]},
    {"func": load_fondo_desde_csv, "args": ["data/populate/fondos_europeos.csv"]},
    {"func": load_reglamento_desde_csv, "args": ["data/populate/reglamentos.csv"]},
    {"func": load_regiones, "args": []},
    {"func": load_catalogo, "args": [Instrumento, "instrumentos"]},
    {"func": load_catalogo, "args": [TipoBeneficiario, "beneficiarios", {"vpd": VPD}]},
    {"func": load_catalogo, "args": [SectorProducto, "sectores"]},
    {"func": load_catalogo, "args": [Finalidad, "finalidades", {"vpd": VPD}]},
    {"func": load_catalogo, "args": [Objetivo, "objetivos"]},
    {"func": load_catalogo, "args": [Reglamento, "reglamentos"]},   
]

def main():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        logger.info("Poblando órganos...")
        load_organos(session)
        logger.info("Órganos listos.")
        for entry in CATALOGOS_ETL:
            f = entry["func"]
            args = entry["args"]
            logger.info(f"Poblando catálogo con {f.__name__}({', '.join(map(str, args))})...")
            try:
                f(session, *args)
            except Exception as e:
                logger.error(f"Error poblando catálogo {f.__name__}: {e}")
        logger.info("Inicialización completa.")

if __name__ == "__main__":
    main()
