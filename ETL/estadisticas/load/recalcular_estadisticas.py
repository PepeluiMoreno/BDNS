# ETL/estadisticas/load/recalcular_estadisticas.py
"""
Recalcula estadisticas de beneficiarios desde cero.

Este script se usa para:
1. Carga inicial de estadisticas (despues de cargar concesiones sin triggers)
2. Reconstruccion de estadisticas si hay inconsistencias
3. Verificacion de que los triggers funcionan correctamente

El proceso:
1. Trunca la tabla beneficiario_estadisticas_anuales
2. Recalcula agregados desde la tabla concesion
3. Inserta los resultados

NOTA: Los triggers PostgreSQL mantienen las estadisticas actualizadas
automaticamente, este script solo se necesita para inicializacion.
"""

import logging
import argparse
from datetime import datetime
from sqlalchemy import text
from app.db.session import get_session
from ETL.etl_utils import get_or_create_dir

MODULO = "recalcular_estadisticas"

RUTA_LOGS = get_or_create_dir("logs")

logging.basicConfig(
    filename=RUTA_LOGS / f"{datetime.now():%Y%m%d_%H%M%S}_{MODULO}.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(module)s] %(message)s"
)


def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{MODULO}] {msg}")
    getattr(logging, level.lower(), logging.info)(f"[{MODULO}] {msg}")


SQL_RECALCULAR = """
-- Recalcula estadisticas agregadas por beneficiario/ejercicio/organo
INSERT INTO beneficiario_estadisticas_anuales (
    beneficiario_id,
    ejercicio,
    organo_id,
    num_concesiones,
    importe_total,
    importe_medio,
    fecha_primera_concesion,
    fecha_ultima_concesion,
    created_at
)
SELECT
    c.id_beneficiario,
    EXTRACT(YEAR FROM c.fecha_concesion)::INTEGER as ejercicio,
    conv.organo_id,
    COUNT(*) as num_concesiones,
    COALESCE(SUM(c.importe), 0) as importe_total,
    COALESCE(AVG(c.importe), 0) as importe_medio,
    MIN(c.fecha_concesion) as fecha_primera_concesion,
    MAX(c.fecha_concesion) as fecha_ultima_concesion,
    NOW() as created_at
FROM concesion c
JOIN convocatoria conv ON c.codigo_bdns = conv.id
WHERE c.fecha_concesion IS NOT NULL
  AND conv.organo_id IS NOT NULL
GROUP BY c.id_beneficiario, EXTRACT(YEAR FROM c.fecha_concesion), conv.organo_id
ON CONFLICT (beneficiario_id, ejercicio, organo_id)
DO UPDATE SET
    num_concesiones = EXCLUDED.num_concesiones,
    importe_total = EXCLUDED.importe_total,
    importe_medio = EXCLUDED.importe_medio,
    fecha_primera_concesion = EXCLUDED.fecha_primera_concesion,
    fecha_ultima_concesion = EXCLUDED.fecha_ultima_concesion,
    updated_at = NOW();
"""

SQL_TRUNCATE = "TRUNCATE TABLE beneficiario_estadisticas_anuales;"

SQL_COUNT = "SELECT COUNT(*) FROM beneficiario_estadisticas_anuales;"

SQL_STATS = """
SELECT
    ejercicio,
    COUNT(*) as num_registros,
    SUM(num_concesiones) as total_concesiones,
    SUM(importe_total) as importe_total
FROM beneficiario_estadisticas_anuales
GROUP BY ejercicio
ORDER BY ejercicio DESC;
"""


def recalcular(truncate_first: bool = False, year: int = None):
    """
    Recalcula las estadisticas.

    Args:
        truncate_first: Si True, trunca la tabla antes de recalcular
        year: Si se especifica, solo recalcula para ese ejercicio
    """
    with get_session() as session:
        if truncate_first:
            log("Truncando tabla beneficiario_estadisticas_anuales...")
            session.execute(text(SQL_TRUNCATE))
            session.commit()

        log("Recalculando estadisticas...")

        if year:
            # Modificar SQL para filtrar por año
            sql = SQL_RECALCULAR.replace(
                "WHERE c.fecha_concesion IS NOT NULL",
                f"WHERE c.fecha_concesion IS NOT NULL AND EXTRACT(YEAR FROM c.fecha_concesion) = {year}"
            )
        else:
            sql = SQL_RECALCULAR

        result = session.execute(text(sql))
        session.commit()

        # Contar registros insertados
        count = session.execute(text(SQL_COUNT)).scalar()
        log(f"Total registros en estadisticas: {count}")

        # Mostrar resumen por ejercicio
        log("Resumen por ejercicio:")
        rows = session.execute(text(SQL_STATS)).fetchall()
        for row in rows:
            ejercicio, num_registros, total_concesiones, importe_total = row
            log(f"  {ejercicio}: {num_registros} registros, {total_concesiones} concesiones, {importe_total:,.2f} EUR")

    return count


def verificar_consistencia():
    """Verifica que las estadisticas coincidan con los datos de concesiones."""
    log("Verificando consistencia...")

    sql_check = """
    WITH calculated AS (
        SELECT
            c.id_beneficiario,
            EXTRACT(YEAR FROM c.fecha_concesion)::INTEGER as ejercicio,
            conv.organo_id,
            COUNT(*) as num_concesiones,
            COALESCE(SUM(c.importe), 0) as importe_total
        FROM concesion c
        JOIN convocatoria conv ON c.codigo_bdns = conv.id
        WHERE c.fecha_concesion IS NOT NULL
          AND conv.organo_id IS NOT NULL
        GROUP BY c.id_beneficiario, EXTRACT(YEAR FROM c.fecha_concesion), conv.organo_id
    )
    SELECT
        COUNT(*) as total_diff
    FROM calculated c
    LEFT JOIN beneficiario_estadisticas_anuales e
        ON c.id_beneficiario = e.beneficiario_id
        AND c.ejercicio = e.ejercicio
        AND c.organo_id = e.organo_id
    WHERE e.id IS NULL
       OR e.num_concesiones != c.num_concesiones
       OR ABS(e.importe_total - c.importe_total) > 0.01;
    """

    with get_session() as session:
        diff = session.execute(text(sql_check)).scalar()

        if diff == 0:
            log("OK: Estadisticas consistentes con datos de concesiones")
            return True
        else:
            log(f"WARN: {diff} registros inconsistentes", "WARNING")
            return False


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Recalcula estadisticas de beneficiarios.")
    parser.add_argument("--truncate", action="store_true", help="Truncar tabla antes de recalcular")
    parser.add_argument("--year", type=int, help="Solo recalcular para un ejercicio especifico")
    parser.add_argument("--verify", action="store_true", help="Solo verificar consistencia, no recalcular")
    args = parser.parse_args()

    if args.verify:
        ok = verificar_consistencia()
        return 0 if ok else 1

    log("Iniciando recalculo de estadisticas...")
    count = recalcular(truncate_first=args.truncate, year=args.year)
    log(f"Proceso completado: {count} registros de estadisticas")

    # Verificar
    verificar_consistencia()

    return 0


if __name__ == "__main__":
    exit(main())
