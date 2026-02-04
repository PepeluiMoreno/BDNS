
#!/usr/bin/env python3
import logging
from concurrent.futures import ProcessPoolExecutor
from app.db.session import get_session
from app.db.models.etl_job import EtlJob
from sqlalchemy import text
from datetime import datetime
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [orchestrator] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")


def bootstrap_jobs(year: int):
    TIPOS = ["C", "A", "L", "O"]
    STAGES = ["extract", "transform", "load"]

    with get_session() as s:
        for tipo in TIPOS:
            for mes in range(1, 13):
                for stage in STAGES:
                    s.execute(
                        text("""
                        INSERT INTO etl_job (entity, year, mes, tipo, stage)
                        VALUES ('convocatoria', :y, :m, :t, :s)
                        ON CONFLICT DO NOTHING
                        """),
                        {"y": year, "m": mes, "t": tipo, "s": stage},
                    )
        s.commit()


def acquire_job(stage: str):
    with get_session() as s:
        row = s.execute(
            text("""
            UPDATE etl_job
            SET status='running', started_at=now()
            WHERE id = (
                SELECT id FROM etl_job
                WHERE entity='convocatoria'
                  AND stage=:stage
                  AND status='pending'
                ORDER BY year, mes
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, year, mes, tipo
            """),
            {"stage": stage},
        ).fetchone()
        s.commit()
        return row


def finish_job(job_id: int, ok: bool, error: str | None = None):
    with get_session() as s:
        s.execute(
            text("""
            UPDATE etl_job
            SET status=:status,
                finished_at=now(),
                last_error=:err,
                retries = CASE WHEN :ok THEN retries ELSE retries + 1 END
            WHERE id=:id
            """),
            {
                "id": job_id,
                "status": "done" if ok else "error",
                "err": error,
                "ok": ok,
            },
        )
        s.commit()


def run_stage(stage: str, workers: int):
    while True:
        job = acquire_job(stage)
        if not job:
            return

        job_id, year, mes, tipo = job
        logger.info(f"[{stage.upper()}] {tipo}-{year}-{mes:02d}")

        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    f"ETL.convocatorias.{stage}.{stage}_convocatorias",
                    "--year", str(year),
                    "--mes", str(mes),
                    "--tipo", tipo,
                    "--workers", str(workers),
                ],
                cwd=str(BASE_DIR),
                check=True,
            )
            finish_job(job_id, True)
        except Exception as e:
            finish_job(job_id, False, str(e))


def main(year: int, workers: int):
    bootstrap_jobs(year)

    for stage in ("extract", "transform", "load"):
        logger.info(f"Iniciando etapa {stage}")
        run_stage(stage, workers)

    logger.info("ETL convocatorias completado")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--workers", type=int, default=6)
    args = p.parse_args()

    main(args.year, args.workers)

