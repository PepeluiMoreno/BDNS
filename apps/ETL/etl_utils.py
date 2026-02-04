# ETL/etl_utils.py
"""
Utilidades ETL para BDNS.

Incluye:
- Gestión de rutas y carpetas
- Clases base para resultados ETL
- Escritor de incidencias
- Configuración de logging
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import sys
import json
import csv
import logging


# ============================================================
# CONSTANTES
# ============================================================

ETL_USERNAME = "etl_system"


def get_etl_user_name() -> str:
    """Retorna el nombre del usuario ETL del sistema."""
    return ETL_USERNAME


# ============================================================
# UTILIDADES DE RUTAS
# ============================================================

def get_or_create_dir(*parts):
    """
    Devuelve un Path absoluto asegurando que la ruta existe.
    Ejemplo: get_or_create_dir("json", "convocatorias")
    """
    base_dir = Path(__file__).resolve().parent  # ETL/
    full_path = base_dir.joinpath(*parts)
    full_path.mkdir(parents=True, exist_ok=True)
    return full_path


def registrar_pendiente(modelo: str, valor: str):
    """
    Añade un valor pendiente al CSV del catálogo/modelo correspondiente.
    Solo lo añade si no está ya registrado.

    Args:
        modelo: nombre del catálogo (ej: 'organo', 'finalidad')
        valor: la descripción o clave no resuelta
    """
    pending_dir = get_or_create_dir("pending")
    archivo = pending_dir / f"{modelo}.csv"
    existe = archivo.exists()

    existentes = set()
    if existe:
        with open(archivo, encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Cabecera
            for row in reader:
                if row:
                    existentes.add(row[0])

    if valor in existentes:
        return

    with open(archivo, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not existe:
            writer.writerow(["valor"])
        writer.writerow([valor])


# ============================================================
# CÓDIGOS DE SALIDA Y RESULTADOS
# ============================================================

class ExitCode(IntEnum):
    """Códigos de salida estándar para scripts ETL"""
    SUCCESS = 0   # Todo procesado correctamente
    WARNING = 1   # Completado con incidencias
    ERROR = 2     # Fallo crítico, proceso abortado


@dataclass
class ETLResult:
    """Resultado de ejecución de un script ETL"""
    status: ExitCode
    records_processed: int = 0
    records_success: int = 0
    records_failed: int = 0
    incidencias_file: Optional[str] = None
    message: str = ""
    started_at: datetime = field(default_factory=datetime.now)
    ended_at: Optional[datetime] = None
    stats: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.ended_at is None:
            self.ended_at = datetime.now()

    @property
    def duration_seconds(self) -> float:
        """Duración en segundos"""
        if self.ended_at and self.started_at:
            return (self.ended_at - self.started_at).total_seconds()
        return 0.0

    @property
    def success_rate(self) -> float:
        """Tasa de éxito (0.0 - 1.0)"""
        if self.records_processed == 0:
            return 1.0
        return self.records_success / self.records_processed

    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario para serialización"""
        return {
            "status": self.status.name,
            "status_code": self.status.value,
            "records_processed": self.records_processed,
            "records_success": self.records_success,
            "records_failed": self.records_failed,
            "success_rate": f"{self.success_rate:.2%}",
            "incidencias_file": self.incidencias_file,
            "message": self.message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "stats": self.stats,
        }

    def to_json(self) -> str:
        """Serializa a JSON"""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


def etl_exit(result: ETLResult, logger: Optional[logging.Logger] = None):
    """
    Salida estándar para scripts ETL.
    Imprime resumen de ejecución y sale con el código apropiado.
    """
    if result.ended_at is None:
        result.ended_at = datetime.now()

    separator = "=" * 60
    summary = f"""
{separator}
ETL RESULTADO: {result.status.name}
{separator}
Procesados:  {result.records_processed:,}
Exitosos:    {result.records_success:,}
Fallidos:    {result.records_failed:,}
Tasa éxito:  {result.success_rate:.2%}
Duración:    {result.duration_seconds:.1f}s
"""

    if result.incidencias_file:
        summary += f"Incidencias: {result.incidencias_file}\n"

    if result.message:
        summary += f"Mensaje:     {result.message}\n"

    if result.stats:
        summary += "\nEstadísticas adicionales:\n"
        for key, value in result.stats.items():
            summary += f"  {key}: {value}\n"

    summary += separator

    print(summary)

    if logger:
        log_level = {
            ExitCode.SUCCESS: logging.INFO,
            ExitCode.WARNING: logging.WARNING,
            ExitCode.ERROR: logging.ERROR,
        }.get(result.status, logging.INFO)
        logger.log(log_level, f"ETL completado: {result.status.name}")

    sys.exit(result.status.value)


# ============================================================
# ESCRITOR DE INCIDENCIAS
# ============================================================

class IncidenciasWriter:
    """
    Escritor de CSV de incidencias.

    Uso:
        with IncidenciasWriter("concesiones", output_dir) as writer:
            writer.write(row_data, "Beneficiario no encontrado", {"nif": "12345678A"})
    """

    def __init__(self, entity_name: str, output_dir: Path):
        self.entity_name = entity_name
        self.output_dir = Path(output_dir)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filename = f"{entity_name}_incidencias_{self.timestamp}.csv"
        self.filepath = self.output_dir / self.filename
        self._file = None
        self._writer = None
        self._headers_written = False
        self._count = 0

    def __enter__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._file = open(self.filepath, 'w', newline='', encoding='utf-8-sig')
        self._writer = csv.writer(self._file)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
        if self._count == 0 and self.filepath.exists():
            self.filepath.unlink()

    def write(self, original_row: Dict[str, Any], motivo: str, valores_intentados: Dict[str, Any] = None):
        """Escribe una incidencia al CSV."""
        if not self._headers_written:
            headers = list(original_row.keys()) + ["_motivo_fallo", "_valores_intentados", "_timestamp"]
            self._writer.writerow(headers)
            self._headers_written = True

        valores_str = json.dumps(valores_intentados, ensure_ascii=False) if valores_intentados else ""
        row = list(original_row.values()) + [motivo, valores_str, datetime.now().isoformat()]
        self._writer.writerow(row)
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def has_incidencias(self) -> bool:
        return self._count > 0

    def get_filepath(self) -> Optional[str]:
        return str(self.filepath) if self._count > 0 else None


# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================

def setup_etl_logging(name: str, log_dir: Path = None, level: int = logging.INFO) -> logging.Logger:
    """
    Configura logging estándar para un script ETL.

    Args:
        name: Nombre del logger (ej: "load_concesiones")
        log_dir: Directorio donde guardar los logs (default: ETL/logs)
        level: Nivel de logging

    Returns:
        Logger configurado
    """
    if log_dir is None:
        log_dir = get_or_create_dir("logs")
    else:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{name}_{timestamp}.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    # Handler para archivo
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)

    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_format)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ============================================================
# EXPORTACIONES
# ============================================================

__all__ = [
    # Constantes
    "ETL_USERNAME",
    "get_etl_user_name",
    # Rutas
    "get_or_create_dir",
    "registrar_pendiente",
    # Resultados
    "ExitCode",
    "ETLResult",
    "etl_exit",
    # Incidencias
    "IncidenciasWriter",
    # Logging
    "setup_etl_logging",
]
