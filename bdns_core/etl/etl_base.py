# bdns_core/etl/etl_base.py
"""
Clases base para el sistema ETL de BDNS.

Define:
- ExitCode: Codigos de salida estandar (SUCCESS, WARNING, ERROR)
- ETLResult: Resultado de ejecucion de un script ETL
- etl_exit: Funcion para salida estandar con reporte
- IncidenciasWriter: Escritor de CSV de incidencias
- setup_etl_logging: Configuracion de logging

Adaptado de sipi_api/ETL/common/etl_base.py
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import sys
import json
import logging


class ExitCode(IntEnum):
    """Codigos de salida estandar para scripts ETL"""
    SUCCESS = 0   # Todo procesado correctamente
    WARNING = 1   # Completado con incidencias (hay CSV de incidencias)
    ERROR = 2     # Fallo critico, proceso abortado


@dataclass
class ETLResult:
    """Resultado de ejecucion de un script ETL"""
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
        """Duracion en segundos"""
        if self.ended_at and self.started_at:
            return (self.ended_at - self.started_at).total_seconds()
        return 0.0

    @property
    def success_rate(self) -> float:
        """Tasa de exito (0.0 - 1.0)"""
        if self.records_processed == 0:
            return 1.0
        return self.records_success / self.records_processed

    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario para serializacion"""
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
    Salida estandar para scripts ETL.

    Imprime resumen de ejecucion y sale con el codigo apropiado.

    Args:
        result: Resultado de la ejecucion ETL
        logger: Logger opcional para registrar el resultado
    """
    # Asegurar que ended_at esta establecido
    if result.ended_at is None:
        result.ended_at = datetime.now()

    # Construir mensaje de resumen
    separator = "=" * 60
    summary = f"""
{separator}
ETL RESULTADO: {result.status.name}
{separator}
Procesados:  {result.records_processed:,}
Exitosos:    {result.records_success:,}
Fallidos:    {result.records_failed:,}
Tasa exito:  {result.success_rate:.2%}
Duracion:    {result.duration_seconds:.1f}s
"""

    if result.incidencias_file:
        summary += f"Incidencias: {result.incidencias_file}\n"

    if result.message:
        summary += f"Mensaje:     {result.message}\n"

    if result.stats:
        summary += "\nEstadisticas adicionales:\n"
        for key, value in result.stats.items():
            summary += f"  {key}: {value}\n"

    summary += separator

    # Imprimir a consola
    print(summary)

    # Log si hay logger
    if logger:
        log_level = {
            ExitCode.SUCCESS: logging.INFO,
            ExitCode.WARNING: logging.WARNING,
            ExitCode.ERROR: logging.ERROR,
        }.get(result.status, logging.INFO)

        logger.log(log_level, f"ETL completado: {result.status.name}")
        logger.log(log_level, f"Procesados: {result.records_processed}, Exitosos: {result.records_success}, Fallidos: {result.records_failed}")
        if result.incidencias_file:
            logger.warning(f"Archivo de incidencias: {result.incidencias_file}")

    # Salir con codigo apropiado
    sys.exit(result.status.value)


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
        import csv
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._file = open(self.filepath, 'w', newline='', encoding='utf-8-sig')
        self._writer = csv.writer(self._file)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
        # Si no se escribieron incidencias, eliminar el archivo vacio
        if self._count == 0 and self.filepath.exists():
            self.filepath.unlink()

    def write(self, original_row: Dict[str, Any], motivo: str, valores_intentados: Dict[str, Any] = None):
        """
        Escribe una incidencia al CSV.

        Args:
            original_row: Diccionario con los campos originales del registro
            motivo: Descripcion del motivo del fallo
            valores_intentados: Valores que se intentaron usar para la resolucion
        """
        if not self._headers_written:
            # Escribir cabeceras
            headers = list(original_row.keys()) + ["_motivo_fallo", "_valores_intentados", "_timestamp"]
            self._writer.writerow(headers)
            self._headers_written = True

        # Escribir fila
        valores_str = json.dumps(valores_intentados, ensure_ascii=False) if valores_intentados else ""
        row = list(original_row.values()) + [motivo, valores_str, datetime.now().isoformat()]
        self._writer.writerow(row)
        self._count += 1

    @property
    def count(self) -> int:
        """Numero de incidencias escritas"""
        return self._count

    @property
    def has_incidencias(self) -> bool:
        """Indica si hay incidencias"""
        return self._count > 0

    def get_filepath(self) -> Optional[str]:
        """Retorna la ruta del archivo si hay incidencias, None si no"""
        return str(self.filepath) if self._count > 0 else None


def setup_etl_logging(name: str, log_dir: Path, level: int = logging.INFO) -> logging.Logger:
    """
    Configura logging estandar para un script ETL.

    Args:
        name: Nombre del logger (ej: "load_concesiones")
        log_dir: Directorio donde guardar los logs
        level: Nivel de logging

    Returns:
        Logger configurado
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{name}_{timestamp}.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Limpiar handlers existentes
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
