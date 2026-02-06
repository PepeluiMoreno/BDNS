import strawberry
from typing import Optional, List
from datetime import datetime
from enum import Enum

@strawberry.enum
class ETLStatus(str, Enum):
    """Estado de ejecución del ETL."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

@strawberry.type
class ETLStepResult:
    """Resultado de un paso del ETL."""
    name: str
    status: str  # SUCCESS, FAILED, SKIPPED
    duration: float
    message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

@strawberry.type
class ETLExecution:
    """Información de una ejecución del ETL."""
    id: strawberry.ID
    year: int
    month: Optional[int] = None
    status: ETLStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    total_duration: Optional[float] = None
    steps_executed: int
    steps_total: int
    steps_failed: int
    error_message: Optional[str] = None
    created_by: Optional[str] = None
    steps_results: List[ETLStepResult]

@strawberry.type
class ETLStep:
    """Información de un paso del ETL."""
    name: str
    module: str
    description: str
    enabled: bool
    depends_on: List[str]

@strawberry.type
class ETLConfig:
    """Configuración disponible del ETL."""
    available_steps: List[ETLStep]
    min_year: int
    max_year: int
    current_year: int