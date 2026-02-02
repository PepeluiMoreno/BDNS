"""
Tipos GraphQL para el sistema de notificaciones.

Define tipos para Usuario, Suscripcion, Ejecucion y Query Builder.
"""
import strawberry
from typing import List, Optional
from datetime import datetime
from enum import Enum


@strawberry.enum
class Frecuencia(Enum):
    DIARIA = "diaria"
    SEMANAL = "semanal"
    MENSUAL = "mensual"


@strawberry.enum
class EstadoEjecucion(Enum):
    EJECUTANDO = "ejecutando"
    COMPLETADO = "completado"
    ERROR = "error"


@strawberry.type
class Usuario:
    """Usuario del sistema con vinculacion a Telegram."""
    id: strawberry.ID
    email: str
    nombre: Optional[str]
    telegram_chat_id: Optional[str]
    telegram_username: Optional[str]
    telegram_verificado: bool
    activo: bool
    created_at: datetime


@strawberry.type
class Suscripcion:
    """Suscripcion a notificaciones basada en query GraphQL."""
    id: strawberry.ID
    usuario_id: int
    nombre: str
    descripcion: Optional[str]
    graphql_query: str
    campo_id: str
    campos_comparar: Optional[List[str]]
    frecuencia: str
    hora_preferida: int
    activo: bool
    pausado_por_errores: bool
    errores_consecutivos: int
    ultimo_error: Optional[str]
    last_check: Optional[datetime]
    last_check_count: Optional[int]
    proxima_ejecucion: Optional[datetime]
    created_at: datetime


@strawberry.type
class Ejecucion:
    """Registro de ejecucion del monitor."""
    id: strawberry.ID
    subscripcion_id: int
    fecha_ejecucion: datetime
    estado: str
    registros_actuales: Optional[int]
    registros_anteriores: Optional[int]
    nuevos: int
    modificados: int
    eliminados: int
    notificacion_enviada: bool
    error: Optional[str]
    created_at: datetime


@strawberry.type
class TelegramLink:
    """Token para vincular cuenta de Telegram."""
    token: str
    instrucciones: str


# ==================== QUERY BUILDER TYPES ====================

@strawberry.type
class FilterOption:
    """Opcion para filtros tipo select."""
    value: str
    label: str


@strawberry.type
class FilterDefinition:
    """Definicion de un filtro disponible."""
    name: str
    label: str
    type: str  # text, number, date, select, checkbox
    options: Optional[List[FilterOption]]
    description: str
    ejemplo: str
    graphql_param: str


@strawberry.type
class FieldDefinition:
    """Definicion de un campo seleccionable."""
    name: str
    description: str


@strawberry.type
class QueryPreview:
    """Preview de query GraphQL generada."""
    query: str
    entity: str
    filters_active: int
    fields_selected: int
    fields: List[str]


@strawberry.type
class TestResult:
    """Resultado de test de query."""
    registros: int
    primeros_registros: List[strawberry.scalars.JSON]
    query_ejecutada: str
    duracion_ms: float


@strawberry.type
class EntidadesDisponibles:
    """Entidades disponibles para suscripcion."""
    entities: List[str]
    default: str


# ==================== INPUTS ====================

@strawberry.input
class UsuarioInput:
    """Input para crear/actualizar usuario."""
    email: str
    nombre: Optional[str] = None


@strawberry.input
class UsuarioUpdateInput:
    """Input para actualizar usuario."""
    email: Optional[str] = None
    nombre: Optional[str] = None
    activo: Optional[bool] = None


@strawberry.input
class FiltrosInput:
    """Filtros dinamicos como JSON."""
    # Los filtros se pasan como dict generico
    pass


@strawberry.input
class QueryBuilderInput:
    """Input para el query builder."""
    entity: str = "concesiones"
    filters: strawberry.scalars.JSON = strawberry.UNSET
    fields: Optional[List[str]] = None
    limite: int = 1000


@strawberry.input
class SuscripcionInput:
    """Input para crear suscripcion."""
    usuario_id: int
    nombre: str
    descripcion: Optional[str] = None
    graphql_query: str
    campo_id: str = "id"
    campos_comparar: Optional[List[str]] = None
    frecuencia: str = "semanal"
    hora_preferida: int = 8


@strawberry.input
class SuscripcionDesdeBuilderInput:
    """Input para crear suscripcion desde query builder."""
    usuario_id: int
    nombre: str
    descripcion: Optional[str] = None
    entity: str = "concesiones"
    filters: strawberry.scalars.JSON = strawberry.UNSET
    fields: Optional[List[str]] = None
    limite: int = 1000
    frecuencia: str = "semanal"
    hora_preferida: int = 8


@strawberry.input
class SuscripcionUpdateInput:
    """Input para actualizar suscripcion."""
    nombre: Optional[str] = None
    descripcion: Optional[str] = None
    graphql_query: Optional[str] = None
    campo_id: Optional[str] = None
    campos_comparar: Optional[List[str]] = None
    frecuencia: Optional[str] = None
    hora_preferida: Optional[int] = None
    activo: Optional[bool] = None
    max_errores: Optional[int] = None
