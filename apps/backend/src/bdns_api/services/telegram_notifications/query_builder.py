#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Constructor de Queries GraphQL para el sistema de notificaciones.

Traduce filtros visuales (JSON) a queries GraphQL validas.
Permite al usuario ver exactamente que query se ejecutara.

Ejemplo de uso:
    from telegram_notifications.query_builder import QueryBuilder

    builder = QueryBuilder("concesiones")
    builder.add_filter("año", 2024)
    builder.add_filter("beneficiario.nif_empieza", "R")
    builder.add_filter("importe_minimo", 10000)
    builder.select_fields(["id", "importe", "fecha_concesion", "beneficiario.nif", "beneficiario.nombre"])

    query = builder.build()
    # Devuelve la query GraphQL formateada
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import json


@dataclass
class FilterDefinition:
    """Definicion de un filtro disponible."""
    name: str
    label: str
    graphql_param: str
    input_type: str  # text, number, date, select, checkbox
    options: Optional[List[Dict]] = None  # Para selects
    description: str = ""
    ejemplo: str = ""


# Filtros disponibles para cada entidad
FILTROS_CONCESIONES = [
    FilterDefinition(
        name="año",
        label="Ejercicio (Año)",
        graphql_param="año",
        input_type="number",
        description="Año del ejercicio fiscal",
        ejemplo="2024"
    ),
    FilterDefinition(
        name="codigo_bdns",
        label="Código BDNS Convocatoria",
        graphql_param="codigo_bdns",
        input_type="text",
        description="Código de la convocatoria en BDNS",
        ejemplo="123456"
    ),
    FilterDefinition(
        name="organo_id",
        label="Órgano Convocante",
        graphql_param="organo_id",
        input_type="text",  # Idealmente seria un select con busqueda
        description="ID del órgano que convoca",
        ejemplo="L01234567"
    ),
    FilterDefinition(
        name="beneficiario_id",
        label="ID Beneficiario",
        graphql_param="beneficiario_id",
        input_type="text",
        description="ID interno del beneficiario",
        ejemplo="12345"
    ),
    FilterDefinition(
        name="tipo_beneficiario",
        label="Tipo de Beneficiario",
        graphql_param="tipo_beneficiario",
        input_type="select",
        options=[
            {"value": "A", "label": "Persona física"},
            {"value": "B", "label": "Sociedad limitada"},
            {"value": "G", "label": "Asociación"},
            {"value": "R", "label": "Congregación religiosa"},
            {"value": "Q", "label": "Organismo público"},
        ],
        description="Tipo de entidad beneficiaria",
        ejemplo="R"
    ),
    FilterDefinition(
        name="fecha_desde",
        label="Fecha Desde",
        graphql_param="fecha_desde",
        input_type="date",
        description="Fecha mínima de concesión",
        ejemplo="2024-01-01"
    ),
    FilterDefinition(
        name="fecha_hasta",
        label="Fecha Hasta",
        graphql_param="fecha_hasta",
        input_type="date",
        description="Fecha máxima de concesión",
        ejemplo="2024-12-31"
    ),
    FilterDefinition(
        name="importe_minimo",
        label="Importe Mínimo (€)",
        graphql_param="importe_minimo",
        input_type="number",
        description="Importe mínimo de la subvención",
        ejemplo="10000"
    ),
    FilterDefinition(
        name="importe_maximo",
        label="Importe Máximo (€)",
        graphql_param="importe_maximo",
        input_type="number",
        description="Importe máximo de la subvención",
        ejemplo="100000"
    ),
    FilterDefinition(
        name="tipo_ayuda",
        label="Tipo de Ayuda",
        graphql_param="tipo_ayuda",
        input_type="select",
        options=[
            {"value": "S", "label": "Subvención"},
            {"value": "P", "label": "Préstamo"},
            {"value": "A", "label": "Anticipo"},
        ],
        description="Modalidad de la ayuda",
        ejemplo="S"
    ),
]

FILTROS_BENEFICIARIOS = [
    FilterDefinition(
        name="nif",
        label="NIF/CIF",
        graphql_param="nif",
        input_type="text",
        description="NIF o CIF completo del beneficiario",
        ejemplo="B12345678"
    ),
    FilterDefinition(
        name="nif_empieza",
        label="NIF empieza por",
        graphql_param="nif_empieza",
        input_type="text",
        description="Primeros caracteres del NIF (ej: R para religiosos)",
        ejemplo="R"
    ),
    FilterDefinition(
        name="nombre_contiene",
        label="Nombre contiene",
        graphql_param="nombre_contiene",
        input_type="text",
        description="Texto que debe aparecer en el nombre",
        ejemplo="FUNDACION"
    ),
    FilterDefinition(
        name="forma_juridica",
        label="Forma Jurídica",
        graphql_param="forma_juridica",
        input_type="select",
        options=[
            {"value": "A", "label": "Sociedad anónima"},
            {"value": "B", "label": "Sociedad limitada"},
            {"value": "G", "label": "Asociación"},
            {"value": "R", "label": "Congregación religiosa"},
            {"value": "Q", "label": "Organismo público"},
            {"value": "P", "label": "Partido político"},
            {"value": "V", "label": "Otros"},
        ],
        description="Tipo de entidad según el NIF",
        ejemplo="R"
    ),
]

# Campos seleccionables para cada entidad
CAMPOS_CONCESIONES = {
    "id": "ID único de la concesión",
    "codigo_bdns": "Código BDNS de la convocatoria",
    "fecha_concesion": "Fecha de la concesión",
    "importe": "Importe concedido en euros",
    "descripcion_proyecto": "Descripción del proyecto",
    "programa_presupuestario": "Programa presupuestario",
    "tipo_ayuda": "Tipo de ayuda (S/P/A)",
    "año": "Ejercicio fiscal",
    "beneficiario.id": "ID del beneficiario",
    "beneficiario.nif": "NIF del beneficiario",
    "beneficiario.nombre": "Nombre del beneficiario",
    "beneficiario.tipo": "Tipo de beneficiario",
    "organo.id": "ID del órgano convocante",
    "organo.nombre": "Nombre del órgano",
    "organo.codigo": "Código del órgano",
    "convocatoria.id": "ID de la convocatoria",
    "convocatoria.codigo_bdns": "Código BDNS",
    "convocatoria.titulo": "Título de la convocatoria",
}

CAMPOS_BENEFICIARIOS = {
    "id": "ID único del beneficiario",
    "nif": "NIF/CIF",
    "nombre": "Nombre o razón social",
    "tipo": "Tipo de entidad",
    "forma_juridica": "Forma jurídica deducida del NIF",
}


class QueryBuilder:
    """
    Constructor de queries GraphQL.

    Uso:
        builder = QueryBuilder("concesiones")
        builder.add_filter("año", 2024)
        builder.add_filter("tipo_beneficiario", "R")
        builder.select_fields(["id", "importe", "beneficiario.nif"])
        query = builder.build()
    """

    ENTITIES = {
        "concesiones": {
            "query_name": "concesiones",
            "input_type": "ConcesionInput",
            "filters": FILTROS_CONCESIONES,
            "fields": CAMPOS_CONCESIONES,
            "default_fields": ["id", "codigo_bdns", "fecha_concesion", "importe", "beneficiario.nif", "beneficiario.nombre"],
        },
        "beneficiarios": {
            "query_name": "beneficiarios",
            "input_type": "BeneficiarioInput",
            "filters": FILTROS_BENEFICIARIOS,
            "fields": CAMPOS_BENEFICIARIOS,
            "default_fields": ["id", "nif", "nombre", "tipo"],
        },
    }

    def __init__(self, entity: str, limite: int = 1000, offset: int = 0):
        if entity not in self.ENTITIES:
            raise ValueError(f"Entidad no soportada: {entity}. Use: {list(self.ENTITIES.keys())}")

        self.entity = entity
        self.config = self.ENTITIES[entity]
        self.filters: Dict[str, Any] = {}
        self.fields: List[str] = list(self.config["default_fields"])
        self.limite = limite
        self.offset = offset

    def add_filter(self, name: str, value: Any) -> "QueryBuilder":
        """Agrega un filtro."""
        if value is not None and value != "":
            self.filters[name] = value
        return self

    def remove_filter(self, name: str) -> "QueryBuilder":
        """Elimina un filtro."""
        self.filters.pop(name, None)
        return self

    def clear_filters(self) -> "QueryBuilder":
        """Limpia todos los filtros."""
        self.filters.clear()
        return self

    def select_fields(self, fields: List[str]) -> "QueryBuilder":
        """Define los campos a seleccionar."""
        self.fields = fields
        return self

    def add_field(self, field: str) -> "QueryBuilder":
        """Agrega un campo a la seleccion."""
        if field not in self.fields:
            self.fields.append(field)
        return self

    def set_pagination(self, limite: int, offset: int = 0) -> "QueryBuilder":
        """Configura paginacion."""
        self.limite = limite
        self.offset = offset
        return self

    def _format_value(self, value: Any) -> str:
        """Formatea un valor para GraphQL."""
        if isinstance(value, str):
            # Escapar comillas
            escaped = value.replace('\\', '\\\\').replace('"', '\\"')
            return f'"{escaped}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif value is None:
            return "null"
        else:
            return str(value)

    def _build_selection_set(self, fields: List[str], indent: int = 4) -> str:
        """
        Construye el selection set para campos, incluyendo anidados.

        Ejemplo:
            ["id", "importe", "beneficiario.nif", "beneficiario.nombre"]
        Se convierte en:
            id
            importe
            beneficiario {
              nif
              nombre
            }
        """
        # Agrupar campos por prefijo
        simple_fields = []
        nested: Dict[str, List[str]] = {}

        for field in fields:
            if "." in field:
                parts = field.split(".", 1)
                parent = parts[0]
                child = parts[1]
                if parent not in nested:
                    nested[parent] = []
                nested[parent].append(child)
            else:
                simple_fields.append(field)

        # Construir string
        lines = []
        prefix = " " * indent

        for f in simple_fields:
            lines.append(f"{prefix}{f}")

        for parent, children in nested.items():
            lines.append(f"{prefix}{parent} {{")
            child_set = self._build_selection_set(children, indent + 2)
            lines.append(child_set)
            lines.append(f"{prefix}}}")

        return "\n".join(lines)

    def build(self) -> str:
        """
        Construye la query GraphQL completa.

        Returns:
            String con la query GraphQL formateada.
        """
        query_name = self.config["query_name"]

        # Construir argumentos
        args = []

        # Filtros
        if self.filters:
            filter_parts = []
            for key, value in self.filters.items():
                filter_parts.append(f"{key}: {self._format_value(value)}")
            filters_str = ", ".join(filter_parts)
            args.append(f"filtros: {{ {filters_str} }}")

        # Paginacion
        args.append(f"limite: {self.limite}")
        if self.offset > 0:
            args.append(f"offset: {self.offset}")

        args_str = ", ".join(args)

        # Selection set
        selection = self._build_selection_set(self.fields)

        # Query completa
        query = f"""query {{
  {query_name}({args_str}) {{
{selection}
  }}
}}"""

        return query

    def to_json(self) -> Dict:
        """
        Exporta la configuracion como JSON para almacenar.

        Returns:
            Dict con toda la configuracion del builder.
        """
        return {
            "entity": self.entity,
            "filters": self.filters,
            "fields": self.fields,
            "limite": self.limite,
            "offset": self.offset,
        }

    @classmethod
    def from_json(cls, data: Dict) -> "QueryBuilder":
        """
        Crea un QueryBuilder desde JSON guardado.

        Args:
            data: Dict con configuracion previamente exportada.

        Returns:
            QueryBuilder configurado.
        """
        builder = cls(
            entity=data["entity"],
            limite=data.get("limite", 1000),
            offset=data.get("offset", 0)
        )
        for key, value in data.get("filters", {}).items():
            builder.add_filter(key, value)
        if "fields" in data:
            builder.select_fields(data["fields"])
        return builder

    @classmethod
    def get_available_filters(cls, entity: str) -> List[Dict]:
        """
        Obtiene los filtros disponibles para una entidad.

        Returns:
            Lista de definiciones de filtros para el UI.
        """
        if entity not in cls.ENTITIES:
            return []

        config = cls.ENTITIES[entity]
        return [
            {
                "name": f.name,
                "label": f.label,
                "type": f.input_type,
                "options": f.options,
                "description": f.description,
                "ejemplo": f.ejemplo,
                "graphql_param": f.graphql_param,
            }
            for f in config["filters"]
        ]

    @classmethod
    def get_available_fields(cls, entity: str) -> Dict[str, str]:
        """
        Obtiene los campos disponibles para una entidad.

        Returns:
            Dict de campo -> descripcion.
        """
        if entity not in cls.ENTITIES:
            return {}
        return cls.ENTITIES[entity]["fields"]

    def preview(self) -> Dict:
        """
        Genera preview para mostrar en el UI.

        Returns:
            Dict con query, filtros activos, campos seleccionados.
        """
        return {
            "query": self.build(),
            "entity": self.entity,
            "filters_active": len(self.filters),
            "filters": self.filters,
            "fields_selected": len(self.fields),
            "fields": self.fields,
            "pagination": {
                "limite": self.limite,
                "offset": self.offset,
            }
        }


# Funciones de conveniencia para la API

def build_query_from_filters(
    entity: str,
    filters: Dict[str, Any],
    fields: Optional[List[str]] = None,
    limite: int = 1000
) -> str:
    """
    Funcion de conveniencia para construir query desde parametros.

    Args:
        entity: "concesiones" o "beneficiarios"
        filters: Dict con filtros activos
        fields: Lista de campos a seleccionar (o None para defaults)
        limite: Limite de resultados

    Returns:
        Query GraphQL como string.
    """
    builder = QueryBuilder(entity, limite=limite)

    for key, value in filters.items():
        builder.add_filter(key, value)

    if fields:
        builder.select_fields(fields)

    return builder.build()


def get_query_preview(
    entity: str,
    filters: Dict[str, Any],
    fields: Optional[List[str]] = None,
    limite: int = 1000
) -> Dict:
    """
    Obtiene preview de la query para mostrar en UI.

    Returns:
        Dict con query formateada y metadatos.
    """
    builder = QueryBuilder(entity, limite=limite)

    for key, value in filters.items():
        builder.add_filter(key, value)

    if fields:
        builder.select_fields(fields)

    return builder.preview()


# Ejemplos de uso
if __name__ == "__main__":
    # Ejemplo: Concesiones a congregaciones religiosas en 2024, importe > 10000€
    print("=" * 60)
    print("Ejemplo: Concesiones a religiosos 2024 > 10000€")
    print("=" * 60)

    builder = QueryBuilder("concesiones")
    builder.add_filter("año", 2024)
    builder.add_filter("tipo_beneficiario", "R")
    builder.add_filter("importe_minimo", 10000)
    builder.select_fields([
        "id", "codigo_bdns", "fecha_concesion", "importe",
        "beneficiario.nif", "beneficiario.nombre",
        "organo.nombre"
    ])

    print(builder.build())
    print()

    # Mostrar filtros disponibles
    print("=" * 60)
    print("Filtros disponibles para concesiones:")
    print("=" * 60)
    for f in QueryBuilder.get_available_filters("concesiones"):
        print(f"  - {f['name']}: {f['label']} ({f['type']})")
