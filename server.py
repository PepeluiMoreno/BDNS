"""
BDNS GraphQL Server

Servidor principal que expone la API GraphQL con Strawberry.

Uso:
    python server.py
    # o
    uvicorn server:app --reload --port 8000
"""
import sys
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from app.graphql.schema import schema

# Crear app FastAPI
app = FastAPI(
    title="BDNS GraphQL API",
    description="API GraphQL para consultar datos de la Base de Datos Nacional de Subvenciones",
    version="1.0.0"
)

# Configurar CORS para permitir el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En produccion, especificar dominios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Montar GraphQL router
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
async def root():
    return {
        "message": "BDNS GraphQL API",
        "graphql": "/graphql",
        "docs": "/graphql"  # Strawberry GraphiQL
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
