"""
BDNS GraphQL API - Aplicación FastAPI principal
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from bdns_api.graphql import graphql_schema as schema



# Crear app FastAPI
app = FastAPI(
    title="BDNS GraphQL API",
    description="API GraphQL para consultar datos de la Base de Datos Nacional de Subvenciones",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GraphQL
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
async def root():
    return {
        "message": "BDNS GraphQL API",
        "graphql": "/graphql",
        "docs": "/graphql"
    }


@app.get("/health")
async def health():
    return {"status": "ok"}