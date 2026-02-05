"""
BDNS Server – wrapper de desarrollo

Uso recomendado:
    PYTHONPATH=src uvicorn bdns_api.main:app --reload

Uso alternativo (solo dev):
    python server.py
"""

import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "bdns_api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
    )
