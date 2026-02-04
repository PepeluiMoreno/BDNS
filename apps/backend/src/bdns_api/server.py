"""
BDNS Server - Wrapper de desarrollo

Uso:
    python server.py
    uvicorn server:app --reload --port 8000
"""
import sys
from pathlib import Path

# Setup paths para desarrollo local
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from backend.main import app  # Import desde el paquete

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "bdns_backend.main:app",  # String para reload
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"]
    )