# etl_utils.py
# Utilidades ETL: gestión de pendientes y rutas de carpetas absolutas

from pathlib import Path
import csv

def registrar_pendiente(modelo, valor):
    """
    Añade un valor pendiente al CSV del catálogo/modelo correspondiente solo si no está ya.
    - modelo: nombre del catálogo (ej: 'organo', 'finalidad', ...)
    - valor: la descripción o clave no resuelta
    """
    pending_dir = Path(__file__).resolve().parent / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
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



def get_or_create_dir(*parts):
    """
    Devuelve un Path absoluto asegurando que la ruta existe.
    Ejemplo: get_or_create_dir("jsons", "convocatorias")
    """
    base_dir = Path(__file__).resolve().parent.parent  # Apunta a la raíz del ETL
    full_path = base_dir.joinpath(*parts)
    full_path.mkdir(parents=True, exist_ok=True)
    return full_path

