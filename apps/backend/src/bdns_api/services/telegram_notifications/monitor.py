#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Monitor de Notificaciones GraphQL para BDNS.

Ejecuta queries GraphQL periodicamente y detecta cambios (CRUD).
Envia notificaciones a Telegram cuando hay cambios.

Uso:
    python telegram_notifications/monitor.py --once      # Una ejecucion
    python telegram_notifications/monitor.py --daemon    # Modo daemon
    python telegram_notifications/monitor.py --test 123  # Test subscripcion ID

"""
import argparse
import hashlib
import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Setup path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env.development')

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Configuracion
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://bdns:bdns@localhost:5432/bdns')
GRAPHQL_URL = os.getenv('GRAPHQL_URL', 'http://localhost:8000/graphql')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def log(msg: str, level: str = "INFO"):
    """Log con timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


def execute_graphql_query(query: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Ejecuta una query GraphQL contra la API local.

    Returns:
        (data, error): data si ok, error message si falla
    """
    try:
        response = requests.post(
            GRAPHQL_URL,
            json={'query': query},
            headers={'Content-Type': 'application/json'},
            timeout=60
        )
        response.raise_for_status()
        result = response.json()

        if 'errors' in result:
            return None, str(result['errors'])

        return result.get('data'), None

    except requests.exceptions.ConnectionError:
        return None, "No se pudo conectar al servidor GraphQL"
    except requests.exceptions.Timeout:
        return None, "Timeout ejecutando query"
    except Exception as e:
        return None, str(e)


def extract_records(data: Dict, campo_id: str = "id") -> Dict[str, Dict]:
    """
    Extrae registros del resultado GraphQL como diccionario por ID.

    Maneja estructuras como:
    - {"concesiones": [...]}
    - {"data": {"concesiones": [...]}}
    - {"concesiones": {"items": [...]}}
    """
    if not data:
        return {}

    # Buscar el primer array en la respuesta
    def find_array(obj, depth=0):
        if depth > 5:
            return None
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ('__typename', 'pageInfo', 'totalCount'):
                    continue
                result = find_array(value, depth + 1)
                if result is not None:
                    return result
        return None

    records = find_array(data) or []

    # Convertir a dict por ID
    result = {}
    for record in records:
        if isinstance(record, dict):
            record_id = str(record.get(campo_id, ''))
            if record_id:
                result[record_id] = record

    return result


def compute_hash(records: Dict) -> str:
    """Calcula hash SHA256 de los registros."""
    serialized = json.dumps(records, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def detect_changes(
    current: Dict[str, Dict],
    previous: Dict[str, Dict],
    campos_comparar: Optional[List[str]] = None
) -> Dict[str, List]:
    """
    Detecta cambios entre dos snapshots.

    Returns:
        {
            'nuevos': [record, ...],
            'modificados': [{'anterior': record, 'actual': record}, ...],
            'eliminados': [record, ...]
        }
    """
    current_ids = set(current.keys())
    previous_ids = set(previous.keys()) if previous else set()

    nuevos = []
    modificados = []
    eliminados = []

    # Nuevos: en current pero no en previous
    for id_ in current_ids - previous_ids:
        nuevos.append(current[id_])

    # Eliminados: en previous pero no en current
    for id_ in previous_ids - current_ids:
        eliminados.append(previous[id_])

    # Modificados: en ambos, pero con diferencias
    for id_ in current_ids & previous_ids:
        curr = current[id_]
        prev = previous[id_]

        if campos_comparar:
            # Solo comparar campos especificos
            curr_subset = {k: curr.get(k) for k in campos_comparar}
            prev_subset = {k: prev.get(k) for k in campos_comparar}
            if curr_subset != prev_subset:
                modificados.append({'anterior': prev, 'actual': curr})
        else:
            # Comparar todo
            if curr != prev:
                modificados.append({'anterior': prev, 'actual': curr})

    return {
        'nuevos': nuevos,
        'modificados': modificados,
        'eliminados': eliminados
    }


def format_telegram_message(
    subscripcion_nombre: str,
    changes: Dict,
    limit: int = 5
) -> str:
    """Formatea mensaje para Telegram."""
    lines = [f"📊 *{subscripcion_nombre}*", ""]

    nuevos = changes.get('nuevos', [])
    modificados = changes.get('modificados', [])
    eliminados = changes.get('eliminados', [])

    total = len(nuevos) + len(modificados) + len(eliminados)

    if nuevos:
        lines.append(f"➕ *{len(nuevos)} nuevos*")
        for r in nuevos[:limit]:
            id_ = r.get('id', r.get('codigo_bdns', '?'))
            importe = r.get('importe', '')
            if importe:
                lines.append(f"  • ID {id_}: {importe:,.2f}€")
            else:
                lines.append(f"  • ID {id_}")
        if len(nuevos) > limit:
            lines.append(f"  _... y {len(nuevos) - limit} más_")
        lines.append("")

    if modificados:
        lines.append(f"✏️ *{len(modificados)} modificados*")
        for m in modificados[:limit]:
            id_ = m['actual'].get('id', m['actual'].get('codigo_bdns', '?'))
            lines.append(f"  • ID {id_}")
        if len(modificados) > limit:
            lines.append(f"  _... y {len(modificados) - limit} más_")
        lines.append("")

    if eliminados:
        lines.append(f"➖ *{len(eliminados)} eliminados*")
        for r in eliminados[:limit]:
            id_ = r.get('id', r.get('codigo_bdns', '?'))
            lines.append(f"  • ID {id_}")
        if len(eliminados) > limit:
            lines.append(f"  _... y {len(eliminados) - limit} más_")

    lines.append("")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    return "\n".join(lines)


def send_telegram_message(chat_id: str, text: str) -> bool:
    """Envia mensaje a Telegram."""
    if not TELEGRAM_TOKEN:
        log("TELEGRAM_TOKEN no configurado", "WARNING")
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        response = requests.post(url, json={
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'Markdown'
        }, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        log(f"Error enviando Telegram: {e}", "ERROR")
        return False


def calculate_next_execution(frecuencia: str, hora_preferida: int = 8) -> datetime:
    """Calcula proxima ejecucion segun frecuencia."""
    now = datetime.utcnow()

    if frecuencia == 'diaria':
        next_run = now.replace(hour=hora_preferida, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
    elif frecuencia == 'semanal':
        next_run = now.replace(hour=hora_preferida, minute=0, second=0, microsecond=0)
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and next_run <= now:
            days_until_monday = 7
        next_run += timedelta(days=days_until_monday)
    else:  # mensual
        next_run = now.replace(day=1, hour=hora_preferida, minute=0, second=0, microsecond=0)
        if now.month == 12:
            next_run = next_run.replace(year=now.year + 1, month=1)
        else:
            next_run = next_run.replace(month=now.month + 1)

    return next_run


def process_subscription(session, subscripcion) -> bool:
    """
    Procesa una suscripcion: ejecuta query, detecta cambios, notifica.

    Returns:
        True si hubo cambios, False si no
    """
    from bdns_core.db.models import EjecucionNotificacion

    log(f"Procesando: {subscripcion.nombre} (ID: {subscripcion.id})")

    # Crear registro de ejecucion
    ejecucion = EjecucionNotificacion(
        subscripcion_id=subscripcion.id,
        estado='ejecutando',
        registros_anteriores=subscripcion.last_check_count
    )
    session.add(ejecucion)
    session.flush()

    try:
        # Ejecutar query
        data, error = execute_graphql_query(subscripcion.graphql_query)

        if error:
            raise Exception(error)

        # Extraer registros
        current = extract_records(data, subscripcion.campo_id)
        previous = subscripcion.last_results or {}

        ejecucion.registros_actuales = len(current)

        # Detectar cambios
        campos_comparar = subscripcion.campos_comparar
        changes = detect_changes(current, previous, campos_comparar)

        ejecucion.nuevos = len(changes['nuevos'])
        ejecucion.modificados = len(changes['modificados'])
        ejecucion.eliminados = len(changes['eliminados'])

        total_cambios = ejecucion.nuevos + ejecucion.modificados + ejecucion.eliminados

        # Si hay cambios, notificar
        if total_cambios > 0:
            log(f"  Cambios: +{ejecucion.nuevos} ~{ejecucion.modificados} -{ejecucion.eliminados}")

            # Obtener chat_id del usuario
            chat_id = subscripcion.usuario.telegram_chat_id

            if chat_id and subscripcion.usuario.telegram_verificado:
                msg = format_telegram_message(subscripcion.nombre, changes)
                if send_telegram_message(chat_id, msg):
                    ejecucion.notificacion_enviada = True
                    ejecucion.mensaje_enviado = msg
                    log(f"  Notificacion enviada a {chat_id}")
            else:
                log(f"  Usuario sin Telegram verificado, no se notifica", "WARNING")

            # Guardar detalle de cambios (limitar para no saturar DB)
            if total_cambios <= 100:
                ejecucion.detalle_cambios = changes
        else:
            log(f"  Sin cambios ({len(current)} registros)")

        # Actualizar snapshot
        subscripcion.last_results = current
        subscripcion.last_result_hash = compute_hash(current)
        subscripcion.last_check = datetime.utcnow()
        subscripcion.last_check_count = len(current)
        subscripcion.errores_consecutivos = 0
        subscripcion.proxima_ejecucion = calculate_next_execution(
            subscripcion.frecuencia,
            subscripcion.hora_preferida
        )

        ejecucion.estado = 'completado'
        session.commit()

        return total_cambios > 0

    except Exception as e:
        log(f"  Error: {e}", "ERROR")

        subscripcion.errores_consecutivos += 1
        subscripcion.ultimo_error = str(e)

        if subscripcion.errores_consecutivos >= subscripcion.max_errores:
            subscripcion.pausado_por_errores = True
            log(f"  Suscripcion pausada por {subscripcion.max_errores} errores consecutivos", "WARNING")

        ejecucion.estado = 'error'
        ejecucion.error = str(e)
        session.commit()

        return False


def run_once():
    """Ejecuta una vez todas las suscripciones pendientes."""
    from bdns_core.db.models import SubscripcionNotificacion

    log("Iniciando ejecucion unica del monitor")

    with SessionLocal() as session:
        # Obtener suscripciones activas y pendientes
        now = datetime.utcnow()
        subs = session.query(SubscripcionNotificacion).filter(
            SubscripcionNotificacion.activo == True,
            SubscripcionNotificacion.pausado_por_errores == False,
            (SubscripcionNotificacion.proxima_ejecucion <= now) |
            (SubscripcionNotificacion.proxima_ejecucion == None)
        ).all()

        log(f"Encontradas {len(subs)} suscripciones pendientes")

        changes_detected = 0
        for sub in subs:
            if process_subscription(session, sub):
                changes_detected += 1

        log(f"Completado: {changes_detected} suscripciones con cambios")


def run_daemon(interval: int = 300):
    """Ejecuta en modo daemon, verificando cada N segundos."""
    log(f"Iniciando modo daemon (intervalo: {interval}s)")

    while True:
        try:
            run_once()
        except Exception as e:
            log(f"Error en daemon: {e}", "ERROR")

        log(f"Esperando {interval}s...")
        time.sleep(interval)


def test_subscription(subscription_id: int, limit: int = 100):
    """
    Ejecuta una suscripcion en modo test.

    Muestra los primeros N registros sin guardar ni notificar.
    """
    from bdns_core.db.models import SubscripcionNotificacion

    log(f"Modo TEST para suscripcion ID: {subscription_id}")

    with SessionLocal() as session:
        sub = session.query(SubscripcionNotificacion).get(subscription_id)

        if not sub:
            log(f"Suscripcion {subscription_id} no encontrada", "ERROR")
            return

        log(f"Nombre: {sub.nombre}")
        log(f"Query: {sub.graphql_query[:100]}...")
        log("")

        # Ejecutar query
        data, error = execute_graphql_query(sub.graphql_query)

        if error:
            log(f"Error: {error}", "ERROR")
            return

        # Extraer registros
        records = extract_records(data, sub.campo_id)

        log(f"Registros encontrados: {len(records)}")
        log("")

        # Mostrar primeros N
        log(f"Primeros {min(limit, len(records))} registros:")
        log("-" * 60)

        for i, (id_, record) in enumerate(list(records.items())[:limit]):
            importe = record.get('importe', '')
            beneficiario = record.get('beneficiario', {})
            nif = beneficiario.get('nif', '') if isinstance(beneficiario, dict) else ''

            if importe:
                log(f"  [{id_}] NIF: {nif} | Importe: {importe:,.2f}€")
            else:
                log(f"  [{id_}] {json.dumps(record, default=str)[:80]}")

        if len(records) > limit:
            log(f"  ... y {len(records) - limit} más")

        # Simular deteccion de cambios si hay snapshot previo
        if sub.last_results:
            changes = detect_changes(records, sub.last_results, sub.campos_comparar)
            log("")
            log("Cambios detectados (vs snapshot anterior):")
            log(f"  Nuevos: {len(changes['nuevos'])}")
            log(f"  Modificados: {len(changes['modificados'])}")
            log(f"  Eliminados: {len(changes['eliminados'])}")


def main():
    parser = argparse.ArgumentParser(description='Monitor de notificaciones GraphQL')
    parser.add_argument('--once', action='store_true', help='Ejecutar una vez')
    parser.add_argument('--daemon', action='store_true', help='Modo daemon')
    parser.add_argument('--interval', type=int, default=300, help='Intervalo en segundos (daemon)')
    parser.add_argument('--test', type=int, help='Test suscripcion ID')
    parser.add_argument('--limit', type=int, default=100, help='Limite registros en test')

    args = parser.parse_args()

    if args.test:
        test_subscription(args.test, args.limit)
    elif args.daemon:
        run_daemon(args.interval)
    else:
        run_once()


if __name__ == "__main__":
    main()
