import psycopg2
import psycopg2.extensions
import requests
import json
import time
import os
from dotenv import load_dotenv

# Cargar configuración desde la raíz del proyecto
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
DB_URL = f"postgresql://{os.getenv('DB_USER_LOCAL')}:{os.getenv('DB_PASSWORD_LOCAL')}@{os.getenv('DB_HOST_LOCAL')}:{os.getenv('DB_PORT_LOCAL')}/{os.getenv('DB_NAME_LOCAL')}"

def match_filters(data, filters, table_name):
    """
    Lógica de filtrado basada en ConcesionInput y ConvocatoriaInput.
    """
    try:
        if table_name == 'concesion':
            # Filtros para Concesiones
            if filters.get('importe_minimo') and data.get('importe', 0) < filters['importe_minimo']:
                return False
            if filters.get('beneficiario_id') and data.get('id_beneficiario') != filters['beneficiario_id']:
                return False
            if filters.get('codigo_bdns') and data.get('codigo_bdns') != filters['codigo_bdns']:
                return False
        
        elif table_name == 'convocatoria':
            # Filtros para Convocatorias
            if filters.get('mrr') is not None and data.get('mrr') != filters['mrr']:
                return False
            if filters.get('presupuesto_minimo') and data.get('presupuesto_total', 0) < filters['presupuesto_minimo']:
                return False
            
        return True
    except Exception as e:
        print(f"Error evaluando filtros: {e}")
        return False

def send_telegram_msg(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Error Telegram: {e}")

def format_message(payload):
    table = payload['table']
    action = payload['action']
    data = payload['data']
    
    emoji = "💰" if table == 'concesion' else "📢"
    action_text = "Nueva" if action == 'INSERT' else "Actualizada"
    
    if table == 'concesion':
        return (
            f"{emoji} *{action_text} Concesión BDNS*\n\n"
            f"🔹 *Importe:* {data.get('importe'):,.2f} €\n"
            f"🔹 *Fecha:* {data.get('fecha_concesion')}\n"
            f"🔹 *Beneficiario ID:* `{data.get('id_beneficiario')}`\n"
            f"🔗 [Ver Convocatoria](https://www.pap.hacienda.gob.es/bdnstrans/GE/es/convocatoria/{data.get('codigo_bdns')})"
        )
    else:
        return (
            f"{emoji} *{action_text} Convocatoria BDNS*\n\n"
            f"📝 *Descripción:* {data.get('descripcion')[:200]}...\n"
            f"💰 *Presupuesto:* {data.get('presupuesto_total'):,.2f} €\n"
            f"📅 *Fin Solicitud:* {data.get('fecha_fin_solicitud')}\n"
            f"🔗 [Enlace BDNS](https://www.pap.hacienda.gob.es/bdnstrans/GE/es/convocatoria/{data.get('id')})"
        )

def run():
    print("Iniciando motor de notificaciones Telegram para BDNS_graphql...")
    conn = psycopg2.connect(DB_URL)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute("LISTEN bdns_changes;")

    while True:
        if conn.poll() == psycopg2.extensions.POLL_OK:
            while conn.notifies:
                notify = conn.notifies.pop(0)
                payload = json.loads(notify.payload)
                table_name = payload['table']
                
                # Consultar suscripciones
                with psycopg2.connect(DB_URL) as conn_sub:
                    with conn_sub.cursor() as cur_sub:
                        cur_sub.execute("SELECT chat_id, filters FROM user_subscriptions WHERE active = TRUE")
                        for chat_id, filters in cur_sub.fetchall():
                            if match_filters(payload['data'], filters, table_name):
                                msg = format_message(payload)
                                send_telegram_msg(chat_id, msg)
        time.sleep(1)

if __name__ == "__main__":
    run()
