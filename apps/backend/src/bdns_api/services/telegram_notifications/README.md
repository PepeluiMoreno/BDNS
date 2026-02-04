# Módulo de Notificaciones de Telegram para BDNS_graphql

Este módulo permite enviar alertas en tiempo real a Telegram basadas en suscripciones de usuarios con filtros tipo GraphQL.

## Estructura de la Carpeta

- `notifier.py`: El motor en Python que escucha la base de datos y envía los mensajes.
- `db_setup.sql`: Script para configurar las tablas y triggers necesarios en PostgreSQL.

## Pasos para la Integración

### 1. Configurar la Base de Datos
Ejecuta el script SQL para crear la tabla de suscripciones y los triggers:
```bash
psql -U tu_usuario -d tu_db -f telegram_notifications/db_setup.sql
```

### 2. Configurar Variables de Envorno
Asegúrate de tener tu token de Telegram en el archivo `.env` de la raíz del proyecto:
```env
TELEGRAM_TOKEN=tu_token_aqui
```

### 3. Ejecutar el Notificador
Desde la raíz del proyecto:
```bash
python telegram_notifications/notifier.py
```

## Funcionamiento
El sistema utiliza el comando `LISTEN/NOTIFY` de PostgreSQL. Cuando tus procesos ETL insertan datos en `concesion` o `convocatoria`, el trigger envía una señal que `notifier.py` captura, filtra según las preferencias del usuario y reenvía a Telegram.
