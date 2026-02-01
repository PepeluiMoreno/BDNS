# Estrategia de Extraccion ETL - BDNS

> Fecha: 2026-02-01
> Proyecto: BDNS_graphql

## 1. Vision General

El sistema ETL de BDNS sigue el paradigma **E-T-L estricto**:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   EXTRACT   │ --> │  TRANSFORM  │ --> │    LOAD     │
│  (API/CSV)  │     │ (Enriquecer)│     │    (BD)     │
└─────────────┘     └─────────────┘     └─────────────┘
     │                    │                    │
     v                    v                    v
  JSON/CSV raw      JSON enriquecido      PostgreSQL
```

Cada fase genera archivos intermedios que permiten:
- **Reintentos**: Si falla una fase, no se pierde el trabajo anterior
- **Auditoria**: Trazabilidad completa del proceso
- **Paralelismo**: Fases independientes pueden ejecutarse en paralelo

---

## 2. Modelos y Estrategia por Entidad

### 2.1 Catalogos (Datos Estaticos)

| Entidad | Fuente | Estrategia | Frecuencia |
|---------|--------|------------|------------|
| Organo | API BDNS `/organos` | Carga completa | Inicial + anual |
| Instrumento | API BDNS `/instrumentos` | Carga completa | Inicial |
| TipoBeneficiario | API BDNS `/beneficiarios` | Carga completa | Inicial |
| SectorActividad | CSV local (CNAE) | Carga desde CSV | Inicial |
| SectorProducto | API BDNS `/sectores` | Carga completa | Inicial |
| Region | API BDNS `/regiones` | Carga completa | Inicial |
| Finalidad | API BDNS `/finalidades` | Carga completa | Inicial |
| Objetivo | API BDNS `/objetivos` | Carga completa | Inicial |
| Reglamento | CSV local | Carga desde CSV | Inicial |
| Fondo | CSV local | Carga desde CSV | Inicial |

**Caracteristicas:**
- Sin dependencias externas (se cargan primero)
- Datos relativamente estaticos
- `INSERT ... ON CONFLICT DO NOTHING`

---

### 2.2 Convocatorias

```
┌──────────────────────────────────────────────────────────────────┐
│                     PIPELINE CONVOCATORIAS                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. extract_control_csv.py --year YYYY                           │
│     └─> API /convocatorias/busqueda                              │
│     └─> control/convocatoria_{year}.csv (lista de IDs)           │
│                                                                   │
│  2. extract_convocatorias.py --year YYYY                         │
│     └─> API /convocatorias?numConv={id} (paralelo, 10 workers)   │
│     └─> json/convocatorias/raw/raw_convocatorias_{tipo}_{mes}.json│
│                                                                   │
│  3. transform_convocatorias.py --year YYYY                       │
│     └─> Resolver FKs: organo_id, instrumentos, etc.              │
│     └─> json/convocatorias/transformed/convocatorias_{mes}.json  │
│                                                                   │
│  4. load_convocatorias.py --year YYYY                            │
│     └─> INSERT ... ON CONFLICT DO NOTHING                        │
│     └─> Batch size: 1000                                         │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

**Volumen esperado:** ~50,000-70,000 convocatorias/año

**Campos clave:**
- `id` (codigo_bdns) - PK
- `organo_id` - FK a organo
- `reglamento_id` - FK a reglamento
- Relaciones N:M con instrumentos, tipos_beneficiarios, sectores, etc.

**CSV de Control:**
```csv
codigo_bdns,fecha_recepcion,tipo_administracion,status,last_error,last_attempt,retries
123456,2024-01-15,A,pending,,,0
```

Estados: `pending` → `extracted` → (load actualiza en BD)

---

### 2.3 Concesiones

```
┌──────────────────────────────────────────────────────────────────┐
│                     PIPELINE CONCESIONES                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. extract_concesiones.py --year YYYY                           │
│     └─> API /concesiones?ejercicio={year}                        │
│     └─> control/concesiones_{year}.csv (datos completos)         │
│                                                                   │
│  2. transform_beneficiarios.py --year YYYY                       │
│     └─> Extraer beneficiarios unicos de CSV                      │
│     └─> Deducir forma_juridica desde NIF                         │
│     └─> Detectar pseudonimos (nombres alternativos)              │
│     └─> json/beneficiarios/transformed/beneficiarios_{year}.json │
│                                                                   │
│  3. load_beneficiarios.py --year YYYY                            │
│     └─> INSERT beneficiarios ON CONFLICT DO NOTHING              │
│     └─> INSERT pseudonimos ON CONFLICT DO NOTHING                │
│                                                                   │
│  4. transform_concesiones.py --year YYYY                         │
│     └─> Validar FKs: beneficiario_id, convocatoria_id            │
│     └─> Registrar incidencias (FKs no encontradas)               │
│     └─> json/concesiones/transformed/concesiones_{year}.json     │
│                                                                   │
│  5. load_concesiones.py --year YYYY                              │
│     └─> INSERT ... ON CONFLICT DO NOTHING                        │
│     └─> Batch size: 5000                                         │
│     └─> TRIGGERS actualizan estadisticas automaticamente         │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

**Volumen esperado:** ~1,000,000 concesiones/año

**Dependencias:**
- Requiere convocatorias cargadas (FK codigo_bdns)
- Requiere beneficiarios cargados (FK id_beneficiario)

**Optimizaciones:**
- Bulk insert con `INSERT ... ON CONFLICT DO NOTHING`
- Procesamiento por batches (5000 registros)
- Triggers PostgreSQL para estadisticas (no SQLAlchemy events)

---

### 2.4 Beneficiarios

**Estrategia especial:** Los beneficiarios NO se extraen directamente de la API.
Se extraen de los CSV de concesiones para garantizar consistencia.

```
CSV Concesiones
     │
     ├── idPersona: 12345
     ├── beneficiario: "B12345678 - EMPRESA EJEMPLO SL"
     │
     v
Transform:
     ├── id: 12345
     ├── nif: "B12345678"
     ├── nombre: "EMPRESA EJEMPLO SL"
     ├── nombre_norm: "empresa ejemplo sl"
     └── forma_juridica: "B" (deducido del NIF)
```

**Deteccion de pseudonimos:**
- Un mismo `id_persona` puede aparecer con diferentes nombres
- El nombre mas largo/normalizado se usa como principal
- Los demas se guardan como pseudonimos

---

### 2.5 Estadisticas (Calculadas)

**Tabla:** `beneficiario_estadisticas_anuales`

**Estrategia:** Triggers PostgreSQL actualizan automaticamente al insertar/actualizar/eliminar concesiones.

```sql
-- Trigger INSERT
CREATE TRIGGER trg_concesion_estadisticas_insert
AFTER INSERT ON concesion
FOR EACH ROW EXECUTE FUNCTION fn_estadisticas_concesion_insert();
```

**Metricas por beneficiario/año/organo:**
- `num_concesiones`
- `importe_total`
- `importe_medio`
- `fecha_primera_concesion`
- `fecha_ultima_concesion`

**Reconstruccion manual:**
```bash
python -m ETL.estadisticas.load.recalcular_estadisticas --truncate
```

---

### 2.6 Minimis y Ayudas de Estado

```
┌──────────────────────────────────────────────────────────────────┐
│                  PIPELINE MINIMIS / AYUDAS ESTADO                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. extract_minimis.py --year YYYY                               │
│     └─> API /minimis?ejercicio={year}                            │
│     └─> control/minimis_{year}.csv                               │
│                                                                   │
│  2. load_minimis.py --year YYYY                                  │
│     └─> Requiere: concesiones cargadas                           │
│     └─> INSERT ... ON CONFLICT DO NOTHING                        │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

**Dependencias:** Concesiones deben estar cargadas primero.

---

## 3. Sincronizacion Mensual

Los datos de BDNS pueden mutar hasta **48 meses** despues de publicarse.

```
┌──────────────────────────────────────────────────────────────────┐
│                    SYNC MENSUAL (cron)                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. detect_changes.py --meses 48                                 │
│     └─> Extraer IDs/hashes de API (ventana 48 meses)             │
│     └─> Comparar con BD local                                    │
│     └─> Detectar: NUEVAS, MODIFICADAS, ELIMINADAS                │
│     └─> sync/sync_changes_YYYYMMDD.json                          │
│                                                                   │
│  2. apply_changes.py --archivo sync_changes_YYYYMMDD.json        │
│     └─> INSERT nuevas                                            │
│     └─> UPDATE modificadas                                       │
│     └─> DELETE eliminadas                                        │
│     └─> Triggers actualizan estadisticas automaticamente         │
│                                                                   │
│  3. Registro en sync_control                                     │
│     └─> fecha_ejecucion, ventana_meses                           │
│     └─> inserts/updates/deletes detectados/aplicados             │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

**Algoritmo de deteccion:**
```python
ids_api = set(concesiones_api.keys())
ids_local = set(concesiones_local.keys())

NUEVAS = ids_api - ids_local
ELIMINADAS = ids_local - ids_api
POSIBLES_MODIFICADAS = ids_api & ids_local  # comparar hash
```

---

## 4. Orden de Ejecucion

### Carga Inicial (año completo)

```bash
# 1. Catalogos (prerequisito)
python -m ETL.run_etl --year 2024 --only create_etl_user,load_catalogos

# 2. Generar CSV de control de convocatorias
python ETL/convocatorias/extract_control_csv.py --year 2024

# 3. Pipeline completo
python -m ETL.run_etl --year 2024

# 4. Recalcular estadisticas (si se cargaron datos sin triggers)
python -m ETL.estadisticas.load.recalcular_estadisticas --truncate
```

### Sincronizacion Mensual (cron)

```bash
# Detectar y aplicar cambios de los ultimos 48 meses
python -m ETL.sync.detect_changes --meses 48
python -m ETL.sync.apply_changes --archivo sync/sync_changes_*.json
```

---

## 5. Gestion de Errores

### Reintentos
- CSV de control guarda `retries` y `last_error`
- Scripts reintentan automaticamente registros con `status=pending`
- Maximo 3 reintentos por defecto

### Incidencias
- FKs no encontradas se registran en `incidencias/`
- Formato: `{entidad}_{year}_{timestamp}.json`
- Permite analisis posterior sin bloquear la carga

### Transaccionalidad
- Cada batch es una transaccion
- Rollback automatico si falla el batch
- `ON CONFLICT DO NOTHING` evita duplicados

---

## 6. Volumetria y Rendimiento

| Entidad | Registros/año | Tiempo estimado |
|---------|---------------|-----------------|
| Convocatorias | ~67,000 | 2-4 horas |
| Concesiones | ~1,000,000 | 1-2 horas |
| Beneficiarios | ~200,000 | 15-30 min |
| Minimis | ~500,000 | 30-60 min |

**Factores que afectan rendimiento:**
- Velocidad de API BDNS (rate limiting)
- Conexion de red
- Workers paralelos (default: 10 para extract, 4 para load)
- Tamaño de batch (default: 5000)

---

## 7. Archivos de Configuracion

### ETL/config/etl_steps.yaml
Define pasos, dependencias y comportamiento en error.

### .env.development
```
DATABASE_URL=postgresql+psycopg2://bdns:bdns@localhost:5432/bdns
ETL_BATCH_SIZE=5000
ETL_MAX_WORKERS=4
```

---

## 8. Directorios

```
BDNS_graphql/
├── control/                    # CSVs de control (estado de extraccion)
├── json/
│   ├── convocatorias/
│   │   ├── raw/               # JSONs crudos de API
│   │   └── transformed/       # JSONs enriquecidos
│   ├── concesiones/
│   │   └── transformed/
│   └── beneficiarios/
│       └── transformed/
├── incidencias/               # Errores de validacion
├── sync/                      # Archivos de sincronizacion
└── logs/                      # Logs de ejecucion
```

---

## 9. Comandos Utiles

```bash
# Ver pasos disponibles
python -m ETL.run_etl --list

# Dry-run (sin ejecutar)
python -m ETL.run_etl --year 2024 --dry-run

# Solo algunos pasos
python -m ETL.run_etl --year 2024 --only load_catalogos,extract_convocatorias

# Saltar pasos
python -m ETL.run_etl --year 2024 --skip extract_minimis,load_minimis

# Verificar estadisticas
python -m ETL.estadisticas.load.recalcular_estadisticas --verify

# Sincronizacion (dry-run)
python -m ETL.sync.detect_changes --meses 24 --dry-run
```
