-- 1. Tabla de Suscripciones de Usuarios
CREATE TABLE IF NOT EXISTS user_subscriptions (
    id SERIAL PRIMARY KEY,
    chat_id TEXT NOT NULL,
    filters JSONB NOT NULL, -- Almacena los filtros tipo ConcesionInput
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Función de Notificación para Concesiones
CREATE OR REPLACE FUNCTION notify_concesion_changes()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
BEGIN
    -- Capturamos el cambio en la tabla concesion
    IF (TG_OP = 'DELETE') THEN
        payload = json_build_object('table', 'concesion', 'action', TG_OP, 'data', row_to_json(OLD));
    ELSE
        payload = json_build_object('table', 'concesion', 'action', TG_OP, 'data', row_to_json(NEW));
    END IF;

    PERFORM pg_notify('bdns_changes', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 3. Trigger para la tabla concesion
DROP TRIGGER IF EXISTS trg_concesion_notify ON concesion;
CREATE TRIGGER trg_concesion_notify
AFTER INSERT OR UPDATE OR DELETE ON concesion
FOR EACH ROW EXECUTE FUNCTION notify_concesion_changes();

-- 4. Función de Notificación para Convocatorias
CREATE OR REPLACE FUNCTION notify_convocatoria_changes()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        payload = json_build_object('table', 'convocatoria', 'action', TG_OP, 'data', row_to_json(OLD));
    ELSE
        payload = json_build_object('table', 'convocatoria', 'action', TG_OP, 'data', row_to_json(NEW));
    END IF;

    PERFORM pg_notify('bdns_changes', payload::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 5. Trigger para la tabla convocatoria
DROP TRIGGER IF EXISTS trg_convocatoria_notify ON convocatoria;
CREATE TRIGGER trg_convocatoria_notify
AFTER INSERT OR UPDATE OR DELETE ON convocatoria
FOR EACH ROW EXECUTE FUNCTION notify_convocatoria_changes();
