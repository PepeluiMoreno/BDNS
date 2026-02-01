"""add estadisticas triggers

Revision ID: c3d4e5f6g7h8
Revises: b2c3d4e5f6g7
Create Date: 2026-02-01

Agrega triggers PostgreSQL para actualizar automaticamente
beneficiario_estadisticas_anuales cuando se modifican concesiones.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'c3d4e5f6g7h8'
down_revision = 'b2c3d4e5f6g7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Funcion para INSERT
    op.execute("""
    CREATE OR REPLACE FUNCTION fn_estadisticas_concesion_insert()
    RETURNS TRIGGER AS $$
    DECLARE
        v_organo_id VARCHAR;
        v_ejercicio INTEGER;
    BEGIN
        -- Obtener organo_id de la convocatoria
        SELECT organo_id INTO v_organo_id
        FROM convocatoria
        WHERE id = NEW.codigo_bdns;

        -- Calcular ejercicio desde fecha_concesion
        v_ejercicio := EXTRACT(YEAR FROM NEW.fecha_concesion);

        -- Si no hay organo o ejercicio, salir
        IF v_organo_id IS NULL OR v_ejercicio IS NULL THEN
            RETURN NEW;
        END IF;

        -- Insertar o actualizar estadisticas
        INSERT INTO beneficiario_estadisticas_anuales
            (beneficiario_id, ejercicio, organo_id,
             num_concesiones, importe_total, importe_medio,
             fecha_primera_concesion, fecha_ultima_concesion, created_at)
        VALUES
            (NEW.id_beneficiario, v_ejercicio, v_organo_id,
             1, COALESCE(NEW.importe, 0), COALESCE(NEW.importe, 0),
             NEW.fecha_concesion, NEW.fecha_concesion, NOW())
        ON CONFLICT (beneficiario_id, ejercicio, organo_id)
        DO UPDATE SET
            num_concesiones = beneficiario_estadisticas_anuales.num_concesiones + 1,
            importe_total = beneficiario_estadisticas_anuales.importe_total + COALESCE(NEW.importe, 0),
            importe_medio = (beneficiario_estadisticas_anuales.importe_total + COALESCE(NEW.importe, 0)) /
                           (beneficiario_estadisticas_anuales.num_concesiones + 1),
            fecha_primera_concesion = LEAST(
                beneficiario_estadisticas_anuales.fecha_primera_concesion,
                NEW.fecha_concesion
            ),
            fecha_ultima_concesion = GREATEST(
                beneficiario_estadisticas_anuales.fecha_ultima_concesion,
                NEW.fecha_concesion
            ),
            updated_at = NOW();

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)

    # Funcion para UPDATE
    op.execute("""
    CREATE OR REPLACE FUNCTION fn_estadisticas_concesion_update()
    RETURNS TRIGGER AS $$
    DECLARE
        v_organo_id_old VARCHAR;
        v_organo_id_new VARCHAR;
        v_ejercicio_old INTEGER;
        v_ejercicio_new INTEGER;
    BEGIN
        -- Obtener organos y ejercicios
        SELECT organo_id INTO v_organo_id_old FROM convocatoria WHERE id = OLD.codigo_bdns;
        SELECT organo_id INTO v_organo_id_new FROM convocatoria WHERE id = NEW.codigo_bdns;
        v_ejercicio_old := EXTRACT(YEAR FROM OLD.fecha_concesion);
        v_ejercicio_new := EXTRACT(YEAR FROM NEW.fecha_concesion);

        -- Si cambio la clave (beneficiario, ejercicio, organo), decrementar vieja e incrementar nueva
        IF OLD.id_beneficiario != NEW.id_beneficiario
           OR v_ejercicio_old != v_ejercicio_new
           OR v_organo_id_old != v_organo_id_new THEN

            -- Decrementar estadistica anterior
            IF v_organo_id_old IS NOT NULL AND v_ejercicio_old IS NOT NULL THEN
                UPDATE beneficiario_estadisticas_anuales SET
                    num_concesiones = GREATEST(num_concesiones - 1, 0),
                    importe_total = GREATEST(importe_total - COALESCE(OLD.importe, 0), 0),
                    importe_medio = CASE
                        WHEN num_concesiones <= 1 THEN 0
                        ELSE GREATEST(importe_total - COALESCE(OLD.importe, 0), 0) / GREATEST(num_concesiones - 1, 1)
                    END,
                    updated_at = NOW()
                WHERE beneficiario_id = OLD.id_beneficiario
                  AND ejercicio = v_ejercicio_old
                  AND organo_id = v_organo_id_old;
            END IF;

            -- Incrementar estadistica nueva
            IF v_organo_id_new IS NOT NULL AND v_ejercicio_new IS NOT NULL THEN
                INSERT INTO beneficiario_estadisticas_anuales
                    (beneficiario_id, ejercicio, organo_id,
                     num_concesiones, importe_total, importe_medio,
                     fecha_primera_concesion, fecha_ultima_concesion, created_at)
                VALUES
                    (NEW.id_beneficiario, v_ejercicio_new, v_organo_id_new,
                     1, COALESCE(NEW.importe, 0), COALESCE(NEW.importe, 0),
                     NEW.fecha_concesion, NEW.fecha_concesion, NOW())
                ON CONFLICT (beneficiario_id, ejercicio, organo_id)
                DO UPDATE SET
                    num_concesiones = beneficiario_estadisticas_anuales.num_concesiones + 1,
                    importe_total = beneficiario_estadisticas_anuales.importe_total + COALESCE(NEW.importe, 0),
                    importe_medio = (beneficiario_estadisticas_anuales.importe_total + COALESCE(NEW.importe, 0)) /
                                   (beneficiario_estadisticas_anuales.num_concesiones + 1),
                    fecha_primera_concesion = LEAST(
                        beneficiario_estadisticas_anuales.fecha_primera_concesion,
                        NEW.fecha_concesion
                    ),
                    fecha_ultima_concesion = GREATEST(
                        beneficiario_estadisticas_anuales.fecha_ultima_concesion,
                        NEW.fecha_concesion
                    ),
                    updated_at = NOW();
            END IF;
        ELSE
            -- Solo cambio el importe o fecha, actualizar estadistica existente
            IF v_organo_id_new IS NOT NULL AND v_ejercicio_new IS NOT NULL THEN
                UPDATE beneficiario_estadisticas_anuales SET
                    importe_total = importe_total - COALESCE(OLD.importe, 0) + COALESCE(NEW.importe, 0),
                    importe_medio = (importe_total - COALESCE(OLD.importe, 0) + COALESCE(NEW.importe, 0)) /
                                   GREATEST(num_concesiones, 1),
                    fecha_primera_concesion = LEAST(fecha_primera_concesion, NEW.fecha_concesion),
                    fecha_ultima_concesion = GREATEST(fecha_ultima_concesion, NEW.fecha_concesion),
                    updated_at = NOW()
                WHERE beneficiario_id = NEW.id_beneficiario
                  AND ejercicio = v_ejercicio_new
                  AND organo_id = v_organo_id_new;
            END IF;
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """)

    # Funcion para DELETE
    op.execute("""
    CREATE OR REPLACE FUNCTION fn_estadisticas_concesion_delete()
    RETURNS TRIGGER AS $$
    DECLARE
        v_organo_id VARCHAR;
        v_ejercicio INTEGER;
        v_count INTEGER;
    BEGIN
        SELECT organo_id INTO v_organo_id FROM convocatoria WHERE id = OLD.codigo_bdns;
        v_ejercicio := EXTRACT(YEAR FROM OLD.fecha_concesion);

        IF v_organo_id IS NULL OR v_ejercicio IS NULL THEN
            RETURN OLD;
        END IF;

        -- Verificar si quedan concesiones
        SELECT COUNT(*) INTO v_count
        FROM concesion c
        JOIN convocatoria conv ON c.codigo_bdns = conv.id
        WHERE c.id_beneficiario = OLD.id_beneficiario
          AND EXTRACT(YEAR FROM c.fecha_concesion) = v_ejercicio
          AND conv.organo_id = v_organo_id
          AND c.id != OLD.id;

        IF v_count = 0 THEN
            -- Eliminar registro de estadisticas
            DELETE FROM beneficiario_estadisticas_anuales
            WHERE beneficiario_id = OLD.id_beneficiario
              AND ejercicio = v_ejercicio
              AND organo_id = v_organo_id;
        ELSE
            -- Decrementar estadisticas
            UPDATE beneficiario_estadisticas_anuales SET
                num_concesiones = GREATEST(num_concesiones - 1, 0),
                importe_total = GREATEST(importe_total - COALESCE(OLD.importe, 0), 0),
                importe_medio = CASE
                    WHEN num_concesiones <= 1 THEN 0
                    ELSE GREATEST(importe_total - COALESCE(OLD.importe, 0), 0) / GREATEST(num_concesiones - 1, 1)
                END,
                updated_at = NOW()
            WHERE beneficiario_id = OLD.id_beneficiario
              AND ejercicio = v_ejercicio
              AND organo_id = v_organo_id;
        END IF;

        RETURN OLD;
    END;
    $$ LANGUAGE plpgsql;
    """)

    # Crear triggers
    op.execute("""
    CREATE TRIGGER trg_concesion_estadisticas_insert
    AFTER INSERT ON concesion
    FOR EACH ROW EXECUTE FUNCTION fn_estadisticas_concesion_insert();
    """)

    op.execute("""
    CREATE TRIGGER trg_concesion_estadisticas_update
    AFTER UPDATE ON concesion
    FOR EACH ROW EXECUTE FUNCTION fn_estadisticas_concesion_update();
    """)

    op.execute("""
    CREATE TRIGGER trg_concesion_estadisticas_delete
    AFTER DELETE ON concesion
    FOR EACH ROW EXECUTE FUNCTION fn_estadisticas_concesion_delete();
    """)


def downgrade() -> None:
    # Eliminar triggers
    op.execute("DROP TRIGGER IF EXISTS trg_concesion_estadisticas_delete ON concesion;")
    op.execute("DROP TRIGGER IF EXISTS trg_concesion_estadisticas_update ON concesion;")
    op.execute("DROP TRIGGER IF EXISTS trg_concesion_estadisticas_insert ON concesion;")

    # Eliminar funciones
    op.execute("DROP FUNCTION IF EXISTS fn_estadisticas_concesion_delete();")
    op.execute("DROP FUNCTION IF EXISTS fn_estadisticas_concesion_update();")
    op.execute("DROP FUNCTION IF EXISTS fn_estadisticas_concesion_insert();")
