CREATE TABLE etl_job (
    id BIGSERIAL PRIMARY KEY,

    entity TEXT NOT NULL,              -- 'convocatoria', 'concesion'
    year INTEGER NOT NULL,
    mes INTEGER,
    tipo CHAR(1),

    stage TEXT NOT NULL,               -- 'extract', 'transform', 'load', 'sync'
    status TEXT NOT NULL DEFAULT 'pending',
        -- pending | running | done | error

    retries INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),

    UNIQUE (entity, year, mes, tipo, stage)
);

CREATE INDEX idx_etl_job_pending
    ON etl_job (status, stage);

CREATE INDEX idx_etl_job_scope
    ON etl_job (entity, year, mes, tipo);
