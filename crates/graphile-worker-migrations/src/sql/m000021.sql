CREATE TABLE :GRAPHILE_WORKER_SCHEMA._private_worker_control (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    paused boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- graphile-worker-rs:statement

INSERT INTO :GRAPHILE_WORKER_SCHEMA._private_worker_control (id, paused)
VALUES (true, false);

-- graphile-worker-rs:statement

CREATE INDEX _private_jobs_queue_claim_idx
    ON :GRAPHILE_WORKER_SCHEMA._private_jobs (job_queue_id, priority, run_at, id)
    INCLUDE (task_id)
    WHERE is_available = true AND job_queue_id IS NOT NULL;
