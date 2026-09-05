CREATE TABLE :GRAPHILE_WORKER_SCHEMA._private_worker_control (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    paused boolean NOT NULL DEFAULT false,
    pause_reason text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((paused AND pause_reason IS NOT NULL) OR (NOT paused AND pause_reason IS NULL))
);

-- graphile-worker-rs:statement

INSERT INTO :GRAPHILE_WORKER_SCHEMA._private_worker_control (id, paused)
VALUES (true, false);

-- graphile-worker-rs:statement

CREATE INDEX _private_jobs_queue_claim_idx
    ON :GRAPHILE_WORKER_SCHEMA._private_jobs (task_id, job_queue_id, run_at, priority, id)
    WHERE is_available = true AND job_queue_id IS NOT NULL;

-- graphile-worker-rs:statement

CREATE INDEX _private_jobs_unqueued_task_claim_idx
    ON :GRAPHILE_WORKER_SCHEMA._private_jobs (task_id, run_at, priority, id)
    WHERE is_available = true AND job_queue_id IS NULL;
