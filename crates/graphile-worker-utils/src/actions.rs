use graphile_worker_database::{DbExecutorArg, DbParams, DbRow, DbValue};
use indoc::formatdoc;

use super::client::WorkerUtils;
use super::types::{RescheduleJobOptions, WorkerControlState, WorkerPauseUpdate};
use graphile_worker_job::DbJob;
use graphile_worker_queries::errors::GraphileWorkerError;
use graphile_worker_queries::schema_names::WorkerFunction;

pub(super) async fn worker_control_state(
    utils: &WorkerUtils,
    mut executor: impl DbExecutorArg,
) -> Result<WorkerControlState, GraphileWorkerError> {
    let worker_control = utils.schema.private_table("worker_control");
    let sql = formatdoc!(
        r#"
            select paused, pause_reason, updated_at
            from {worker_control}
            where id = true;
        "#
    );
    let row = executor.fetch_one(&sql, DbParams::new()).await?;
    worker_control_state_from_row(&row, "paused", "pause_reason", "updated_at")
}

pub(super) async fn set_worker_pause(
    utils: &WorkerUtils,
    mut executor: impl DbExecutorArg,
    pause_reason: Option<&str>,
) -> Result<WorkerPauseUpdate, GraphileWorkerError> {
    let worker_control = utils.schema.private_table("worker_control");
    let sql = formatdoc!(
        r#"
            with previous as materialized (
                select paused, pause_reason, updated_at
                from {worker_control}
                where id = true
                for update
            ),
            updated as (
                update {worker_control} as worker_control
                set
                    paused = $1::text is not null,
                    pause_reason = $1::text,
                    updated_at = case
                        when worker_control.pause_reason is distinct from $1::text then now()
                        else worker_control.updated_at
                    end
                from previous
                where worker_control.id = true
                returning
                    previous.paused as previous_paused,
                    previous.pause_reason as previous_pause_reason,
                    previous.updated_at as previous_updated_at,
                    worker_control.paused,
                    worker_control.pause_reason,
                    worker_control.updated_at
            )
            select * from updated;
        "#
    );
    let row = executor
        .fetch_one(
            &sql,
            vec![DbValue::TextOpt(pause_reason.map(str::to_string))].into(),
        )
        .await?;
    Ok(WorkerPauseUpdate {
        previous: worker_control_state_from_row(
            &row,
            "previous_paused",
            "previous_pause_reason",
            "previous_updated_at",
        )?,
        current: worker_control_state_from_row(&row, "paused", "pause_reason", "updated_at")?,
    })
}

fn worker_control_state_from_row(
    row: &DbRow,
    paused: &str,
    pause_reason: &str,
    updated_at: &str,
) -> Result<WorkerControlState, GraphileWorkerError> {
    Ok(WorkerControlState {
        paused: row.try_get(paused)?,
        pause_reason: row.try_get(pause_reason)?,
        updated_at: row.try_get(updated_at)?,
    })
}

pub(super) async fn remove_job(
    utils: &WorkerUtils,
    mut executor: impl DbExecutorArg,
    job_key: &str,
) -> Result<(), GraphileWorkerError> {
    let remove_job = WorkerFunction::RemoveJob.qualified(&utils.schema);
    let sql = formatdoc!(
        r#"
            select * from {remove_job}($1::text);
        "#
    );

    executor
        .execute(&sql, vec![DbValue::Text(job_key.to_string())].into())
        .await?;

    Ok(())
}

pub(super) async fn complete_jobs(
    utils: &WorkerUtils,
    executor: impl DbExecutorArg,
    ids: &[i64],
) -> Result<Vec<DbJob>, GraphileWorkerError> {
    let complete_jobs = WorkerFunction::CompleteJobs.qualified(&utils.schema);
    let sql = formatdoc!(
        r#"
            select * from {complete_jobs}($1::bigint[]);
        "#
    );

    fetch_db_jobs(executor, &sql, vec![DbValue::I64Array(ids.to_vec())]).await
}

pub(super) async fn permanently_fail_jobs(
    utils: &WorkerUtils,
    executor: impl DbExecutorArg,
    ids: &[i64],
    reason: &str,
) -> Result<Vec<DbJob>, GraphileWorkerError> {
    let permanently_fail_jobs = WorkerFunction::PermanentlyFailJobs.qualified(&utils.schema);
    let sql = formatdoc!(
        r#"
            select * from {permanently_fail_jobs}($1::bigint[], $2::text);
        "#
    );

    fetch_db_jobs(
        executor,
        &sql,
        vec![
            DbValue::I64Array(ids.to_vec()),
            DbValue::Text(reason.to_string()),
        ],
    )
    .await
}

pub(super) async fn reschedule_jobs(
    utils: &WorkerUtils,
    executor: impl DbExecutorArg,
    ids: &[i64],
    options: RescheduleJobOptions,
) -> Result<Vec<DbJob>, GraphileWorkerError> {
    let reschedule_jobs = WorkerFunction::RescheduleJobs.qualified(&utils.schema);
    let sql = formatdoc!(
        r#"
            select * from {reschedule_jobs}(
                $1::bigint[],
                run_at := $2::timestamptz,
                priority := $3::int,
                attempts := $4::int,
                max_attempts := $5::int
            );
        "#
    );

    fetch_db_jobs(
        executor,
        &sql,
        vec![
            DbValue::I64Array(ids.to_vec()),
            DbValue::TimestampTzOpt(options.run_at),
            DbValue::I32Opt(options.priority.map(i32::from)),
            DbValue::I32Opt(options.attempts.map(i32::from)),
            DbValue::I32Opt(options.max_attempts.map(i32::from)),
        ],
    )
    .await
}

pub(super) async fn force_unlock_workers(
    utils: &WorkerUtils,
    mut executor: impl DbExecutorArg,
    worker_ids: &[&str],
) -> Result<(), GraphileWorkerError> {
    let force_unlock_workers = WorkerFunction::ForceUnlockWorkers.qualified(&utils.schema);
    let sql = formatdoc!(
        r#"
            select * from {force_unlock_workers}($1::text[]);
        "#
    );

    executor
        .execute(
            &sql,
            vec![DbValue::TextArray(
                worker_ids
                    .iter()
                    .map(|worker_id| worker_id.to_string())
                    .collect(),
            )]
            .into(),
        )
        .await?;

    Ok(())
}

async fn fetch_db_jobs(
    mut executor: impl DbExecutorArg,
    sql: &str,
    params: Vec<DbValue>,
) -> Result<Vec<DbJob>, GraphileWorkerError> {
    let jobs = executor
        .fetch_all(sql, params.into())
        .await?
        .iter()
        .map(graphile_worker_queries::rows::db_job_from_row)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(jobs)
}
