use chrono::{DateTime, Utc};
use graphile_worker_database::{DbExecutorArg, DbValue, Schema};
use graphile_worker_job::Job;
use indoc::formatdoc;

use crate::errors::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerControlState {
    pub paused: bool,
    pub pause_reason: Option<String>,
}

/// Reads the singleton worker-control row before one claim wave.
///
/// A missing row is an error, so callers fail closed rather than claiming
/// without observing pause. This is deliberately a plain snapshot read: a
/// concurrent pause may race with the following bounded claim and admit one
/// final wave, which callers finish normally.
pub async fn read_worker_control(
    mut executor: impl DbExecutorArg,
    schema: impl Into<Schema>,
) -> Result<WorkerControlState> {
    let worker_control = schema.into().private_table("worker_control");
    let row = executor
        .fetch_one(
            &format!("select paused, pause_reason from {worker_control} where id = true"),
            Vec::<DbValue>::new().into(),
        )
        .await?;
    Ok(WorkerControlState {
        paused: row.try_get("paused")?,
        pause_reason: row.try_get("pause_reason")?,
    })
}

/// The result of one bounded claim for a single task identifier.
#[derive(Debug)]
pub struct QueueClaim {
    pub jobs: Vec<Job>,
    /// Last ordering-row id examined. `None` means the current pass reached the end.
    pub next_ordering_cursor: Option<i32>,
}

/// Claims up to `quota` jobs for exactly one task identifier.
///
/// The caller owns the claim transaction after observing pause separately. This
/// function performs a bounded loose-index walk over at most `2 * quota`
/// ordering rows, locks ordering rows before jobs, and returns at most one job
/// per non-null ordering key.
#[allow(clippy::too_many_arguments)]
pub async fn claim_queue_jobs(
    mut executor: impl DbExecutorArg,
    schema: impl Into<Schema>,
    worker_id: &str,
    task_id: i32,
    task_identifier: &str,
    flags_to_skip: &[String],
    quota: i32,
    ordering_cursor: Option<i32>,
    now: Option<DateTime<Utc>>,
) -> Result<QueueClaim> {
    if quota <= 0 {
        return Ok(QueueClaim {
            jobs: Vec::new(),
            next_ordering_cursor: ordering_cursor,
        });
    }

    let schema = schema.into();
    let sql = claim_queue_jobs_sql(&schema);
    let rows = executor
        .fetch_all(
            &sql,
            vec![
                DbValue::Text(worker_id.to_string()),
                DbValue::I32(task_id),
                DbValue::I32(quota),
                DbValue::I32Opt(ordering_cursor),
                DbValue::TextArray(flags_to_skip.to_vec()),
                DbValue::TimestampTzOpt(now),
            ]
            .into(),
        )
        .await?;

    let next_ordering_cursor = rows
        .first()
        .map(|row| row.try_get("next_ordering_cursor"))
        .transpose()?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(job) = super::rows::db_job_from_nullable_row(&row)? {
            jobs.push(Job::from_db_job(job, task_identifier.to_string()));
        }
    }

    Ok(QueueClaim {
        jobs,
        next_ordering_cursor: next_ordering_cursor.flatten(),
    })
}

/// Builds the production queue-scoped claim statement for plan-contract tests.
pub fn claim_queue_jobs_sql(schema: &Schema) -> String {
    let jobs = schema.private_table("jobs");
    let job_queues = schema.private_table("job_queues");

    formatdoc!(
        r#"
            with recursive ordering_scan(job_queue_id, ordinal) as materialized (
                select first_key.job_queue_id, 1
                from lateral (
                    select jobs.job_queue_id
                    from {jobs} as jobs
                    where jobs.task_id = $2::int
                    and jobs.is_available = true
                    and jobs.job_queue_id is not null
                    and jobs.job_queue_id > coalesce($4::int, 0)
                    order by jobs.job_queue_id
                    limit 1
                ) as first_key

                union all

                select next_key.job_queue_id, ordering_scan.ordinal + 1
                from ordering_scan
                cross join lateral (
                    select jobs.job_queue_id
                    from {jobs} as jobs
                    where jobs.task_id = $2::int
                    and jobs.is_available = true
                    and jobs.job_queue_id is not null
                    and jobs.job_queue_id > ordering_scan.job_queue_id
                    order by jobs.job_queue_id
                    limit 1
                ) as next_key
                where ordering_scan.ordinal < ($3::int * 2)
            ),
            locked_queues as materialized (
                select job_queues.id, ordering_scan.ordinal
                from ordering_scan
                inner join {job_queues} as job_queues
                    on job_queues.id = ordering_scan.job_queue_id
                where job_queues.is_available = true
                order by ordering_scan.ordinal
                limit $3::int
                for update of job_queues
                skip locked
            ),
            progress as materialized (
                select case
                    when (select count(*) from ordering_scan) < ($3::int * 2)
                     and (select count(*) from locked_queues) = (select count(*) from ordering_scan)
                    then null::int
                    else coalesce(
                        (select max(id)::int from locked_queues),
                        (select max(job_queue_id)::int from ordering_scan)
                    )
                end as next_ordering_cursor
            ),
            queued_candidates as materialized (
                select candidate.id, candidate.run_at, candidate.priority
                from locked_queues
                cross join lateral (
                    select jobs.id, jobs.run_at, jobs.priority
                    from {jobs} as jobs
                    where jobs.task_id = $2::int
                    and jobs.job_queue_id = locked_queues.id
                    and jobs.is_available = true
                    and jobs.run_at <= coalesce($6::timestamptz, now())
                    and (cardinality($5::text[]) = 0 or (jobs.flags ?| $5::text[]) is not true)
                    order by jobs.run_at, jobs.priority, jobs.id
                    limit 1
                ) as candidate
            ),
            unqueued_candidates as materialized (
                select jobs.id, jobs.run_at, jobs.priority
                from {jobs} as jobs
                where jobs.task_id = $2::int
                and jobs.job_queue_id is null
                and jobs.is_available = true
                and jobs.run_at <= coalesce($6::timestamptz, now())
                and (cardinality($5::text[]) = 0 or (jobs.flags ?| $5::text[]) is not true)
                order by jobs.run_at, jobs.priority, jobs.id
                limit $3::int
            ),
            candidate_ids as materialized (
                select candidates.id
                from (
                    select * from queued_candidates
                    union all
                    select * from unqueued_candidates
                ) as candidates
                order by candidates.run_at, candidates.priority, candidates.id
                limit $3::int
            ),
            selected as materialized (
                select jobs.id, jobs.job_queue_id
                from {jobs} as jobs
                inner join candidate_ids on candidate_ids.id = jobs.id
                where jobs.task_id = $2::int
                and jobs.is_available = true
                and jobs.run_at <= coalesce($6::timestamptz, now())
                and (cardinality($5::text[]) = 0 or (jobs.flags ?| $5::text[]) is not true)
                order by jobs.run_at, jobs.priority, jobs.id
                limit $3::int
                for update of jobs
                skip locked
            ),
            lock_ordering_rows as (
                update {job_queues} as job_queues
                set locked_by = $1::text,
                    locked_at = coalesce($6::timestamptz, now())
                from selected
                where selected.job_queue_id = job_queues.id
            ),
            claimed as (
                update {jobs} as jobs
                set attempts = jobs.attempts + 1,
                    locked_by = $1::text,
                    locked_at = coalesce($6::timestamptz, now())
                from selected
                where jobs.id = selected.id
                returning jobs.*
            )
            select
                claimed.id,
                claimed.job_queue_id,
                claimed.payload,
                claimed.priority,
                claimed.run_at,
                claimed.attempts,
                claimed.max_attempts,
                claimed.last_error,
                claimed.created_at,
                claimed.updated_at,
                claimed.key,
                claimed.revision,
                claimed.locked_at,
                claimed.locked_by,
                claimed.flags,
                claimed.task_id,
                progress.next_ordering_cursor
            from progress
            left join claimed on true
            order by claimed.run_at, claimed.priority, claimed.id
        "#
    )
}
