use chrono::{DateTime, Utc};
use graphile_worker_database::{DbExecutorArg, DbParams, DbValue, Schema};
use indoc::formatdoc;

use crate::errors::Result;
use graphile_worker_job::Job;

use super::job_query_helpers::{
    get_flag_clause, get_now_clause, get_update_queue_clause, get_worker_control_cte,
};
use super::task_identifiers::TaskDetails;

pub async fn batch_get_jobs(
    mut executor: impl DbExecutorArg,
    task_details: &TaskDetails,
    schema: impl Into<Schema>,
    worker_id: &str,
    flags_to_skip: &[String],
    batch_size: i32,
    now: Option<DateTime<Utc>>,
) -> Result<Vec<Job>> {
    let schema = schema.into();
    let has_flags = !flags_to_skip.is_empty();
    let has_now = now.is_some();

    let mut next_param: u8 = 4;
    let flag_param = if has_flags {
        let p = next_param;
        next_param += 1;
        Some(p)
    } else {
        None
    };
    let now_param = if has_now { Some(next_param) } else { None };

    let flag_clause = flag_param
        .map(|p| get_flag_clause(flags_to_skip, p))
        .unwrap_or_default();
    let jobs = schema.private_table("jobs");
    let job_queues = schema.private_table("job_queues");
    let update_queue_clause = get_update_queue_clause(&schema, 1, now_param);
    let worker_control_cte = get_worker_control_cte(&schema);
    let now_clause = get_now_clause(now_param);

    let sql = formatdoc!(
        r#"
            with {worker_control_cte},
            available_queues as materialized (
                select
                    job_queues.id,
                    candidate.id as candidate_id,
                    candidate.priority as candidate_priority,
                    candidate.run_at as candidate_run_at
                    from {job_queues} as job_queues
                    cross join worker_control
                    cross join lateral (
                        select jobs.id, jobs.priority, jobs.run_at
                        from {jobs} as jobs
                        where jobs.job_queue_id = job_queues.id
                        and jobs.is_available = true
                        and jobs.run_at <= {now_clause}
                        and jobs.task_id = any($2::int[])
                        {flag_clause}
                        order by jobs.priority asc, jobs.run_at asc, jobs.id asc
                        limit 1
                    ) as candidate
                    where worker_control.paused = false
                    and job_queues.is_available = true
                    order by candidate.priority asc, candidate.run_at asc, candidate.id asc
                    limit $3::int
                    for update of job_queues
                    skip locked
            ),
            queued_candidates as materialized (
                select
                    available_queues.candidate_id as id,
                    available_queues.candidate_priority as priority,
                    available_queues.candidate_run_at as run_at
                    from available_queues
            ),
            unqueued_candidates as materialized (
                select jobs.id, jobs.priority, jobs.run_at
                    from {jobs} as jobs
                    cross join worker_control
                    where jobs.is_available = true
                    and worker_control.paused = false
                    and jobs.job_queue_id is null
                    and jobs.run_at <= {now_clause}
                    and jobs.task_id = any($2::int[])
                    {flag_clause}
                    order by jobs.priority asc, jobs.run_at asc, jobs.id asc
                    limit $3::int
                    for update of jobs
                    skip locked
            ),
            candidate_ids as materialized (
                select candidates.id
                from (
                    select * from queued_candidates
                    union all
                    select * from unqueued_candidates
                ) as candidates
                order by candidates.priority asc, candidates.run_at asc, candidates.id asc
                limit $3::int
            ),
            j as (
                select jobs.job_queue_id, jobs.priority, jobs.run_at, jobs.id
                    from {jobs} as jobs
                    inner join candidate_ids on candidate_ids.id = jobs.id
                    cross join worker_control
                    where jobs.is_available = true
                    and worker_control.paused = false
                    and jobs.run_at <= {now_clause}
                    and jobs.task_id = any($2::int[])
                    {flag_clause}
                    order by jobs.priority asc, jobs.run_at asc, jobs.id asc
                    for update
                    of jobs
                    skip locked
                ) {update_queue_clause}
                    update {jobs} as jobs
                        set
                            attempts = jobs.attempts + 1,
                            locked_by = $1::text,
                            locked_at = {now_clause}
                        from j
                        where jobs.id = j.id
                        returning *
        "#
    );

    let mut params = vec![
        DbValue::Text(worker_id.to_string()),
        DbValue::I32Array(task_details.task_ids().to_vec()),
        DbValue::I32(batch_size),
    ];

    if has_flags {
        params.push(DbValue::TextArray(flags_to_skip.to_vec()));
    }
    if let Some(ts) = now {
        params.push(DbValue::TimestampTz(ts));
    }

    let jobs = executor
        .fetch_all(&sql, DbParams::from(params))
        .await?
        .into_iter()
        .map(|row| super::rows::db_job_from_row(&row))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(jobs
        .into_iter()
        .map(|job| {
            let task_identifier = task_details.get_or_empty(job.id(), job.task_id());
            Job::from_db_job(job, task_identifier)
        })
        .collect())
}
