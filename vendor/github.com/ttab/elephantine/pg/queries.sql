-- name: GetJobLock :one
SELECT holder, touched, iteration
FROM job_lock
WHERE name = $1
FOR UPDATE;

-- name: InsertJobLock :one
INSERT INTO job_lock(name, holder, touched, iteration)
VALUES (@name, @holder, now(), 1)
RETURNING iteration;

-- name: PingJobLock :execrows
UPDATE job_lock
SET touched = now(),
    iteration = iteration + 1
WHERE name = @name
      AND holder = @holder
      AND iteration = @iteration;

-- name: StealJobLock :execrows
UPDATE job_lock
SET holder = @new_holder,
    touched = now(),
    iteration = iteration + 1
WHERE name = @name
      AND holder = @previous_holder
      AND iteration = @iteration;

-- name: ReleaseJobLock :execrows
DELETE FROM job_lock
WHERE name = @name
      AND holder = @holder;

-- name: AcquireTXLock :exec
SELECT pg_advisory_xact_lock(@id::bigint);

-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);
