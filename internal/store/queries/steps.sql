-- name: InsertStep :exec
INSERT INTO /* TEMPLATE: schema */run_steps
  (run_id, seq, at, step_id, kind, detail, snapshot, attempt, error)
VALUES (@run_id, @seq, @at, @step_id, @kind, sqlc.narg(detail), sqlc.narg(snapshot), sqlc.narg(attempt), sqlc.narg(error));

-- name: ListSteps :many
SELECT run_id, seq, at, step_id, kind, detail, snapshot, attempt, error
FROM /* TEMPLATE: schema */run_steps
WHERE run_id = @run_id
ORDER BY seq;

-- name: StepRecorded :one
-- Reports whether the run has a step of this id, kind and detail.
SELECT EXISTS (
  SELECT 1 FROM /* TEMPLATE: schema */run_steps
  WHERE run_id = @run_id AND step_id = @step_id AND kind = @kind AND detail = @detail
);
