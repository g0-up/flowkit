-- name: ListRuns :many
-- One keyset page of runs, live and summarised, newest first in
-- (started_at, run_id) order, after the cursor when one is given. A run is
-- in one of the two tables: summarising deletes it from runs. last_at is
-- the time a terminal run ended; a live run's last update otherwise.
-- Each branch reads one table, so the planner merges the two keyset
-- indexes in order and stops at the limit; a join in the runs branch would
-- make it read and sort the whole table instead.
SELECT u.run_id, u.def_name, u.def_version, u.state, u.terminal_name, u.step_count,
       u.trigger_event_type, u.entity_kind, u.entity_id, u.started_at, u.terminal, u.last_at
FROM (
  SELECT r.id AS run_id, r.def_name, r.def_version, r.state, r.terminal_name, r.step_seq AS step_count,
         (SELECT e.type FROM /* TEMPLATE: schema */domain_events e WHERE e.id = r.trigger_event_id) AS trigger_event_type,
         r.entity_kind, r.entity_id, r.created_at AS started_at, r.terminal, r.updated_at AS last_at
  FROM /* TEMPLATE: schema */runs r
  UNION ALL
  SELECT s.run_id, s.def_name, s.def_version, s.state, s.terminal_name, s.step_count,
         s.trigger_event_type, s.entity_kind, s.entity_id, s.started_at, true, s.ended_at
  FROM /* TEMPLATE: schema */run_summaries s
) u
WHERE (sqlc.narg(def_name)::text IS NULL OR u.def_name = sqlc.narg(def_name)::text)
  AND (cardinality(@states::text[]) = 0 OR u.state = ANY(@states::text[]))
  AND (sqlc.narg(entity_kind)::text IS NULL OR u.entity_kind = sqlc.narg(entity_kind)::text)
  AND (sqlc.narg(entity_id)::text IS NULL OR u.entity_id = sqlc.narg(entity_id)::text)
  AND (sqlc.narg(started_from)::timestamptz IS NULL OR u.started_at >= sqlc.narg(started_from)::timestamptz)
  AND (sqlc.narg(started_before)::timestamptz IS NULL OR u.started_at < sqlc.narg(started_before)::timestamptz)
  AND (sqlc.narg(after_at)::timestamptz IS NULL
       OR (u.started_at, u.run_id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY u.started_at DESC, u.run_id DESC
LIMIT @max_rows;
