-- name: LockExpiredTerminalRuns :many
-- Terminal runs that ended before the cutoff, locked for summarising. A run
-- another transaction holds is left for a later batch.
SELECT id
FROM /* TEMPLATE: schema */runs
WHERE terminal AND updated_at < @ended_before
ORDER BY updated_at, id
LIMIT @max_rows
FOR UPDATE SKIP LOCKED;

-- name: InsertRunSummaries :exec
-- A terminal run's updated_at is the time it ended: nothing updates a run
-- after its terminal transition.
INSERT INTO /* TEMPLATE: schema */run_summaries
  (run_id, def_name, def_version, state, terminal_name, step_count, trigger_event_type,
   entity_kind, entity_id, started_at, ended_at)
SELECT r.id, r.def_name, r.def_version, r.state, r.terminal_name, r.step_seq, e.type,
       r.entity_kind, r.entity_id, r.created_at, r.updated_at
FROM /* TEMPLATE: schema */runs r
JOIN /* TEMPLATE: schema */domain_events e ON e.id = r.trigger_event_id
WHERE r.id = ANY(@run_ids::uuid[]) AND r.terminal
ON CONFLICT (run_id) DO NOTHING;

-- name: DeleteRunStepsOf :exec
DELETE FROM /* TEMPLATE: schema */run_steps
WHERE run_id = ANY(@run_ids::uuid[]);

-- name: DeleteConsumedEventsOf :exec
DELETE FROM /* TEMPLATE: schema */consumed_events
WHERE run_id = ANY(@run_ids::uuid[]);

-- name: DeleteSubscriptionsOf :exec
DELETE FROM /* TEMPLATE: schema */subscriptions
WHERE run_id = ANY(@run_ids::uuid[]);

-- name: DeleteEffectsOf :exec
DELETE FROM /* TEMPLATE: schema */executed_effects
WHERE run_id = ANY(@run_ids::uuid[]);

-- name: DeleteTerminalRuns :execrows
DELETE FROM /* TEMPLATE: schema */runs
WHERE id = ANY(@run_ids::uuid[]) AND terminal;

-- name: DeleteExpiredSummaries :execrows
-- Walks rs_def_ended once per definition; definitions are never deleted.
DELETE FROM /* TEMPLATE: schema */run_summaries
WHERE run_id IN (
  SELECT x.run_id
  FROM /* TEMPLATE: schema */definitions d
  CROSS JOIN LATERAL (
    SELECT s.run_id FROM /* TEMPLATE: schema */run_summaries s
    WHERE s.def_name = d.name AND s.ended_at < @ended_before
    LIMIT @max_rows
  ) x
  LIMIT @max_rows
);

-- name: DeleteExpiredEvents :execrows
-- Events stored before the cutoff that no run, consumed event or queued
-- trigger references.
DELETE FROM /* TEMPLATE: schema */domain_events
WHERE id IN (
  SELECT e.id FROM /* TEMPLATE: schema */domain_events e
  WHERE e.inserted_at < @inserted_before
    AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */runs r WHERE r.trigger_event_id = e.id)
    AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */consumed_events c WHERE c.event_id = e.id)
    AND NOT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */run_queue q WHERE q.trigger_event_id = e.id)
  ORDER BY e.inserted_at
  LIMIT @max_rows
);

-- name: DeleteResolvedDeadLetters :execrows
-- Open dead letters are never deleted.
DELETE FROM /* TEMPLATE: schema */dead_letters
WHERE id IN (
  SELECT d.id FROM /* TEMPLATE: schema */dead_letters d
  WHERE d.resolved_at IS NOT NULL AND d.resolved_at < @resolved_before
  ORDER BY d.resolved_at
  LIMIT @max_rows
);
