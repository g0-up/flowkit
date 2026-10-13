-- name: ListTriggeredDefinitions :many
SELECT name, enabled, current_version, trigger_event_type, concurrency_policy,
       serial_key_cel, dedup_window_seconds, created_at, updated_at
FROM /* TEMPLATE: schema */definitions
WHERE enabled AND trigger_event_type = @event_type AND current_version IS NOT NULL
ORDER BY name;

-- name: RunExists :one
SELECT EXISTS (SELECT 1 FROM /* TEMPLATE: schema */runs WHERE id = @id);

-- name: QueuedEventExists :one
-- Whether the event waits in the definition's queue under any key: a
-- republished serial key cannot queue the same event twice. rq_event
-- serves it.
SELECT EXISTS (
  SELECT 1 FROM /* TEMPLATE: schema */run_queue
  WHERE def_name = @def_name AND trigger_event_id = @trigger_event_id
);

-- name: InsertConsumedEvent :exec
INSERT INTO /* TEMPLATE: schema */consumed_events (run_id, event_id, step_id, at)
VALUES (@run_id, @event_id, @step_id, @at);

-- name: LockSerial :one
-- The concurrency gate. DO UPDATE locks the row whether it is new or not,
-- waiting for a transaction that has just inserted it. Rows are never
-- deleted: a deleted row would let a second transaction insert a fresh one
-- without waiting for the holder of the old one.
INSERT INTO /* TEMPLATE: schema */serial_locks (def_name, serial_key)
VALUES (@def_name, @serial_key)
ON CONFLICT (def_name, serial_key) DO UPDATE SET def_name = EXCLUDED.def_name
RETURNING 1;

-- name: LockActiveExclusiveRun :one
-- The non-terminal exclusive run of a key, at most one by runs_serial_active.
-- Callers hold the serial lock first.
SELECT id, def_name, def_version, metamodel_version, lib_version, definition, trigger_event_id,
       state, terminal, terminal_name, entity_kind, entity_id, scope_extra, serial_key, exclusive,
       wait_step, wait_seq, lookback_pending, frames, facts, rev, step_seq, created_at, updated_at
FROM /* TEMPLATE: schema */runs
WHERE def_name = @def_name AND serial_key = @serial_key AND exclusive AND NOT terminal
FOR UPDATE;

-- name: RunQueueNonEmpty :one
SELECT EXISTS (
  SELECT 1 FROM /* TEMPLATE: schema */run_queue
  WHERE def_name = @def_name AND serial_key = @serial_key
);

-- name: RecentRunExists :one
SELECT EXISTS (
  SELECT 1 FROM /* TEMPLATE: schema */runs
  WHERE def_name = @def_name AND serial_key = @serial_key AND created_at > @since
);

-- name: NewerRunExists :one
-- Whether a run of the key, in any state, was triggered by an event emitted
-- after the given one, in (inserted_at, id) order.
SELECT EXISTS (
  SELECT 1 FROM /* TEMPLATE: schema */runs r
  JOIN /* TEMPLATE: schema */domain_events e ON e.id = r.trigger_event_id
  WHERE r.def_name = @def_name AND r.serial_key = @serial_key
    AND (e.inserted_at > @event_at OR (e.inserted_at = @event_at AND e.id > @event_id))
);

-- name: InsertRunQueue :execrows
INSERT INTO /* TEMPLATE: schema */run_queue (def_name, serial_key, trigger_event_id, event_at, created_at)
VALUES (@def_name, @serial_key, @trigger_event_id, @event_at, @created_at)
ON CONFLICT (def_name, serial_key, trigger_event_id) DO NOTHING;

-- name: PeekRunQueue :one
-- The head of a key's queue, in emit order. Callers hold the serial lock,
-- so nobody else pops it.
SELECT def_name, serial_key, trigger_event_id, event_at, created_at
FROM /* TEMPLATE: schema */run_queue
WHERE def_name = @def_name AND serial_key = @serial_key
ORDER BY event_at, trigger_event_id
LIMIT 1;

-- name: DeleteRunQueue :execrows
DELETE FROM /* TEMPLATE: schema */run_queue
WHERE def_name = @def_name AND serial_key = @serial_key AND trigger_event_id = @trigger_event_id;

-- name: InsertConsumedEventIfAbsent :execrows
-- Marks an event consumed by a wait; no row when the run already consumed it.
INSERT INTO /* TEMPLATE: schema */consumed_events (run_id, event_id, step_id, at)
VALUES (@run_id, @event_id, @step_id, @at)
ON CONFLICT (run_id, event_id) DO NOTHING;
