-- name: ListStaleLookbacks :many
-- Waiting runs whose look-back has not finished since before the cutoff:
-- the pump that ran it died.
SELECT id
FROM /* TEMPLATE: schema */runs
WHERE lookback_pending AND state = 'waiting' AND updated_at < @updated_before
ORDER BY updated_at, id
LIMIT @max_rows;

-- name: ListOrphanedQueues :many
-- Serial keys of enabled, published definitions with queued triggers and no
-- non-terminal exclusive run to promote them when it ends.
SELECT DISTINCT q.def_name, q.serial_key
FROM /* TEMPLATE: schema */run_queue q
JOIN /* TEMPLATE: schema */definitions d ON d.name = q.def_name
WHERE d.enabled AND d.current_version IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */runs r
    WHERE r.def_name = q.def_name AND r.serial_key = q.serial_key AND r.exclusive AND NOT r.terminal
  )
ORDER BY q.def_name, q.serial_key
LIMIT @max_rows;
