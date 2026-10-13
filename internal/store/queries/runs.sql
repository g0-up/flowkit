-- name: InsertRun :exec
INSERT INTO /* TEMPLATE: schema */runs
  (id, def_name, def_version, metamodel_version, lib_version, definition, trigger_event_id,
   state, terminal, terminal_name, entity_kind, entity_id, scope_extra, serial_key, exclusive,
   wait_step, wait_seq, lookback_pending, frames, facts, rev, step_seq, created_at, updated_at)
VALUES
  (@id, @def_name, @def_version, @metamodel_version, @lib_version, @definition, @trigger_event_id,
   @state, @terminal, sqlc.narg(terminal_name), sqlc.narg(entity_kind), sqlc.narg(entity_id), sqlc.narg(scope_extra),
   sqlc.narg(serial_key), @exclusive,
   sqlc.narg(wait_step), @wait_seq, @lookback_pending, @frames, @facts, @rev, @step_seq, @created_at, @updated_at);

-- name: GetRun :one
SELECT id, def_name, def_version, metamodel_version, lib_version, definition, trigger_event_id,
       state, terminal, terminal_name, entity_kind, entity_id, scope_extra, serial_key, exclusive,
       wait_step, wait_seq, lookback_pending, frames, facts, rev, step_seq, created_at, updated_at
FROM /* TEMPLATE: schema */runs
WHERE id = @id;

-- name: NextStepSeq :one
-- Allocates the next run_steps.seq. It does not touch rev, so recording a
-- step never conflicts with the optimistic run version.
UPDATE /* TEMPLATE: schema */runs
SET step_seq = step_seq + 1
WHERE id = @id
RETURNING step_seq;

-- name: ListRunningRunIDs :many
SELECT id
FROM /* TEMPLATE: schema */runs
WHERE def_name = @def_name AND state = 'running' AND NOT terminal
ORDER BY id;

-- name: CountPinnedRuns :many
-- Non-terminal runs per definition version: the versions still in use.
SELECT def_version, count(*) AS run_count
FROM /* TEMPLATE: schema */runs
WHERE def_name = @def_name AND NOT terminal
GROUP BY def_version
ORDER BY def_version;

-- name: FinishRun :execrows
-- Moves a run to a terminal state if it still has rev and is not terminal.
UPDATE /* TEMPLATE: schema */runs
SET state = @state, terminal = true, terminal_name = @terminal_name, wait_step = NULL,
    lookback_pending = false, rev = rev + 1, updated_at = @updated_at
WHERE id = @id AND rev = @rev AND NOT terminal;

-- name: DeleteRunSubscriptions :exec
DELETE FROM /* TEMPLATE: schema */subscriptions
WHERE run_id = @run_id;

-- name: UpdateRun :execrows
-- Advances a running run whose rev is still the one the caller read.
UPDATE /* TEMPLATE: schema */runs
SET frames = @frames, facts = @facts, rev = rev + 1, updated_at = @updated_at
WHERE id = @id AND rev = @rev AND state = 'running';

-- name: EnterWait :one
-- Parks a running run on a wait step, opening the next wait sequence. The
-- look-back that follows the commit clears lookback_pending.
UPDATE /* TEMPLATE: schema */runs
SET state = 'waiting', wait_step = @wait_step, wait_seq = wait_seq + 1, lookback_pending = true,
    frames = @frames, facts = @facts, rev = rev + 1, updated_at = @updated_at
WHERE id = @id AND rev = @rev AND state = 'running'
RETURNING wait_seq;

-- name: ClearLookback :execrows
-- Records that the look-back of a wait found nothing. It leaves rev alone,
-- so a claim racing with it is not refused.
UPDATE /* TEMPLATE: schema */runs
SET lookback_pending = false
WHERE id = @id AND state = 'waiting' AND wait_seq = @wait_seq AND lookback_pending;

-- name: ClaimRun :execrows
-- Resumes a waiting run. Exactly one of the event and timeout paths wins a
-- wait: the loser finds the state, sequence or rev changed.
UPDATE /* TEMPLATE: schema */runs
SET state = 'running', wait_step = NULL, lookback_pending = false, frames = @frames,
    rev = rev + 1, updated_at = @updated_at
WHERE id = @id AND state = 'waiting' AND wait_step = @wait_step AND wait_seq = @wait_seq AND rev = @rev;

-- name: InsertSubscription :exec
INSERT INTO /* TEMPLATE: schema */subscriptions (run_id, wait_seq, step_id, event_type, match_cel, created_at)
VALUES (@run_id, @wait_seq, @step_id, @event_type, @match_cel, @created_at);

-- name: DeleteSubscription :exec
DELETE FROM /* TEMPLATE: schema */subscriptions
WHERE run_id = @run_id AND wait_seq = @wait_seq;

-- name: LookbackPage :many
-- One keyset page of the events a wait may claim, after the cursor
-- (after_at, after_id) in (inserted_at, id) order. The caller applies the
-- rest of the eligibility rule.
SELECT e.id, e.type, e.dedup_key, e.payload, e.occurred_at, e.inserted_at
FROM /* TEMPLATE: schema */domain_events e
WHERE e.type = @event_type
  AND (e.inserted_at > @after_at OR (e.inserted_at = @after_at AND e.id > @after_id))
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */consumed_events c
    WHERE c.run_id = @run_id AND c.event_id = e.id
  )
ORDER BY e.inserted_at, e.id
LIMIT @page_size;

-- name: ListEventSubscriptions :many
-- One page, in run id order after the cursor, of the open waits an event may
-- satisfy: the run still waits on the subscribed sequence and has not
-- consumed the event. It reads only the run columns a match and a claim
-- need, not the pinned definition or the facts. The caller applies the rest
-- of the eligibility rule.
SELECT s.run_id, s.wait_seq, s.step_id, s.event_type, s.created_at AS subscribed_at,
       r.def_name, r.def_version, r.trigger_event_id, r.state, r.entity_kind, r.entity_id,
       r.scope_extra, r.serial_key, r.exclusive, r.wait_step, r.frames, r.rev, r.created_at
FROM /* TEMPLATE: schema */subscriptions s
JOIN /* TEMPLATE: schema */runs r ON r.id = s.run_id
WHERE s.event_type = @event_type AND s.run_id > @after_run_id
  AND r.state = 'waiting' AND r.wait_seq = s.wait_seq AND r.wait_step = s.step_id
  AND NOT EXISTS (
    SELECT 1 FROM /* TEMPLATE: schema */consumed_events c
    WHERE c.run_id = r.id AND c.event_id = @event_id
  )
ORDER BY s.run_id
LIMIT @page_size;
