-- name: GetEffect :one
SELECT idem_key, run_id, step_id, action, attempt, claimed_at, claim_token, done_at,
       result, last_error, in_doubt
FROM /* TEMPLATE: schema */executed_effects
WHERE idem_key = @idem_key;

-- name: InsertEffectClaim :execrows
-- Affects no row when the effect already exists.
INSERT INTO /* TEMPLATE: schema */executed_effects
  (idem_key, run_id, step_id, action, attempt, claimed_at, claim_token, in_doubt)
VALUES (@idem_key, @run_id, @step_id, @action, 0, @claimed_at, @claim_token, false)
ON CONFLICT (idem_key) DO NOTHING;

-- name: ReclaimEffect :execrows
-- The attempt bound is checked under the row lock, so a failure recorded
-- after the caller read the effect cannot let it exceed max_attempts.
UPDATE /* TEMPLATE: schema */executed_effects
SET claimed_at = @claimed_at, claim_token = @claim_token
WHERE idem_key = @idem_key AND claimed_at IS NULL AND done_at IS NULL AND NOT in_doubt
  AND attempt < @max_attempts;

-- name: CompleteEffect :execrows
UPDATE /* TEMPLATE: schema */executed_effects
SET done_at = @done_at, result = sqlc.narg(result), last_error = sqlc.narg(last_error)
WHERE idem_key = @idem_key AND claim_token = @claim_token AND done_at IS NULL AND NOT in_doubt;

-- name: FailEffect :one
UPDATE /* TEMPLATE: schema */executed_effects
SET attempt = attempt + 1, last_error = @last_error, claimed_at = NULL, claim_token = NULL
WHERE idem_key = @idem_key AND claim_token = @claim_token AND done_at IS NULL AND NOT in_doubt
RETURNING attempt;

-- name: MarkEffectInDoubt :one
UPDATE /* TEMPLATE: schema */executed_effects
SET in_doubt = true, last_error = @last_error, claim_token = NULL
WHERE idem_key = @idem_key AND claim_token = @claim_token AND done_at IS NULL AND NOT in_doubt
RETURNING attempt;

-- name: ReleaseEffectClaim :execrows
UPDATE /* TEMPLATE: schema */executed_effects
SET claimed_at = NULL, claim_token = NULL
WHERE idem_key = @idem_key AND claim_token = @claim_token AND done_at IS NULL AND NOT in_doubt;

-- name: ListExpiredClaims :many
SELECT idem_key, run_id, step_id, action, attempt, claimed_at, claim_token, done_at,
       result, last_error, in_doubt
FROM /* TEMPLATE: schema */executed_effects
WHERE claimed_at < @claimed_before AND claim_token IS NOT NULL AND done_at IS NULL AND NOT in_doubt
ORDER BY claimed_at, idem_key
LIMIT @max_rows;

-- name: SetInDoubtEffectResult :execrows
-- Keeps the output of an in-doubt effect that turned out to have succeeded.
-- The effect stays in doubt for an operator; the first output kept wins.
UPDATE /* TEMPLATE: schema */executed_effects
SET result = @result
WHERE idem_key = @idem_key AND in_doubt AND done_at IS NULL AND result IS NULL;

-- name: AppendInDoubtEffectNote :execrows
-- Adds a line to the note of the open dead letter of an in-doubt effect.
UPDATE /* TEMPLATE: schema */dead_letters
SET note = concat_ws(E'\n', nullif(note, ''), @note::text)
WHERE kind = 'effect_in_doubt' AND ref = @idem_key AND resolved_at IS NULL;

-- name: RedriveInDoubtEffect :execrows
-- Releases an in-doubt effect so the next request runs the action again.
-- An effect that kept the output of a late success is not released: the
-- action is known to have run, so running it again would repeat it.
UPDATE /* TEMPLATE: schema */executed_effects
SET in_doubt = false, claimed_at = NULL, claim_token = NULL
WHERE idem_key = @idem_key AND in_doubt AND done_at IS NULL AND result IS NULL;

-- name: MarkInDoubtEffectDone :execrows
-- Records the output an operator asserts for an in-doubt effect. The next
-- request returns it as the effect's cached result.
UPDATE /* TEMPLATE: schema */executed_effects
SET done_at = @done_at, result = @result
WHERE idem_key = @idem_key AND in_doubt AND done_at IS NULL;
