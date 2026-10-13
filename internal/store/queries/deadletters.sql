-- name: InsertDeadLetter :one
-- Returns no row when an open dead letter with the same (kind, ref) exists.
INSERT INTO /* TEMPLATE: schema */dead_letters (kind, ref, run_id, event_id, raw, note, created_at)
VALUES (@kind, @ref, sqlc.narg(run_id), sqlc.narg(event_id), @raw, sqlc.narg(note), @created_at)
ON CONFLICT (kind, ref) WHERE resolved_at IS NULL DO NOTHING
RETURNING id;

-- name: GetOpenDeadLetterID :one
SELECT id FROM /* TEMPLATE: schema */dead_letters
WHERE kind = @kind AND ref = @ref AND resolved_at IS NULL;

-- name: GetDeadLetter :one
SELECT id, kind, ref, run_id, event_id, raw, note, created_at,
       resolved_at, resolution, resolved_by, resolve_reason
FROM /* TEMPLATE: schema */dead_letters
WHERE id = @id;

-- name: ClaimDeadLetter :one
-- Resolves an open dead letter. Concurrent resolutions of one dead letter
-- queue on its row; all but the first find it resolved and get no row.
UPDATE /* TEMPLATE: schema */dead_letters
SET resolved_at = @resolved_at, resolution = @resolution, resolved_by = sqlc.narg(resolved_by),
    resolve_reason = sqlc.narg(resolve_reason)
WHERE id = @id AND resolved_at IS NULL
RETURNING id, kind, ref, run_id, event_id, raw, note, created_at,
          resolved_at, resolution, resolved_by, resolve_reason;
