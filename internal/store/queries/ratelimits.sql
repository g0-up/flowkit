-- name: InsertRateLimit :exec
INSERT INTO /* TEMPLATE: schema */rate_limits (def_name, tokens, refilled_at)
VALUES (@def_name, @tokens, @refilled_at)
ON CONFLICT (def_name) DO NOTHING;

-- name: LockRateLimit :one
SELECT def_name, tokens, refilled_at
FROM /* TEMPLATE: schema */rate_limits
WHERE def_name = @def_name
FOR UPDATE;

-- name: UpdateRateLimit :exec
UPDATE /* TEMPLATE: schema */rate_limits
SET tokens = @tokens, refilled_at = @refilled_at
WHERE def_name = @def_name;
