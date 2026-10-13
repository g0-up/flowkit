-- name: InsertEvent :one
-- Returns no row when an event with the same (type, dedup_key) exists.
INSERT INTO /* TEMPLATE: schema */domain_events (type, dedup_key, payload, occurred_at, inserted_at)
VALUES (@type, sqlc.narg(dedup_key), @payload, @occurred_at, @inserted_at)
ON CONFLICT (type, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
RETURNING id;

-- name: GetEventIDByDedupKey :one
SELECT id FROM /* TEMPLATE: schema */domain_events
WHERE type = @type AND dedup_key = @dedup_key;

-- name: GetEvent :one
SELECT id, type, dedup_key, payload, occurred_at, inserted_at
FROM /* TEMPLATE: schema */domain_events
WHERE id = @id;
