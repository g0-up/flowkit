-- name: InsertDefinitionIfAbsent :execrows
INSERT INTO /* TEMPLATE: schema */definitions
  (name, enabled, current_version, trigger_event_type, concurrency_policy,
   serial_key_cel, dedup_window_seconds, created_at, updated_at)
VALUES (@name, @enabled, sqlc.narg(current_version), sqlc.narg(trigger_event_type), @concurrency_policy,
        sqlc.narg(serial_key_cel), sqlc.narg(dedup_window_seconds), @created_at, @updated_at)
ON CONFLICT (name) DO NOTHING;

-- name: GetDefinition :one
SELECT name, enabled, current_version, trigger_event_type, concurrency_policy,
       serial_key_cel, dedup_window_seconds, created_at, updated_at
FROM /* TEMPLATE: schema */definitions
WHERE name = @name;

-- name: LockDefinition :one
SELECT name, enabled, current_version, trigger_event_type, concurrency_policy,
       serial_key_cel, dedup_window_seconds, created_at, updated_at
FROM /* TEMPLATE: schema */definitions
WHERE name = @name
FOR UPDATE;

-- name: UpdateDefinitionPublished :execrows
UPDATE /* TEMPLATE: schema */definitions
SET current_version = @current_version,
    trigger_event_type = sqlc.narg(trigger_event_type),
    concurrency_policy = @concurrency_policy,
    serial_key_cel = sqlc.narg(serial_key_cel),
    dedup_window_seconds = sqlc.narg(dedup_window_seconds),
    updated_at = @updated_at
WHERE name = @name;

-- name: SetDefinitionEnabled :execrows
UPDATE /* TEMPLATE: schema */definitions
SET enabled = @enabled, updated_at = @updated_at
WHERE name = @name;

-- name: InsertDraft :exec
INSERT INTO /* TEMPLATE: schema */drafts (id, name, body, metamodel_version, created_by, created_at)
VALUES (@id, @name, @body, @metamodel_version, sqlc.narg(created_by), @created_at);

-- name: GetDraft :one
SELECT id, name, body, metamodel_version, created_by, created_at
FROM /* TEMPLATE: schema */drafts
WHERE id = @id;

-- name: InsertDefinitionVersion :exec
INSERT INTO /* TEMPLATE: schema */definition_versions
  (name, version, body, metamodel_version, checksum, published_by, note, published_at)
VALUES (@name, @version, @body, @metamodel_version, @checksum, sqlc.narg(published_by), sqlc.narg(note), @published_at);

-- name: GetDefinitionVersion :one
SELECT name, version, body, metamodel_version, checksum, published_by, note, published_at
FROM /* TEMPLATE: schema */definition_versions
WHERE name = @name AND version = @version;

-- name: InsertComponentRefs :exec
-- kinds and refs are parallel arrays; a definition may use the same
-- component more than once.
INSERT INTO /* TEMPLATE: schema */component_refs (def_name, def_version, kind, ref)
SELECT @def_name::text, @def_version::int, unnest(@kinds::text[]), unnest(@refs::text[])
ON CONFLICT DO NOTHING;

-- name: ListComponentUsers :many
SELECT def_name, def_version
FROM /* TEMPLATE: schema */component_refs
WHERE kind = @kind AND ref = @ref
ORDER BY def_name, def_version;
