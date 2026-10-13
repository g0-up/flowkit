-- flowkit migration 0001: initial schema.
-- min_lib_schema: 1
--
-- Rules for every flowkit migration: table names carry the schema token; no
-- functions, procedures, triggers, rules, enum or domain types, DO blocks,
-- defaults computed by functions, ON DELETE CASCADE, CONCURRENTLY or
-- partitioning. Timestamps and UUIDs come from the application.

-- The runner creates flowkit_meta before it takes the migration lock; this
-- statement keeps the table in the DDL that sqlc reads.
-- Keys: schema_version, min_lib_schema, migrate_lock.
CREATE TABLE IF NOT EXISTS /* TEMPLATE: schema */flowkit_meta (k text PRIMARY KEY, v text NOT NULL);

CREATE TABLE /* TEMPLATE: schema */domain_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  type text NOT NULL, dedup_key text,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX de_dedup ON /* TEMPLATE: schema */domain_events(type, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX de_type_inserted ON /* TEMPLATE: schema */domain_events(type, inserted_at, id);
CREATE INDEX de_inserted ON /* TEMPLATE: schema */domain_events(inserted_at);

CREATE TABLE /* TEMPLATE: schema */definitions (
  name text PRIMARY KEY,
  enabled bool NOT NULL,
  current_version int,
  trigger_event_type text,
  concurrency_policy text NOT NULL CHECK (concurrency_policy IN ('concurrent','serial','cancel_previous','dedup_window')),
  serial_key_cel text, dedup_window_seconds int,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE INDEX defs_trigger ON /* TEMPLATE: schema */definitions(trigger_event_type) WHERE enabled;

CREATE TABLE /* TEMPLATE: schema */drafts (
  id uuid PRIMARY KEY, name text NOT NULL,
  body jsonb NOT NULL, metamodel_version int NOT NULL,
  created_by text, created_at timestamptz NOT NULL
);

CREATE TABLE /* TEMPLATE: schema */definition_versions (
  name text NOT NULL REFERENCES /* TEMPLATE: schema */definitions(name), version int NOT NULL,
  body jsonb NOT NULL, metamodel_version int NOT NULL, checksum text NOT NULL,
  published_by text, note text, published_at timestamptz NOT NULL,
  PRIMARY KEY (name, version)
);

CREATE TABLE /* TEMPLATE: schema */component_refs (
  def_name text NOT NULL, def_version int NOT NULL,
  kind text NOT NULL CHECK (kind IN ('event','action','fact')), ref text NOT NULL,
  PRIMARY KEY (def_name, def_version, kind, ref)
);
CREATE INDEX cref_lookup ON /* TEMPLATE: schema */component_refs(kind, ref);

CREATE TABLE /* TEMPLATE: schema */runs (
  id uuid PRIMARY KEY,
  def_name text NOT NULL, def_version int NOT NULL,
  metamodel_version int NOT NULL, lib_version text NOT NULL,
  definition jsonb NOT NULL,
  trigger_event_id bigint NOT NULL REFERENCES /* TEMPLATE: schema */domain_events(id),
  state text NOT NULL CHECK (state IN ('running','waiting','completed','failed','cancelled')),
  terminal bool NOT NULL, terminal_name text,
  CHECK (terminal = (state IN ('completed','failed','cancelled'))),
  entity_kind text, entity_id text, scope_extra jsonb,
  serial_key text, exclusive bool NOT NULL,
  wait_step text, wait_seq int NOT NULL, lookback_pending bool NOT NULL,
  frames jsonb NOT NULL, facts jsonb NOT NULL,
  rev bigint NOT NULL, step_seq int NOT NULL,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX runs_serial_active ON /* TEMPLATE: schema */runs(def_name, serial_key) WHERE exclusive AND NOT terminal;
CREATE INDEX runs_dedup ON /* TEMPLATE: schema */runs(def_name, serial_key, created_at) WHERE serial_key IS NOT NULL;
CREATE INDEX runs_def_state ON /* TEMPLATE: schema */runs(def_name, state) WHERE NOT terminal;
CREATE INDEX runs_terminal_age ON /* TEMPLATE: schema */runs(updated_at) WHERE terminal;
CREATE INDEX runs_lookback ON /* TEMPLATE: schema */runs(updated_at) WHERE lookback_pending;
CREATE INDEX runs_entity ON /* TEMPLATE: schema */runs(entity_kind, entity_id, created_at) WHERE entity_kind IS NOT NULL;
CREATE INDEX runs_trigger ON /* TEMPLATE: schema */runs(trigger_event_id);
-- The keyset order of the run list, newest first, all runs or one definition's.
CREATE INDEX runs_started ON /* TEMPLATE: schema */runs(created_at DESC, id DESC);
CREATE INDEX runs_def_started ON /* TEMPLATE: schema */runs(def_name, created_at DESC, id DESC);

CREATE TABLE /* TEMPLATE: schema */run_steps (
  run_id uuid NOT NULL REFERENCES /* TEMPLATE: schema */runs(id), seq int NOT NULL, at timestamptz NOT NULL,
  step_id text NOT NULL, kind text NOT NULL,
  detail text, snapshot jsonb, attempt int, error text,
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE /* TEMPLATE: schema */subscriptions (
  run_id uuid NOT NULL REFERENCES /* TEMPLATE: schema */runs(id), wait_seq int NOT NULL,
  step_id text NOT NULL, event_type text NOT NULL, match_cel text NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (run_id, wait_seq)
);
CREATE INDEX subs_type ON /* TEMPLATE: schema */subscriptions(event_type);

-- step_id is '$trigger' for the event that started the run.
CREATE TABLE /* TEMPLATE: schema */consumed_events (
  run_id uuid NOT NULL REFERENCES /* TEMPLATE: schema */runs(id),
  event_id bigint NOT NULL REFERENCES /* TEMPLATE: schema */domain_events(id),
  step_id text NOT NULL, at timestamptz NOT NULL,
  PRIMARY KEY (run_id, event_id)
);
CREATE INDEX ce_event ON /* TEMPLATE: schema */consumed_events(event_id);

-- idem_key is runID:stepID:visit_n.
CREATE TABLE /* TEMPLATE: schema */executed_effects (
  idem_key text PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES /* TEMPLATE: schema */runs(id), step_id text NOT NULL, action text NOT NULL,
  attempt int NOT NULL, claimed_at timestamptz, claim_token uuid, done_at timestamptz,
  result jsonb, last_error text, in_doubt bool NOT NULL
);
CREATE INDEX effects_inflight ON /* TEMPLATE: schema */executed_effects(claimed_at) WHERE done_at IS NULL AND claimed_at IS NOT NULL AND NOT in_doubt;
CREATE INDEX effects_run ON /* TEMPLATE: schema */executed_effects(run_id);

-- ref is the dedup key of an open dead letter: idem_key, event_id or
-- event_id:def_name. No foreign key to runs: raw keeps enough context to read
-- the dead letter after the run is deleted.
CREATE TABLE /* TEMPLATE: schema */dead_letters (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('unroutable_event','schema_mismatch','effect_in_doubt','exhausted')),
  ref text NOT NULL,
  run_id uuid, event_id bigint, raw jsonb NOT NULL, note text,
  created_at timestamptz NOT NULL,
  resolved_at timestamptz,
  resolution text CHECK (resolution IN ('redrive','mark_done','skip')),
  resolved_by text, resolve_reason text
);
CREATE UNIQUE INDEX dl_open_ref ON /* TEMPLATE: schema */dead_letters(kind, ref) WHERE resolved_at IS NULL;
CREATE INDEX dl_resolved_age ON /* TEMPLATE: schema */dead_letters(resolved_at) WHERE resolved_at IS NOT NULL;

-- event_at is the trigger event's inserted_at: the queue runs in emit order.
CREATE TABLE /* TEMPLATE: schema */run_queue (
  def_name text NOT NULL, serial_key text NOT NULL,
  trigger_event_id bigint NOT NULL REFERENCES /* TEMPLATE: schema */domain_events(id),
  event_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (def_name, serial_key, trigger_event_id)
);
CREATE INDEX rq_pop ON /* TEMPLATE: schema */run_queue(def_name, serial_key, event_at, trigger_event_id);
CREATE INDEX rq_event ON /* TEMPLATE: schema */run_queue(trigger_event_id, def_name);

-- Rows are never deleted: the concurrency gate relies on the row existing.
CREATE TABLE /* TEMPLATE: schema */serial_locks (
  def_name text NOT NULL, serial_key text NOT NULL,
  PRIMARY KEY (def_name, serial_key)
);

CREATE TABLE /* TEMPLATE: schema */rate_limits (
  def_name text PRIMARY KEY,
  tokens double precision NOT NULL, refilled_at timestamptz NOT NULL
);

-- The only record of a terminal run left after step detail retention.
CREATE TABLE /* TEMPLATE: schema */run_summaries (
  run_id uuid PRIMARY KEY, def_name text NOT NULL, def_version int NOT NULL,
  state text NOT NULL, terminal_name text, step_count int NOT NULL,
  trigger_event_type text NOT NULL, entity_kind text, entity_id text,
  started_at timestamptz NOT NULL, ended_at timestamptz NOT NULL
);
CREATE INDEX rs_def_ended ON /* TEMPLATE: schema */run_summaries(def_name, ended_at);
CREATE INDEX rs_entity ON /* TEMPLATE: schema */run_summaries(entity_kind, entity_id, started_at) WHERE entity_kind IS NOT NULL;
-- The keyset order of the run list, as on runs.
CREATE INDEX rs_started ON /* TEMPLATE: schema */run_summaries(started_at DESC, run_id DESC);
CREATE INDEX rs_def_started ON /* TEMPLATE: schema */run_summaries(def_name, started_at DESC, run_id DESC);
