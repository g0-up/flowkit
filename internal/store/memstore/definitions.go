package memstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store"
)

func (s *Store) InsertEvent(ctx context.Context, tx store.Tx, e store.EventRow) (int64, bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return 0, false, err
	}
	if err := jsonCol("payload", e.Payload, true); err != nil {
		return 0, false, t.fail("insert event", err)
	}
	if e.DedupKey != nil {
		for _, ev := range t.t.events {
			if ev.Type == e.Type && ev.DedupKey != nil && *ev.DedupKey == *e.DedupKey {
				return ev.ID, false, nil
			}
		}
	}
	// Like an identity column, the sequence does not roll back.
	s.lastEventID++
	row := store.EventRow{
		ID:         s.lastEventID,
		Type:       e.Type,
		DedupKey:   cp(e.DedupKey),
		Payload:    cpJSON(e.Payload),
		OccurredAt: ts(e.OccurredAt),
		InsertedAt: ts(e.InsertedAt),
	}
	write(t, tabEvents, &t.t.events)[row.ID] = row
	return row.ID, true, nil
}

func (s *Store) GetEvent(ctx context.Context, tx store.Tx, id int64) (store.EventRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.EventRow{}, err
	}
	e, ok := t.t.events[id]
	if !ok {
		return store.EventRow{}, notFound("get event")
	}
	return cloneEvent(e), nil
}

func cloneEvent(e store.EventRow) store.EventRow {
	e.DedupKey = cp(e.DedupKey)
	e.Payload = cpJSON(e.Payload)
	return e
}

// concurrencyPolicies are the values the definitions CHECK constraint
// allows.
var concurrencyPolicies = []string{"concurrent", "serial", "cancel_previous", "dedup_window"}

func (s *Store) InsertDefinitionIfAbsent(ctx context.Context, tx store.Tx, d store.DefinitionRow) (bool, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return false, err
	}
	if !slices.Contains(concurrencyPolicies, d.ConcurrencyPolicy) {
		return false, t.fail("insert definition", checkErr("definitions_concurrency_policy_check"))
	}
	if _, ok := t.t.definitions[d.Name]; ok {
		return false, nil
	}
	write(t, tabDefinitions, &t.t.definitions)[d.Name] = definitionRow(d)
	return true, nil
}

func definitionRow(d store.DefinitionRow) store.DefinitionRow {
	d.CurrentVersion = cp(d.CurrentVersion)
	d.TriggerEventType = cp(d.TriggerEventType)
	d.SerialKeyCEL = cp(d.SerialKeyCEL)
	d.DedupWindowSeconds = cp(d.DedupWindowSeconds)
	d.CreatedAt = ts(d.CreatedAt)
	d.UpdatedAt = ts(d.UpdatedAt)
	return d
}

func (s *Store) GetDefinition(ctx context.Context, tx store.Tx, name string) (store.DefinitionRow, error) {
	return s.getDefinition(ctx, tx, "get definition", name)
}

// LockDefinition reads the definition; the transaction already excludes
// every other one.
func (s *Store) LockDefinition(ctx context.Context, tx store.Tx, name string) (store.DefinitionRow, error) {
	return s.getDefinition(ctx, tx, "lock definition", name)
}

func (s *Store) getDefinition(ctx context.Context, tx store.Tx, op, name string) (store.DefinitionRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.DefinitionRow{}, err
	}
	d, ok := t.t.definitions[name]
	if !ok {
		return store.DefinitionRow{}, notFound(op)
	}
	return definitionRow(d), nil
}

var errNilCurrentVersion = errors.New("current version is required")

func (s *Store) UpdateDefinitionPublished(ctx context.Context, tx store.Tx, d store.DefinitionRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if d.CurrentVersion == nil {
		return fmt.Errorf("flowkit store: update published definition: %w", errNilCurrentVersion)
	}
	cur, ok := t.t.definitions[d.Name]
	if !ok {
		return notFound("update published definition")
	}
	if !slices.Contains(concurrencyPolicies, d.ConcurrencyPolicy) {
		return t.fail("update published definition", checkErr("definitions_concurrency_policy_check"))
	}
	cur.CurrentVersion = d.CurrentVersion
	cur.TriggerEventType = d.TriggerEventType
	cur.ConcurrencyPolicy = d.ConcurrencyPolicy
	cur.SerialKeyCEL = d.SerialKeyCEL
	cur.DedupWindowSeconds = d.DedupWindowSeconds
	cur.UpdatedAt = d.UpdatedAt
	write(t, tabDefinitions, &t.t.definitions)[d.Name] = definitionRow(cur)
	return nil
}

func (s *Store) SetDefinitionEnabled(ctx context.Context, tx store.Tx, name string, enabled bool, at time.Time) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	d, ok := t.t.definitions[name]
	if !ok {
		return notFound("set definition enabled")
	}
	d.Enabled, d.UpdatedAt = enabled, ts(at)
	write(t, tabDefinitions, &t.t.definitions)[name] = d
	return nil
}

func (s *Store) InsertDraft(ctx context.Context, tx store.Tx, d store.DraftRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if err := jsonCol("body", d.Body, true); err != nil {
		return t.fail("insert draft", err)
	}
	if _, ok := t.t.drafts[d.ID]; ok {
		return t.fail("insert draft", uniqueErr("drafts_pkey"))
	}
	write(t, tabDrafts, &t.t.drafts)[d.ID] = draftRow(d)
	return nil
}

func draftRow(d store.DraftRow) store.DraftRow {
	d.Body = cpJSON(d.Body)
	d.CreatedBy = cp(d.CreatedBy)
	d.CreatedAt = ts(d.CreatedAt)
	return d
}

func (s *Store) GetDraft(ctx context.Context, tx store.Tx, id ids.UUID) (store.DraftRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.DraftRow{}, err
	}
	d, ok := t.t.drafts[id]
	if !ok {
		return store.DraftRow{}, notFound("get draft")
	}
	return draftRow(d), nil
}

func (s *Store) InsertDefinitionVersion(ctx context.Context, tx store.Tx, v store.DefinitionVersionRow) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if err := jsonCol("body", v.Body, true); err != nil {
		return t.fail("insert definition version", err)
	}
	key := store.DefinitionRef{Name: v.Name, Version: v.Version}
	if _, ok := t.t.versions[key]; ok {
		return t.fail("insert definition version", uniqueErr("definition_versions_pkey"))
	}
	if _, ok := t.t.definitions[v.Name]; !ok {
		return t.fail("insert definition version", foreignKeyErr("definition_versions_name_fkey"))
	}
	write(t, tabVersions, &t.t.versions)[key] = versionRow(v)
	return nil
}

func versionRow(v store.DefinitionVersionRow) store.DefinitionVersionRow {
	v.Body = cpJSON(v.Body)
	v.PublishedBy = cp(v.PublishedBy)
	v.Note = cp(v.Note)
	v.PublishedAt = ts(v.PublishedAt)
	return v
}

func (s *Store) GetDefinitionVersion(ctx context.Context, tx store.Tx, name string, version int) (store.DefinitionVersionRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return store.DefinitionVersionRow{}, err
	}
	v, ok := t.t.versions[store.DefinitionRef{Name: name, Version: version}]
	if !ok {
		return store.DefinitionVersionRow{}, notFound("get definition version")
	}
	return versionRow(v), nil
}

// componentKinds are the values the component_refs CHECK constraint allows.
var componentKinds = []string{"event", "action", "fact"}

func (s *Store) InsertComponentRefs(ctx context.Context, tx store.Tx, name string, version int, refs []store.ComponentRef) error {
	t, err := s.use(ctx, tx)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	for _, r := range refs {
		if !slices.Contains(componentKinds, r.Kind) {
			return t.fail("insert component refs", checkErr("component_refs_kind_check"))
		}
	}
	m := write(t, tabRefs, &t.t.refs)
	for _, r := range refs {
		m[componentRefKey{def: store.DefinitionRef{Name: name, Version: version}, ref: r}] = struct{}{}
	}
	return nil
}

func (s *Store) ListComponentUsers(ctx context.Context, tx store.Tx, ref store.ComponentRef) ([]store.DefinitionRef, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []store.DefinitionRef{}
	for k := range t.t.refs {
		if k.ref == ref {
			out = append(out, k.def)
		}
	}
	slices.SortFunc(out, func(a, b store.DefinitionRef) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Version, b.Version))
	})
	return out, nil
}

func (s *Store) ListTriggeredDefinitions(ctx context.Context, tx store.Tx, eventType string) ([]store.DefinitionRow, error) {
	t, err := s.use(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := []store.DefinitionRow{}
	for _, d := range t.t.definitions {
		if d.Enabled && d.TriggerEventType != nil && *d.TriggerEventType == eventType && d.CurrentVersion != nil {
			out = append(out, definitionRow(d))
		}
	}
	slices.SortFunc(out, func(a, b store.DefinitionRow) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}
