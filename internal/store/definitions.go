package store

import (
	"context"
	"errors"
	"time"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

// errNilCurrentVersion rejects a publish update without a version.
var errNilCurrentVersion = errors.New("current_version is required")

func definitionRow(r db.Definition) DefinitionRow {
	return DefinitionRow{
		Name:               r.Name,
		Enabled:            r.Enabled,
		CurrentVersion:     intPtr(r.CurrentVersion),
		TriggerEventType:   r.TriggerEventType,
		ConcurrencyPolicy:  r.ConcurrencyPolicy,
		SerialKeyCEL:       r.SerialKeyCel,
		DedupWindowSeconds: intPtr(r.DedupWindowSeconds),
		CreatedAt:          r.CreatedAt,
		UpdatedAt:          r.UpdatedAt,
	}
}

func (s *PgStore) InsertDefinitionIfAbsent(ctx context.Context, tx Tx, d DefinitionRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	current, err := toInt32Ptr("current_version", d.CurrentVersion)
	if err != nil {
		return false, err
	}
	window, err := toInt32Ptr("dedup_window_seconds", d.DedupWindowSeconds)
	if err != nil {
		return false, err
	}
	n, err := q.InsertDefinitionIfAbsent(ctx, db.InsertDefinitionIfAbsentParams{
		Name:               d.Name,
		Enabled:            d.Enabled,
		CurrentVersion:     current,
		TriggerEventType:   d.TriggerEventType,
		ConcurrencyPolicy:  d.ConcurrencyPolicy,
		SerialKeyCel:       d.SerialKeyCEL,
		DedupWindowSeconds: window,
		CreatedAt:          d.CreatedAt,
		UpdatedAt:          d.UpdatedAt,
	})
	if err != nil {
		return false, wrapErr("insert definition", err)
	}
	return n == 1, nil
}

func (s *PgStore) GetDefinition(ctx context.Context, tx Tx, name string) (DefinitionRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return DefinitionRow{}, err
	}
	r, err := q.GetDefinition(ctx, name)
	if err != nil {
		return DefinitionRow{}, wrapErr("get definition", err)
	}
	return definitionRow(r), nil
}

func (s *PgStore) LockDefinition(ctx context.Context, tx Tx, name string) (DefinitionRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return DefinitionRow{}, err
	}
	r, err := q.LockDefinition(ctx, name)
	if err != nil {
		return DefinitionRow{}, wrapErr("lock definition", err)
	}
	return definitionRow(r), nil
}

func (s *PgStore) UpdateDefinitionPublished(ctx context.Context, tx Tx, d DefinitionRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	if d.CurrentVersion == nil {
		return wrapErr("update published definition", errNilCurrentVersion)
	}
	current, err := toInt32("current_version", *d.CurrentVersion)
	if err != nil {
		return err
	}
	window, err := toInt32Ptr("dedup_window_seconds", d.DedupWindowSeconds)
	if err != nil {
		return err
	}
	n, err := q.UpdateDefinitionPublished(ctx, db.UpdateDefinitionPublishedParams{
		Name:               d.Name,
		CurrentVersion:     &current,
		TriggerEventType:   d.TriggerEventType,
		ConcurrencyPolicy:  d.ConcurrencyPolicy,
		SerialKeyCel:       d.SerialKeyCEL,
		DedupWindowSeconds: window,
		UpdatedAt:          d.UpdatedAt,
	})
	if err != nil {
		return wrapErr("update published definition", err)
	}
	if n == 0 {
		return wrapErr("update published definition", ErrNotFound)
	}
	return nil
}

func (s *PgStore) SetDefinitionEnabled(ctx context.Context, tx Tx, name string, enabled bool, at time.Time) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	n, err := q.SetDefinitionEnabled(ctx, db.SetDefinitionEnabledParams{Name: name, Enabled: enabled, UpdatedAt: at})
	if err != nil {
		return wrapErr("set definition enabled", err)
	}
	if n == 0 {
		return wrapErr("set definition enabled", ErrNotFound)
	}
	return nil
}

func (s *PgStore) InsertDraft(ctx context.Context, tx Tx, d DraftRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	mm, err := toInt32("metamodel_version", d.MetamodelVersion)
	if err != nil {
		return err
	}
	err = q.InsertDraft(ctx, db.InsertDraftParams{
		ID:               d.ID,
		Name:             d.Name,
		Body:             d.Body,
		MetamodelVersion: mm,
		CreatedBy:        d.CreatedBy,
		CreatedAt:        d.CreatedAt,
	})
	if err != nil {
		return wrapErr("insert draft", err)
	}
	return nil
}

func (s *PgStore) GetDraft(ctx context.Context, tx Tx, id ids.UUID) (DraftRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return DraftRow{}, err
	}
	r, err := q.GetDraft(ctx, id)
	if err != nil {
		return DraftRow{}, wrapErr("get draft", err)
	}
	return DraftRow{
		ID:               r.ID,
		Name:             r.Name,
		Body:             r.Body,
		MetamodelVersion: int(r.MetamodelVersion),
		CreatedBy:        r.CreatedBy,
		CreatedAt:        r.CreatedAt,
	}, nil
}

func (s *PgStore) InsertDefinitionVersion(ctx context.Context, tx Tx, v DefinitionVersionRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	version, err := toInt32("version", v.Version)
	if err != nil {
		return err
	}
	mm, err := toInt32("metamodel_version", v.MetamodelVersion)
	if err != nil {
		return err
	}
	err = q.InsertDefinitionVersion(ctx, db.InsertDefinitionVersionParams{
		Name:             v.Name,
		Version:          version,
		Body:             v.Body,
		MetamodelVersion: mm,
		Checksum:         v.Checksum,
		PublishedBy:      v.PublishedBy,
		Note:             v.Note,
		PublishedAt:      v.PublishedAt,
	})
	if err != nil {
		return wrapErr("insert definition version", err)
	}
	return nil
}

func (s *PgStore) GetDefinitionVersion(ctx context.Context, tx Tx, name string, version int) (DefinitionVersionRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return DefinitionVersionRow{}, err
	}
	v, err := toInt32("version", version)
	if err != nil {
		return DefinitionVersionRow{}, err
	}
	r, err := q.GetDefinitionVersion(ctx, db.GetDefinitionVersionParams{Name: name, Version: v})
	if err != nil {
		return DefinitionVersionRow{}, wrapErr("get definition version", err)
	}
	return DefinitionVersionRow{
		Name:             r.Name,
		Version:          int(r.Version),
		Body:             r.Body,
		MetamodelVersion: int(r.MetamodelVersion),
		Checksum:         r.Checksum,
		PublishedBy:      r.PublishedBy,
		Note:             r.Note,
		PublishedAt:      r.PublishedAt,
	}, nil
}

func (s *PgStore) InsertComponentRefs(ctx context.Context, tx Tx, name string, version int, refs []ComponentRef) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	v, err := toInt32("version", version)
	if err != nil {
		return err
	}
	kinds := make([]string, len(refs))
	values := make([]string, len(refs))
	for i, r := range refs {
		kinds[i], values[i] = r.Kind, r.Ref
	}
	err = q.InsertComponentRefs(ctx, db.InsertComponentRefsParams{DefName: name, DefVersion: v, Kinds: kinds, Refs: values})
	if err != nil {
		return wrapErr("insert component refs", err)
	}
	return nil
}

func (s *PgStore) ListComponentUsers(ctx context.Context, tx Tx, ref ComponentRef) ([]DefinitionRef, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListComponentUsers(ctx, db.ListComponentUsersParams{Kind: ref.Kind, Ref: ref.Ref})
	if err != nil {
		return nil, wrapErr("list component users", err)
	}
	out := make([]DefinitionRef, len(rows))
	for i, r := range rows {
		out[i] = DefinitionRef{Name: r.DefName, Version: int(r.DefVersion)}
	}
	return out, nil
}
