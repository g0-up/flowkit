package store

import (
	"context"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) InsertRun(ctx context.Context, tx Tx, r RunRow) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	var p db.InsertRunParams
	if p.DefVersion, err = toInt32("def_version", r.DefVersion); err != nil {
		return err
	}
	if p.MetamodelVersion, err = toInt32("metamodel_version", r.MetamodelVersion); err != nil {
		return err
	}
	if p.WaitSeq, err = toInt32("wait_seq", r.WaitSeq); err != nil {
		return err
	}
	if p.StepSeq, err = toInt32("step_seq", r.StepSeq); err != nil {
		return err
	}
	p.ID = r.ID
	p.DefName = r.DefName
	p.LibVersion = r.LibVersion
	p.Definition = r.Definition
	p.TriggerEventID = r.TriggerEventID
	p.State = r.State
	p.Terminal = r.Terminal
	p.TerminalName = r.TerminalName
	p.EntityKind = r.EntityKind
	p.EntityID = r.EntityID
	p.ScopeExtra = r.ScopeExtra
	p.SerialKey = r.SerialKey
	p.Exclusive = r.Exclusive
	p.WaitStep = r.WaitStep
	p.LookbackPending = r.LookbackPending
	p.Frames = r.Frames
	p.Facts = r.Facts
	p.Rev = r.Rev
	p.CreatedAt = r.CreatedAt
	p.UpdatedAt = r.UpdatedAt
	if err := q.InsertRun(ctx, p); err != nil {
		return wrapErr("insert run", err)
	}
	return nil
}

func (s *PgStore) GetRun(ctx context.Context, tx Tx, id ids.UUID) (RunRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return RunRow{}, err
	}
	r, err := q.GetRun(ctx, id)
	if err != nil {
		return RunRow{}, wrapErr("get run", err)
	}
	return runRow(r), nil
}

func runRow(r db.Run) RunRow {
	return RunRow{
		ID:               r.ID,
		DefName:          r.DefName,
		DefVersion:       int(r.DefVersion),
		MetamodelVersion: int(r.MetamodelVersion),
		LibVersion:       r.LibVersion,
		Definition:       r.Definition,
		TriggerEventID:   r.TriggerEventID,
		State:            r.State,
		Terminal:         r.Terminal,
		TerminalName:     r.TerminalName,
		EntityKind:       r.EntityKind,
		EntityID:         r.EntityID,
		ScopeExtra:       r.ScopeExtra,
		SerialKey:        r.SerialKey,
		Exclusive:        r.Exclusive,
		WaitStep:         r.WaitStep,
		WaitSeq:          int(r.WaitSeq),
		LookbackPending:  r.LookbackPending,
		Frames:           r.Frames,
		Facts:            r.Facts,
		Rev:              r.Rev,
		StepSeq:          int(r.StepSeq),
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
	}
}

func (s *PgStore) ListRunningRunIDs(ctx context.Context, tx Tx, defName string) ([]ids.UUID, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	out, err := q.ListRunningRunIDs(ctx, defName)
	if err != nil {
		return nil, wrapErr("list running runs", err)
	}
	return out, nil
}

func (s *PgStore) CountPinnedRuns(ctx context.Context, tx Tx, defName string) (map[int]int, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	rows, err := q.CountPinnedRuns(ctx, defName)
	if err != nil {
		return nil, wrapErr("count pinned runs", err)
	}
	out := make(map[int]int, len(rows))
	for _, r := range rows {
		out[int(r.DefVersion)] = int(r.RunCount)
	}
	return out, nil
}

func (s *PgStore) FinishRun(ctx context.Context, tx Tx, f FinishRunRow) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	n, err := q.FinishRun(ctx, db.FinishRunParams{
		ID:           f.ID,
		Rev:          f.Rev,
		State:        f.State,
		TerminalName: &f.TerminalName,
		UpdatedAt:    f.At,
	})
	if err != nil {
		return false, wrapErr("finish run", err)
	}
	return n == 1, nil
}

func (s *PgStore) DeleteRunSubscriptions(ctx context.Context, tx Tx, runID ids.UUID) error {
	q, err := s.q(tx)
	if err != nil {
		return err
	}
	if err := q.DeleteRunSubscriptions(ctx, runID); err != nil {
		return wrapErr("delete run subscriptions", err)
	}
	return nil
}
