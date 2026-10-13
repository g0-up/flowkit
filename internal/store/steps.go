package store

import (
	"context"

	"github.com/g0-up/flowkit/internal/ids"
	"github.com/g0-up/flowkit/internal/store/db"
)

func (s *PgStore) NextStepSeq(ctx context.Context, tx Tx, runID ids.UUID) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	seq, err := q.NextStepSeq(ctx, runID)
	if err != nil {
		return 0, wrapErr("next step seq", err)
	}
	return int(seq), nil
}

func (s *PgStore) AppendStep(ctx context.Context, tx Tx, st StepRow) (int, error) {
	q, err := s.q(tx)
	if err != nil {
		return 0, err
	}
	attempt, err := toInt32Ptr("attempt", st.Attempt)
	if err != nil {
		return 0, err
	}
	seq, err := s.NextStepSeq(ctx, tx, st.RunID)
	if err != nil {
		return 0, err
	}
	seq32, err := toInt32("seq", seq)
	if err != nil {
		return 0, err
	}
	err = q.InsertStep(ctx, db.InsertStepParams{
		RunID:    st.RunID,
		Seq:      seq32,
		At:       st.At,
		StepID:   st.StepID,
		Kind:     st.Kind,
		Detail:   st.Detail,
		Snapshot: st.Snapshot,
		Attempt:  attempt,
		Error:    st.Error,
	})
	if err != nil {
		return 0, wrapErr("append step", err)
	}
	return seq, nil
}

func (s *PgStore) ListSteps(ctx context.Context, tx Tx, runID ids.UUID) ([]StepRow, error) {
	q, err := s.q(tx)
	if err != nil {
		return nil, err
	}
	rows, err := q.ListSteps(ctx, runID)
	if err != nil {
		return nil, wrapErr("list steps", err)
	}
	out := make([]StepRow, len(rows))
	for i, r := range rows {
		out[i] = StepRow{
			RunID:    r.RunID,
			Seq:      int(r.Seq),
			At:       r.At,
			StepID:   r.StepID,
			Kind:     r.Kind,
			Detail:   r.Detail,
			Snapshot: r.Snapshot,
			Attempt:  intPtr(r.Attempt),
			Error:    r.Error,
		}
	}
	return out, nil
}

func (s *PgStore) StepRecorded(ctx context.Context, tx Tx, runID ids.UUID, stepID, kind, detail string) (bool, error) {
	q, err := s.q(tx)
	if err != nil {
		return false, err
	}
	ok, err := q.StepRecorded(ctx, db.StepRecordedParams{RunID: runID, StepID: stepID, Kind: kind, Detail: &detail})
	if err != nil {
		return false, wrapErr("step recorded", err)
	}
	return ok, nil
}
