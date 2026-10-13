package flowkit

import (
	"errors"
	"strings"
)

// Sentinel errors. Compare with errors.Is; flowkit wraps them with context.
var (
	// ErrDuplicateEvent is returned by Emit when an event with the same type
	// and dedup key was already stored. The caller's transaction is not
	// affected and may still commit. Deduplication holds for as long as the
	// earlier event is kept (RetentionConfig.Event).
	//
	// Under REPEATABLE READ or SERIALIZABLE, a duplicate that another
	// transaction commits after the caller's snapshot is taken surfaces
	// instead as a serialization failure (SQLSTATE 40001) that aborts the
	// caller's transaction. Retry the whole transaction; the retry gets
	// ErrDuplicateEvent.
	ErrDuplicateEvent = errors.New("flowkit: duplicate event")

	// ErrSchemaDrift is returned by New when the database schema cannot serve
	// this build: it is not migrated, needs a newer library, has objects that
	// do not belong to it, or the binary links an unsupported River version.
	// New never migrates; run `flowkit migrate` (or migrate.Run) first.
	ErrSchemaDrift = errors.New("flowkit: schema drift")

	// ErrDefinitionDisabled is returned when an operation needs an enabled
	// workflow definition and the definition is disabled.
	ErrDefinitionDisabled = errors.New("flowkit: definition disabled")

	// ErrNotFound is returned when a looked-up definition, run or dead letter
	// does not exist, or its detail was removed by retention.
	ErrNotFound = errors.New("flowkit: not found")

	// ErrUnknownEvent is returned by Emit when the event type is not
	// registered. The caller's transaction is not affected.
	ErrUnknownEvent = errors.New("flowkit: unknown event type")

	// ErrAlreadyResolved is returned when resolving a dead letter that is
	// already resolved.
	ErrAlreadyResolved = errors.New("flowkit: already resolved")
)

// ValidationError is one problem in a payload, definition or registration.
// Path is a JSON Pointer (RFC 6901) into the validated document; the empty
// string means the document itself.
type ValidationError struct {
	Path string
	Msg  string
}

// Error returns the path and the message.
func (e *ValidationError) Error() string {
	if e.Path == "" {
		return e.Msg
	}
	return e.Path + ": " + e.Msg
}

// ValidationErrors lists every problem found in one validation. A non-nil
// ValidationErrors is never empty. Match it with errors.As; Unwrap exposes
// each ValidationError to errors.As as well.
type ValidationErrors []*ValidationError

// Error joins the messages of all problems.
func (es ValidationErrors) Error() string {
	var b strings.Builder
	b.WriteString("flowkit: validation failed: ")
	for i, e := range es {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(e.Error())
	}
	return b.String()
}

// Unwrap returns each problem as an error.
func (es ValidationErrors) Unwrap() []error {
	out := make([]error, len(es))
	for i, e := range es {
		out[i] = e
	}
	return out
}
