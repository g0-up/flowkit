package executor

// actionError reports an action's error in an outcome. Its text is the
// redacted text the executor stores, never the raw error; a fresh one wraps
// the raw error for errors.Is and errors.As.
type actionError struct {
	text string
	err  error // nil when the text was read back from the store
}

func (e *actionError) Error() string { return e.text }
func (e *actionError) Unwrap() error { return e.err }

// storedError is the outcome error of action's stored error text, wrapping
// raw when the caller has it.
func storedError(action, text string, raw error) error {
	return &actionError{text: "action " + action + ": " + text, err: raw}
}
