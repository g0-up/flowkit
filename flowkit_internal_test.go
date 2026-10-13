package flowkit

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestEventCheckerAppliesTheRegisteredType(t *testing.T) {
	r := NewRegistry()
	r.MustAddEvent(EventDef{
		Type: "student.enrolled",
		PayloadSchema: json.RawMessage(`{
			"type": "object",
			"required": ["course"],
			"properties": {"student_id": {"type": "integer"}, "school_id": {"type": "string"}, "course": {"type": "string"}}
		}`),
		EntityKind:    "student",
		EntityIDField: "student_id",
		ScopeFields:   map[string]string{"school": "school_id"},
	})
	reg, err := r.compile()
	if err != nil {
		t.Fatal(err)
	}
	c := newEventChecker(reg)

	info, err := c.Check("student.enrolled", json.RawMessage(`{"student_id": 7, "school_id": "hn", "course": "go"}`))
	if err != nil {
		t.Fatal(err)
	}
	if info.Shape == nil {
		t.Error("no shape for an object schema")
	}
	if info.Scope.EntityKind != "student" || info.Scope.EntityID != "7" || info.Scope.Extra["school"] != "hn" {
		t.Errorf("scope = %+v", info.Scope)
	}

	if info, err := c.Check("student.enrolled", json.RawMessage(`{"student_id": 7, "course": "go"}`)); err != nil || info.Scope.Extra != nil {
		t.Errorf("absent scope field: %+v, %v", info.Scope, err)
	}
	if _, err := c.Check("order.paid", json.RawMessage(`{}`)); !errors.Is(err, ErrUnknownEvent) {
		t.Errorf("unregistered type: %v, want ErrUnknownEvent", err)
	}
	var verrs ValidationErrors
	if _, err := c.Check("student.enrolled", json.RawMessage(`{"student_id": 7}`)); !errors.As(err, &verrs) {
		t.Errorf("payload without a required field: %v, want ValidationErrors", err)
	}
	if _, err := c.Check("student.enrolled", json.RawMessage(`{"course": "go"}`)); err == nil {
		t.Error("payload without the entity id fits")
	}
	if _, err := c.Check("student.enrolled", json.RawMessage(`{`)); err == nil {
		t.Error("undecodable payload fits")
	}
}
