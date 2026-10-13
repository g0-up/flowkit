package interpreter

import "testing"

func TestRunIDIsTheUUIDv5OfDefinitionAndEvent(t *testing.T) {
	// Computed independently: Python uuid.uuid5(namespace, "enrol:42").
	const want = "83da80fb-9986-5801-8837-c3825eb527c5"
	if got := RunID("enrol", 42).String(); got != want {
		t.Fatalf("RunID = %s, want %s; the namespace must never change", got, want)
	}
	if RunID("enrol", 42) == RunID("enrol", 43) || RunID("enrol", 42) == RunID("enrol2", 42) {
		t.Fatal("RunID collides across events or definitions")
	}
}
