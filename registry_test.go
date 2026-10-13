package flowkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const orderPaidSchema = `{
	"type": "object",
	"required": ["order_id"],
	"properties": {
		"order_id": {"type": "string"},
		"student_id": {"type": "string"},
		"seq": {"type": "integer"},
		"amount": {"type": "number"},
		"tags": {"type": "array"}
	}
}`

func orderPaid() EventDef {
	return EventDef{Type: "order.paid", Version: 1, PayloadSchema: json.RawMessage(orderPaidSchema)}
}

func sendEmail() ActionDef {
	return ActionDef{
		Name:         "send_email",
		ParamsSchema: json.RawMessage(`{"type": "object"}`),
		Execute: func(context.Context, ActionInput) (ActionResult, error) {
			return ActionResult{}, nil
		},
	}
}

func revenueFact() FactDef {
	return FactDef{
		Name:        "customer.revenue_90d",
		Type:        FactDouble,
		AvailableOn: []string{"order.*"},
		Resolve:     func(context.Context, Querier, Scope) (any, error) { return 0.0, nil },
	}
}

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("no panic, want one mentioning %q", want)
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, want) {
			t.Fatalf("panic %q does not mention %q", msg, want)
		}
	}()
	fn()
}

func TestMustAddEventPanics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*EventDef)
		want   string
	}{
		{"missing type", func(d *EventDef) { d.Type = "" }, "Type is required"},
		{"missing schema", func(d *EventDef) { d.PayloadSchema = nil }, "PayloadSchema is required"},
		{"schema not JSON", func(d *EventDef) { d.PayloadSchema = json.RawMessage(`{`) }, "not valid JSON"},
		{"entity kind without id field", func(d *EventDef) { d.EntityKind = "student" }, "EntityIDField is required"},
		{"entity id field not declared", func(d *EventDef) { d.EntityKind, d.EntityIDField = "student", "nope" }, "EntityIDField"},
		{"entity id field a number", func(d *EventDef) { d.EntityKind, d.EntityIDField = "order", "amount" }, "EntityIDField"},
		{"entity id field an array", func(d *EventDef) { d.EntityKind, d.EntityIDField = "order", "tags" }, "EntityIDField"},
		{"entity id field without kind", func(d *EventDef) { d.EntityIDField = "student_id" }, "EntityKind is required"},
		{"dedup key field misspelt", func(d *EventDef) { d.DedupKeyField = "orderId" }, "DedupKeyField"},
		{"dedup key field a number", func(d *EventDef) { d.DedupKeyField = "amount" }, "DedupKeyField"},
		{"dedup key field an array", func(d *EventDef) { d.DedupKeyField = "tags" }, "DedupKeyField"},
		{"scope field not declared", func(d *EventDef) { d.ScopeFields = map[string]string{"school": "school_id"} }, "ScopeFields"},
		{"scope field empty key", func(d *EventDef) { d.ScopeFields = map[string]string{"": "order_id"} }, "empty key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := orderPaid()
			tt.mutate(&d)
			mustPanic(t, tt.want, func() { NewRegistry().MustAddEvent(d) })
		})
	}
	t.Run("duplicate", func(t *testing.T) {
		r := NewRegistry()
		r.MustAddEvent(orderPaid())
		mustPanic(t, "already registered", func() { r.MustAddEvent(orderPaid()) })
	})
	t.Run("dedup key fields", func(t *testing.T) {
		for _, field := range []string{"order_id", "seq"} {
			d := orderPaid()
			d.DedupKeyField = field // seq is not required: an event without it is not deduplicated
			NewRegistry().MustAddEvent(d)
		}
	})
	t.Run("valid scope", func(t *testing.T) {
		d := orderPaid()
		d.EntityKind, d.EntityIDField = "student", "student_id"
		d.ScopeFields = map[string]string{"order": "order_id", "seq": "seq"}
		NewRegistry().MustAddEvent(d)
	})
}

func TestMustAddActionPanics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActionDef)
		want   string
	}{
		{"missing name", func(a *ActionDef) { a.Name = "" }, "Name is required"},
		{"missing schema", func(a *ActionDef) { a.ParamsSchema = nil }, "ParamsSchema is required"},
		{"schema not JSON", func(a *ActionDef) { a.ParamsSchema = json.RawMessage(`nope`) }, "not valid JSON"},
		{"missing execute", func(a *ActionDef) { a.Execute = nil }, "Execute is required"},
		{"negative retry", func(a *ActionDef) { a.Retry.MaxAttempts = -1 }, "RetryPolicy"},
		{"output fact without name", func(a *ActionDef) { a.OutputFacts = []OutputFact{{Type: FactBool}} }, "OutputFact Name is required"},
		{"output fact without type", func(a *ActionDef) { a.OutputFacts = []OutputFact{{Name: "sent"}} }, "invalid Type"},
		{"output fact not an identifier", func(a *ActionDef) { a.OutputFacts = []OutputFact{{Name: "sent-at", Type: FactBool}} }, "not a valid fact name"},
		{"output fact with an empty segment", func(a *ActionDef) { a.OutputFacts = []OutputFact{{Name: "mail..sent", Type: FactBool}} }, "not a valid fact name"},
		{"output fact twice", func(a *ActionDef) {
			a.OutputFacts = []OutputFact{{Name: "sent", Type: FactBool}, {Name: "sent", Type: FactBool}}
		}, "declared twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := sendEmail()
			tt.mutate(&a)
			mustPanic(t, tt.want, func() { NewRegistry().MustAddAction(a) })
		})
	}
	t.Run("duplicate", func(t *testing.T) {
		r := NewRegistry()
		r.MustAddAction(sendEmail())
		mustPanic(t, "already registered", func() { r.MustAddAction(sendEmail()) })
	})
}

func TestMustAddFactPanics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*FactDef)
		want   string
	}{
		{"missing name", func(f *FactDef) { f.Name = "" }, "Name is required"},
		{"name not an identifier", func(f *FactDef) { f.Name = "customer.revenue-90d" }, "not a valid fact name"},
		{"name starting with a digit", func(f *FactDef) { f.Name = "customer.90d" }, "not a valid fact name"},
		{"reserved word", func(f *FactDef) { f.Name = "customer.in" }, "not a valid fact name"},
		{"trailing dot", func(f *FactDef) { f.Name = "customer." }, "not a valid fact name"},
		{"zero type", func(f *FactDef) { f.Type = 0 }, "invalid Type"},
		{"unknown type", func(f *FactDef) { f.Type = FactTimestamp + 1 }, "invalid Type"},
		{"no patterns", func(f *FactDef) { f.AvailableOn = nil }, "AvailableOn is required"},
		{"empty pattern", func(f *FactDef) { f.AvailableOn = []string{"order.*", ""} }, "empty pattern"},
		{"missing resolve", func(f *FactDef) { f.Resolve = nil }, "Resolve is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := revenueFact()
			tt.mutate(&f)
			mustPanic(t, tt.want, func() { NewRegistry().MustAddFact(f) })
		})
	}
	t.Run("duplicate", func(t *testing.T) {
		r := NewRegistry()
		r.MustAddFact(revenueFact())
		mustPanic(t, "already registered", func() { r.MustAddFact(revenueFact()) })
	})
}

func TestCompileCrossValidates(t *testing.T) {
	tests := []struct {
		name  string
		build func(r *Registry)
		want  []string
	}{
		{"pattern matching no event", func(r *Registry) {
			f := revenueFact()
			f.AvailableOn = []string{"invoice.*", "order.paid"}
			r.MustAddFact(f)
		}, []string{`pattern "invoice.*" matches no registered event`}},
		{"output fact named like a fact", func(r *Registry) {
			r.MustAddFact(revenueFact())
			a := sendEmail()
			a.OutputFacts = []OutputFact{{Name: "customer.revenue_90d", Type: FactDouble}}
			r.MustAddAction(a)
		}, []string{`OutputFact "customer.revenue_90d" has the name of a registered fact`}},
		{"output fact typed two ways", func(r *Registry) {
			a := sendEmail()
			a.OutputFacts = []OutputFact{{Name: "delivered", Type: FactBool}}
			r.MustAddAction(a)
			b := sendEmail()
			b.Name = "send_sms"
			b.OutputFacts = []OutputFact{{Name: "delivered", Type: FactString}}
			r.MustAddAction(b)
		}, []string{`OutputFact "delivered" has type`}},
		{"fact nesting under a fact", func(r *Registry) {
			r.MustAddFact(revenueFact())
			f := revenueFact()
			f.Name = "customer.revenue_90d.trend"
			r.MustAddFact(f)
		}, []string{`fact "customer.revenue_90d.trend" nests under fact "customer.revenue_90d"`}},
		{"output fact nesting under a fact", func(r *Registry) {
			f := revenueFact()
			f.Name = "customer"
			r.MustAddFact(f)
			a := sendEmail()
			a.OutputFacts = []OutputFact{{Name: "customer.notified", Type: FactBool}}
			r.MustAddAction(a)
		}, []string{`OutputFact "customer.notified" of action "send_email" nests under fact "customer"`}},
		{"fact nesting under an output fact", func(r *Registry) {
			a := sendEmail()
			a.OutputFacts = []OutputFact{{Name: "email", Type: FactBool}}
			r.MustAddAction(a)
			f := revenueFact()
			f.Name = "email.opened"
			r.MustAddFact(f)
		}, []string{`fact "email.opened" nests under OutputFact "email" of action "send_email"`}},
		{"payload schema that does not compile", func(r *Registry) {
			d := orderPaid()
			d.Type = "order.refunded"
			d.PayloadSchema = json.RawMessage(`{"type": 12}`)
			r.MustAddEvent(d)
		}, []string{`event "order.refunded": PayloadSchema`}},
		{"params schema with an external ref", func(r *Registry) {
			a := sendEmail()
			a.ParamsSchema = json.RawMessage(`{"$ref": "https://example.com/x.json"}`)
			r.MustAddAction(a)
		}, []string{`action "send_email": ParamsSchema`}},
		{"every problem at once", func(r *Registry) {
			f := revenueFact()
			f.AvailableOn = []string{"nothing"}
			r.MustAddFact(f)
			a := sendEmail()
			a.ParamsSchema = json.RawMessage(`{"type": 12}`)
			r.MustAddAction(a)
		}, []string{`action "send_email"`, `pattern "nothing"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			r.MustAddEvent(orderPaid())
			tt.build(r)
			_, err := New(Config{Pool: placeholderPool}, r)
			if err == nil {
				t.Fatal("New accepted the registry")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

func TestCompileAcceptsValidRegistry(t *testing.T) {
	r := NewRegistry()
	r.MustAddEvent(orderPaid())
	a := sendEmail()
	a.OutputFacts = []OutputFact{{Name: "delivered", Type: FactBool}}
	r.MustAddAction(a)
	b := sendEmail()
	b.Name = "send_sms"
	b.OutputFacts = []OutputFact{{Name: "delivered", Type: FactBool}}
	r.MustAddAction(b)
	r.MustAddFact(revenueFact())
	everywhere := revenueFact()
	everywhere.Name, everywhere.AvailableOn = "tenant.plan", []string{"*"}
	r.MustAddFact(everywhere)
	c, err := r.clone().compile()
	if err != nil {
		t.Fatal(err)
	}
	if c.payloads["order.paid"] == nil || c.params["send_email"] == nil || c.params["send_sms"] == nil {
		t.Fatalf("schemas not compiled: %+v", c)
	}
}

func TestCloneIsolatesTheEngineCopy(t *testing.T) {
	r := NewRegistry()
	d := orderPaid()
	d.ScopeFields = map[string]string{"order": "order_id"}
	r.MustAddEvent(d)
	a := sendEmail()
	a.OutputFacts = []OutputFact{{Name: "delivered", Type: FactBool}}
	r.MustAddAction(a)
	r.MustAddFact(revenueFact())
	c := r.clone()

	// The caller's own values were cloned on the way in too.
	d.PayloadSchema[0] = '['
	d.ScopeFields["order"] = "changed"
	a.OutputFacts[0].Name = "changed"
	r.MustAddEvent(EventDef{Type: "order.late", PayloadSchema: json.RawMessage(`{}`)})
	r.events["order.paid"].ScopeFields["order"] = "changed"
	r.facts["customer.revenue_90d"].AvailableOn[0] = "changed"

	if _, ok := c.events["order.late"]; ok {
		t.Error("a registration after clone reached the copy")
	}
	got := c.events["order.paid"]
	if got.PayloadSchema[0] != '{' || got.ScopeFields["order"] != "order_id" {
		t.Errorf("event copy changed: %+v", got)
	}
	if c.actions["send_email"].OutputFacts[0].Name != "delivered" {
		t.Errorf("action copy changed: %+v", c.actions["send_email"])
	}
	if c.facts["customer.revenue_90d"].AvailableOn[0] != "order.*" {
		t.Errorf("fact copy changed: %+v", c.facts["customer.revenue_90d"])
	}
}

func TestMatchEventPattern(t *testing.T) {
	tests := []struct {
		pattern, event string
		want           bool
	}{
		{"*", "order.paid", true},
		{"order.paid", "order.paid", true},
		{"order.paid", "order.paid.late", false},
		{"order.*", "order.paid", true},
		{"order.*", "order.paid.late", true},
		{"order.*", "order.", false},
		{"order.*", "order", false},
		{"order.*", "orders.paid", false},
		{"order*", "orders", false},
		{"order*", "order*", true},
	}
	for _, tt := range tests {
		if got := matchEventPattern(tt.pattern, tt.event); got != tt.want {
			t.Errorf("matchEventPattern(%q, %q) = %v, want %v", tt.pattern, tt.event, got, tt.want)
		}
	}
}

func TestNotPerformed(t *testing.T) {
	cause := errors.New("connection refused before send")
	if NotPerformed(nil) != nil {
		t.Fatal("NotPerformed(nil) is not nil")
	}
	np := NotPerformed(cause)
	wrapped := fmt.Errorf("send_email: %w", fmt.Errorf("attempt 2: %w", np))
	joined := errors.Join(errors.New("other"), wrapped)
	for name, err := range map[string]error{"direct": np, "wrapped twice": wrapped, "joined": joined} {
		if !isNotPerformed(err) {
			t.Errorf("%s: not recognised as NotPerformed", name)
		}
		if !errors.Is(err, cause) {
			t.Errorf("%s: cause lost", name)
		}
	}
	if !strings.Contains(np.Error(), cause.Error()) {
		t.Errorf("message %q hides the cause", np.Error())
	}
	for name, err := range map[string]error{"nil": nil, "plain": cause, "wrapped plain": fmt.Errorf("x: %w", cause)} {
		if isNotPerformed(err) {
			t.Errorf("%s: recognised as NotPerformed", name)
		}
	}
}

func TestValidationErrors(t *testing.T) {
	a := &ValidationError{Path: "/order_id", Msg: "missing required property"}
	b := &ValidationError{Msg: "payload is too large"}
	if a.Error() != "/order_id: missing required property" || b.Error() != "payload is too large" {
		t.Fatalf("messages: %q, %q", a.Error(), b.Error())
	}
	var err error = fmt.Errorf("emit: %w", ValidationErrors{a, b})
	var verrs ValidationErrors
	if !errors.As(err, &verrs) || len(verrs) != 2 {
		t.Fatalf("errors.As(ValidationErrors) failed on %v", err)
	}
	var one *ValidationError
	if !errors.As(err, &one) || one != a {
		t.Fatalf("errors.As(*ValidationError) = %v, want the first", one)
	}
	if !strings.Contains(err.Error(), a.Error()) || !strings.Contains(err.Error(), b.Error()) {
		t.Fatalf("message %q lacks an entry", err)
	}
}

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{ErrDuplicateEvent, ErrSchemaDrift, ErrDefinitionDisabled, ErrNotFound, ErrUnknownEvent, ErrAlreadyResolved}
	for i, a := range all {
		if !strings.HasPrefix(a.Error(), "flowkit: ") {
			t.Errorf("sentinel %q lacks the package prefix", a)
		}
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v matches %v", a, b)
			}
		}
	}
}
