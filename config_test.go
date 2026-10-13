package flowkit

import (
	"errors"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/g0-up/flowkit/internal/migrate"
)

// placeholderPool satisfies Config.Pool in tests that never reach the
// database.
var placeholderPool = &pgxpool.Pool{}

func TestConfigDefaults(t *testing.T) {
	c, err := Config{Pool: placeholderPool}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"Schema", c.Schema == "flowkit"},
		{"Roles", len(c.Roles) == 2 && c.hasRole(RoleWorker) && c.hasRole(RoleJanitor)},
		{"ClaimTimeout", c.ClaimTimeout == 5*time.Minute},
		{"LookbackMargin", c.LookbackMargin == time.Minute},
		{"FactTimeout", c.FactTimeout == 5*time.Second},
		{"MaxPayloadBytes", c.MaxPayloadBytes == 256*1024},
		{"Workers.MaxWorkers", c.Workers.MaxWorkers == 4},
		{"Workers.PollInterval", c.Workers.PollInterval == 0},
		{"Retention.StepDetail", c.Retention.StepDetail == 90*24*time.Hour},
		{"Retention.Run", c.Retention.Run == 2*365*24*time.Hour},
		{"Retention.Event", c.Retention.Event == 30*24*time.Hour},
		{"Retention.DeadLetter", c.Retention.DeadLetter == 90*24*time.Hour},
		{"Logger", c.Logger == slog.Default()},
		{"Tracer", c.Tracer != nil},
		{"Meter", c.Meter != nil},
		{"Clock", c.Clock == Clock(realClock{})},
		{"InstanceID", strings.HasSuffix(c.InstanceID, "-"+strconv.Itoa(os.Getpid()))},
		{"RedactError", c.RedactError != nil && c.RedactError(errors.New("x")) == "x"},
	}
	for _, ch := range checks {
		if !ch.ok {
			t.Errorf("default %s is wrong: %+v", ch.name, c)
		}
	}
}

func TestConfigKeepsExplicitValues(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	c, err := Config{
		Pool:            placeholderPool,
		Schema:          "tenant_a",
		Logger:          logger,
		InstanceID:      "web-1",
		Roles:           []Role{RoleJanitor, RoleJanitor},
		ClaimTimeout:    10 * time.Minute,
		MaxPayloadBytes: 1024,
		Workers:         WorkerConfig{MaxWorkers: 16, PollInterval: time.Second, PollOnly: true, EffectRateLimit: 2.5},
		Retention:       RetentionConfig{Event: 7 * 24 * time.Hour},
	}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.Schema != "tenant_a" || c.Logger != logger || c.InstanceID != "web-1" || c.ClaimTimeout != 10*time.Minute ||
		c.MaxPayloadBytes != 1024 || c.Workers.MaxWorkers != 16 || !c.Workers.PollOnly || c.Retention.Event != 7*24*time.Hour {
		t.Fatalf("explicit values changed: %+v", c)
	}
	if len(c.Roles) != 1 || c.Roles[0] != RoleJanitor || c.hasRole(RoleWorker) {
		t.Fatalf("Roles = %v, want only the janitor once", c.Roles)
	}
	if c.Retention.Run != 2*365*24*time.Hour {
		t.Fatalf("unset Retention.Run not defaulted: %s", c.Retention.Run)
	}
}

func TestConfigRejects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"nil pool", func(c *Config) { c.Pool = nil }, "Pool is required"},
		{"uppercase schema", func(c *Config) { c.Schema = "Flowkit" }, "Schema"},
		{"public schema", func(c *Config) { c.Schema = "public" }, "Schema"},
		{"pg_ schema", func(c *Config) { c.Schema = "pg_flow" }, "Schema"},
		{"long schema", func(c *Config) { c.Schema = strings.Repeat("a", 47) }, "Schema"},
		{"injection in schema", func(c *Config) { c.Schema = `x"; DROP TABLE t; --` }, "Schema"},
		{"unknown role", func(c *Config) { c.Roles = []Role{RoleWorker, Role(7)} }, "unknown role"},
		{"short claim timeout", func(c *Config) { c.ClaimTimeout = 4 * time.Second }, "ClaimTimeout"},
		{"claim timeout too close to rescue", func(c *Config) { c.ClaimTimeout = 30 * time.Minute }, "ClaimTimeout"},
		{"negative claim timeout", func(c *Config) { c.ClaimTimeout = -time.Second }, "ClaimTimeout must not be negative"},
		{"negative lookback", func(c *Config) { c.LookbackMargin = -1 }, "LookbackMargin"},
		{"negative fact timeout", func(c *Config) { c.FactTimeout = -1 }, "FactTimeout"},
		{"negative retention", func(c *Config) { c.Retention.DeadLetter = -1 }, "Retention.DeadLetter"},
		{"short step detail", func(c *Config) { c.Retention.StepDetail = 23 * time.Hour }, "Retention.StepDetail must be at least"},
		{"run summary shorter than detail", func(c *Config) {
			c.Retention.StepDetail, c.Retention.Run = 10*24*time.Hour, 9*24*time.Hour
		}, "Retention.Run must be at least"},
		{"event shorter than look-back horizon", func(c *Config) {
			c.LookbackMargin, c.ClaimTimeout, c.Retention.Event = time.Minute, time.Minute, 2*time.Minute-time.Nanosecond
		}, "Retention.Event must be at least"},
		{"short dead letter retention", func(c *Config) { c.Retention.DeadLetter = time.Hour - 1 }, "Retention.DeadLetter must be at least"},
		{"negative payload limit", func(c *Config) { c.MaxPayloadBytes = -1 }, "MaxPayloadBytes"},
		{"negative workers", func(c *Config) { c.Workers.MaxWorkers = -1 }, "MaxWorkers"},
		{"too many workers", func(c *Config) { c.Workers.MaxWorkers = 10001 }, "MaxWorkers"},
		{"poll interval too short", func(c *Config) { c.Workers.PollInterval = 50 * time.Millisecond }, "PollInterval"},
		{"negative poll interval", func(c *Config) { c.Workers.PollInterval = -time.Second }, "PollInterval"},
		{"negative rate limit", func(c *Config) { c.Workers.EffectRateLimit = -1 }, "EffectRateLimit"},
		{"NaN rate limit", func(c *Config) { c.Workers.EffectRateLimit = math.NaN() }, "EffectRateLimit"},
		{"infinite rate limit", func(c *Config) { c.Workers.EffectRateLimit = math.Inf(1) }, "EffectRateLimit"},
		{"negative infinite rate limit", func(c *Config) { c.Workers.EffectRateLimit = math.Inf(-1) }, "EffectRateLimit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{Pool: placeholderPool}
			tt.mutate(&c)
			_, err := c.withDefaults()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("withDefaults() = %v, want an error mentioning %q", err, tt.want)
			}
		})
	}
}

func TestConfigAcceptsTimingBounds(t *testing.T) {
	for _, ct := range []time.Duration{5 * time.Second, 30*time.Minute - time.Second} {
		if _, err := (Config{Pool: placeholderPool, ClaimTimeout: ct}).withDefaults(); err != nil {
			t.Errorf("ClaimTimeout %s rejected: %v", ct, err)
		}
	}
}

func TestConfigAcceptsRetentionBounds(t *testing.T) {
	c := Config{
		Pool: placeholderPool, LookbackMargin: time.Minute, ClaimTimeout: time.Minute,
		Retention: RetentionConfig{StepDetail: 24 * time.Hour, Run: 24 * time.Hour, Event: 2 * time.Minute, DeadLetter: time.Hour},
	}
	got, err := c.withDefaults()
	if err != nil {
		t.Fatalf("retention at its bounds rejected: %v", err)
	}
	if got.Retention != c.Retention {
		t.Fatalf("Retention = %+v, want %+v", got.Retention, c.Retention)
	}
}

func TestNewRejectsConfigBeforeTouchingTheDatabase(t *testing.T) {
	_, err := New(Config{Pool: placeholderPool, ClaimTimeout: time.Second}, NewRegistry())
	if err == nil || !strings.Contains(err.Error(), "ClaimTimeout") {
		t.Fatalf("New = %v, want a ClaimTimeout error", err)
	}
	if _, err := New(Config{Pool: placeholderPool}, nil); err == nil {
		t.Fatal("New accepted a nil Registry")
	}
}

func TestDefaultRedactError(t *testing.T) {
	if got := defaultRedactError(nil); got != "" {
		t.Errorf("nil error redacts to %q", got)
	}
	short := strings.Repeat("a", redactLimit)
	if got := defaultRedactError(errors.New(short)); got != short {
		t.Errorf("an error at the limit was cut to %d bytes", len(got))
	}
	if got := defaultRedactError(errors.New(strings.Repeat("b", 5000))); len(got) != redactLimit {
		t.Errorf("long error redacts to %d bytes, want %d", len(got), redactLimit)
	}
	// "é" is two bytes; an odd prefix puts the limit inside one of them.
	got := defaultRedactError(errors.New("x" + strings.Repeat("é", redactLimit)))
	if !utf8.ValidString(got) || len(got) > redactLimit || len(got) < redactLimit-1 {
		t.Errorf("multi-byte error redacts to %d bytes, valid UTF-8 %v", len(got), utf8.ValidString(got))
	}
}

func TestDefaultRedactErrorRepairsText(t *testing.T) {
	tests := []struct {
		name string
		msg  string
	}{
		{"short invalid UTF-8", "bad \xff byte"},
		{"NUL", "a\x00b"},
		{"long run of continuation bytes", strings.Repeat("\x80", 5000)},
		{"continuation bytes at the cut", strings.Repeat("a", redactLimit-2) + strings.Repeat("\xbf", 10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := defaultRedactError(errors.New(tt.msg))
			if got == "" || !utf8.ValidString(got) || strings.ContainsRune(got, 0) || len(got) > redactLimit {
				t.Fatalf("redacts to %q (%d bytes)", got, len(got))
			}
		})
	}
}

func TestRoleNames(t *testing.T) {
	if RoleWorker != 0 || RoleJanitor != 1 {
		t.Fatalf("role values changed: worker %d, janitor %d", RoleWorker, RoleJanitor)
	}
	if roleName(RoleWorker) != "worker" || roleName(RoleJanitor) != "janitor" || roleName(Role(9)) == "" {
		t.Fatalf("role names: %q %q %q", roleName(RoleWorker), roleName(RoleJanitor), roleName(Role(9)))
	}
}

func TestRiverModuleErrorIsSchemaDrift(t *testing.T) {
	if err := riverModuleError(nil); err != nil {
		t.Fatalf("no error maps to %v", err)
	}
	rv := &migrate.RiverVersionError{Module: migrate.RiverModulePath, Found: "v0.50.0"}
	err := riverModuleError(rv)
	var got *migrate.RiverVersionError
	if !errors.Is(err, ErrSchemaDrift) || !errors.As(err, &got) || got != rv {
		t.Fatalf("River version mismatch maps to %v", err)
	}
	other := errors.New("cannot read build info")
	if err := riverModuleError(other); errors.Is(err, ErrSchemaDrift) || !errors.Is(err, other) {
		t.Fatalf("other error maps to %v", err)
	}
}
