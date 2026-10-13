package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDSNHasPassword(t *testing.T) {
	cases := []struct {
		dsn  string
		want bool
	}{
		{"postgres://app@db:5432/flowkit", false},
		{"postgres://db/flowkit?sslmode=require", false},
		{"postgresql://app:secret@db/flowkit", true},
		{"postgres://app:@db/flowkit", true},
		{"postgres://app@db/flowkit?password=secret", true},
		{"host=db user=app dbname=flowkit", false},
		{"host=db user=app password=secret", true},
		{"host = db PASSWORD = secret", true},
		{"host=db user=app dbname='pass word=x'", false},
		{`host=db application_name='it\'s password=x' user=app`, false},
		{"host=db password='with space'", true},
		{"passfile=/home/app/.pgpass host=db", false},
		{"", false},
	}
	for _, c := range cases {
		got, err := dsnHasPassword(c.dsn)
		if err != nil {
			t.Errorf("dsnHasPassword(%q): %v", c.dsn, err)
			continue
		}
		if got != c.want {
			t.Errorf("dsnHasPassword(%q) = %v, want %v", c.dsn, got, c.want)
		}
	}
}

func TestDSNMalformed(t *testing.T) {
	for _, dsn := range []string{
		"host",
		"=db",
		"host=db 'user'=x",
		"host=db password='unterminated",
		"postgres://app:secret@db:port/flowkit",
	} {
		_, err := dsnHasPassword(dsn)
		if !errors.Is(err, errDSNMalformed) {
			t.Errorf("dsnHasPassword(%q) error = %v, want errDSNMalformed", dsn, err)
			continue
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), dsn) {
			t.Errorf("error %q repeats the connection string", err)
		}
	}
}

func TestDSNKeys(t *testing.T) {
	got, err := dsnKeys(" host=db  port = 5432 dbname='a b' sslmode=disable ")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"host", "port", "dbname", "sslmode"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dsnKeys = %v, want %v", got, want)
	}
}

func TestResolveDSN(t *testing.T) {
	env := func(vals map[string]string) func(string) string {
		return func(k string) string { return vals[k] }
	}
	withURL := env(map[string]string{dsnEnvVar: "postgres://app:secret@db/flowkit"})
	empty := env(nil)

	dsn, src, err := resolveDSN("host=flag", withURL)
	if err != nil || dsn != "host=flag" || src != "--dsn" {
		t.Fatalf("flag first: %q %q %v", dsn, src, err)
	}
	dsn, src, err = resolveDSN("", withURL)
	if err != nil || dsn != "postgres://app:secret@db/flowkit" || src != dsnEnvVar {
		t.Fatalf("env var: %q %q %v", dsn, src, err)
	}
	dsn, src, err = resolveDSN("", empty)
	if err != nil || dsn != "" || !strings.Contains(src, "PG*") {
		t.Fatalf("libpq fallback: %q %q %v", dsn, src, err)
	}
	if _, _, err := resolveDSN("postgres://app:secret@db/x", empty); !errors.Is(err, errDSNHasPassword) {
		t.Fatalf("password in --dsn: %v", err)
	}
	if strings.Contains(errDSNHasPassword.Error(), "secret") {
		t.Fatal("error repeats the password")
	}
}
