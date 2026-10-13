package migrate

import (
	"bytes"
	"errors"
	"log/slog"
	"runtime/debug"
	"strings"
	"testing"
)

func TestCheckRiverBuild(t *testing.T) {
	const (
		driver = RiverModulePath + "/riverdriver/riverpgxv5"
		types  = RiverModulePath + "/rivertype"
	)
	mod := func(path, version string, replace *debug.Module) *debug.Module {
		return &debug.Module{Path: path, Version: version, Replace: replace}
	}
	dep := func(version string, replace *debug.Module) *debug.Module {
		return mod(RiverModulePath, version, replace)
	}
	build := func(deps ...*debug.Module) *debug.BuildInfo {
		return &debug.BuildInfo{Main: debug.Module{Path: "example.com/app", Version: "(devel)"}, Deps: deps}
	}
	tests := []struct {
		name       string
		bi         *debug.BuildInfo
		ok         bool
		wantErr    string // substring; empty means no error
		wantModule string // RiverVersionError.Module when wantErr is set
		wantLog    bool
	}{
		{name: "pinned version", bi: build(dep("v0.49.0", nil)), ok: true},
		{name: "later patch", bi: build(dep("v0.49.3", nil)), ok: true},
		{name: "minor mismatch", bi: build(dep("v0.50.0", nil)), ok: true, wantErr: "v0.50.0", wantModule: RiverModulePath},
		{name: "older minor", bi: build(dep("v0.48.2", nil)), ok: true, wantErr: "v0.48.2", wantModule: RiverModulePath},
		{name: "replace wins", bi: build(dep("v0.49.0", &debug.Module{Path: "example.com/river-fork", Version: "v0.51.0"})), ok: true, wantErr: "v0.51.0", wantModule: RiverModulePath},
		{name: "replace with matching version", bi: build(dep("v0.50.0", &debug.Module{Path: RiverModulePath, Version: "v0.49.1"})), ok: true},
		{name: "local replace", bi: build(dep("v0.49.0", &debug.Module{Path: "../river"})), ok: true, wantLog: true},
		{name: "devel", bi: build(dep("(devel)", nil)), ok: true, wantLog: true},
		{name: "no build info", bi: nil, ok: false, wantLog: true},
		{name: "river not linked", bi: build(), ok: true, wantLog: true},
		{name: "river is the main module", bi: &debug.BuildInfo{Main: debug.Module{Path: RiverModulePath, Version: "v0.49.0"}}, ok: true},
		{name: "all submodules on the line", bi: build(dep("v0.49.0", nil), mod(driver, "v0.49.2", nil), mod(types, "v0.49.0", nil)), ok: true},
		{name: "submodule upgraded alone", bi: build(dep("v0.49.0", nil), mod(driver, "v0.50.1", nil), mod(types, "v0.49.0", nil)), ok: true, wantErr: "v0.50.1", wantModule: driver},
		{name: "submodule replaced off the line", bi: build(dep("v0.49.0", nil), mod(types, "v0.49.0", &debug.Module{Path: "example.com/types", Version: "v0.48.0"})), ok: true, wantErr: "v0.48.0", wantModule: types},
		{name: "local submodule with others pinned", bi: build(dep("v0.49.0", nil), mod(driver, "v0.49.0", &debug.Module{Path: "../riverpgxv5"})), ok: true, wantLog: true},
		{name: "submodule only", bi: build(mod(types, "v0.47.0", nil)), ok: true, wantErr: "v0.47.0", wantModule: types},
		{name: "separately versioned module with the name prefix", bi: build(dep("v0.49.0", nil), mod("github.com/riverqueue/riverui", "v0.12.0", nil)), ok: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			err := checkRiverBuild(logger, tt.bi, tt.ok)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "":
				var verr *RiverVersionError
				if !errors.As(err, &verr) || !strings.Contains(err.Error(), tt.wantErr) || !strings.Contains(err.Error(), RequiredRiverPrefix) {
					t.Fatalf("error = %v, want RiverVersionError mentioning %s and %s", err, tt.wantErr, RequiredRiverPrefix)
				}
				if verr.Module != tt.wantModule || !strings.Contains(err.Error(), tt.wantModule) {
					t.Fatalf("error = %v (module %q), want module %s", err, verr.Module, tt.wantModule)
				}
			}
			if gotLog := strings.Contains(logs.String(), "level=WARN"); gotLog != tt.wantLog {
				t.Fatalf("warn logged = %v, want %v (%s)", gotLog, tt.wantLog, logs.String())
			}
		})
	}
}

func TestCheckRiverModuleInThisBuild(t *testing.T) {
	if err := CheckRiverModule(nil); err != nil {
		t.Fatalf("the River version pinned in go.mod fails the check: %v", err)
	}
}
