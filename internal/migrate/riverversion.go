package migrate

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
)

const (
	// RiverModulePath is the root of the River module tree. River releases it
	// and its submodules (riverdriver, riverpgxv5, rivershared, rivertype)
	// together under one version, and the runtime check reads them all.
	RiverModulePath = "github.com/riverqueue/river"
	// RequiredRiverPrefix is the River minor line this build is verified
	// against: the signed allowlist and River's schema behaviour belong to it.
	// Changing flowkit's River pin means re-signing the allowlist.
	RequiredRiverPrefix = "v0.49."
)

// RiverVersionError reports that the binary links a River module outside the
// required minor line, for example after Go's minimum version selection
// upgraded it through another dependency.
type RiverVersionError struct {
	Module string // module path as required, e.g. github.com/riverqueue/river/rivertype
	Found  string // linked version, after any replace directive
}

func (e *RiverVersionError) Error() string {
	return fmt.Sprintf("flowkit: River module %s %s is linked, but flowkit requires %sx (pin every River module in go.mod)",
		e.Module, e.Found, RequiredRiverPrefix)
}

// CheckRiverModule verifies the version of every River module linked into the
// running binary and returns a *RiverVersionError for the first one outside
// the required minor line. Without build information, without any River
// module, or for a module built from a local checkout ("(devel)" or a
// directory replace) the version cannot be known; that module, or the whole
// check, is skipped with a warning on logger (nil logs nothing).
func CheckRiverModule(logger *slog.Logger) error {
	bi, ok := debug.ReadBuildInfo()
	return checkRiverBuild(logger, bi, ok)
}

func checkRiverBuild(logger *slog.Logger, bi *debug.BuildInfo, ok bool) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if !ok || bi == nil {
		logger.Warn("flowkit: no build information; skipping River version check", "required", RequiredRiverPrefix+"x")
		return nil
	}
	mods := riverModules(bi)
	if len(mods) == 0 {
		logger.Warn("flowkit: River module not found in build information; skipping River version check", "required", RequiredRiverPrefix+"x")
		return nil
	}
	for _, mod := range mods {
		linked := mod
		if mod.Replace != nil {
			linked = mod.Replace
		}
		switch {
		case linked.Version == "" || linked.Version == "(devel)":
			logger.Warn("flowkit: River module version unknown (local build); skipping its version check",
				"module", mod.Path, "path", linked.Path, "required", RequiredRiverPrefix+"x")
		case !strings.HasPrefix(linked.Version, RequiredRiverPrefix):
			return &RiverVersionError{Module: mod.Path, Found: linked.Version}
		}
	}
	return nil
}

// riverModules returns the main module and the dependencies that belong to
// the River module tree, in build-info order. Separately versioned modules
// that merely share the name prefix, such as github.com/riverqueue/riverui,
// are not part of it.
func riverModules(bi *debug.BuildInfo) []*debug.Module {
	var out []*debug.Module
	if isRiverModule(bi.Main.Path) {
		out = append(out, &bi.Main)
	}
	for _, d := range bi.Deps {
		if d != nil && isRiverModule(d.Path) {
			out = append(out, d)
		}
	}
	return out
}

func isRiverModule(path string) bool {
	return path == RiverModulePath || strings.HasPrefix(path, RiverModulePath+"/")
}
