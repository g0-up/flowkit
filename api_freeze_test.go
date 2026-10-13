package flowkit_test

import (
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/g0-up/flowkit/internal/sched"
)

const (
	freezeFile = "api/freeze-v1.txt"
	// signatureFile is generated from the compiled packages, unlike
	// freezeFile, by `go test -run '^TestFreezeSignatures$' -update-freeze .`.
	signatureFile = "api/freeze-v1-signatures.txt"
	modulePath    = "github.com/g0-up/flowkit"
)

var updateFreeze = flag.Bool("update-freeze", false, "rewrite "+signatureFile+" from the compiled public packages")

// frozenPackages maps the package names used in the freeze file to their
// import paths.
var frozenPackages = map[string]string{
	"flowkit":     modulePath,
	"migrate":     modulePath + "/migrate",
	"flowkittest": modulePath + "/flowkittest",
}

// forbiddenPrefixes are import paths no exported signature may reference.
var forbiddenPrefixes = []string{
	"github.com/riverqueue/",
	modulePath + "/internal/",
}

var (
	loadOnce   sync.Once
	loaded     map[string]*types.Package
	loadErr    error
	loadedFset = token.NewFileSet()
)

// loadFrozen loads the frozen packages from the compiler's export data, once
// per test binary. `go list -export` builds them (or reuses the build cache)
// and reports where the export data of each package and dependency is.
func loadFrozen(t *testing.T) map[string]*types.Package {
	t.Helper()
	loadOnce.Do(func() {
		paths := slices.Collect(maps.Values(frozenPackages))
		cmd := exec.Command(goCommand(), append([]string{"list", "-export", "-deps", "-f", "{{.ImportPath}}={{.Export}}"}, paths...)...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		listing, err := cmd.Output()
		if err != nil {
			loadErr = fmt.Errorf("go list -export: %w\n%s", err, stderr.String())
			return
		}
		exports := make(map[string]string)
		for line := range strings.Lines(string(listing)) {
			path, file, _ := strings.Cut(strings.TrimSpace(line), "=")
			exports[path] = file
		}
		imp := importer.ForCompiler(loadedFset, "gc", func(path string) (io.ReadCloser, error) {
			file := exports[path]
			if file == "" {
				return nil, fmt.Errorf("no export data for %s", path)
			}
			return os.Open(file)
		})
		loaded = make(map[string]*types.Package)
		for name, path := range frozenPackages {
			pkg, err := imp.Import(path)
			if err != nil {
				loadErr = fmt.Errorf("load %s: %w", path, err)
				return
			}
			loaded[name] = pkg
		}
	})
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	return loaded
}

// goCommand returns the go command of the toolchain that built the test, so
// that it writes export data in the format the test's importer reads, and
// the go command on PATH only when that toolchain has none.
func goCommand() string {
	//lint:ignore SA1019 the test binary runs where its toolchain built it, so the build's root is meaningful
	if p := filepath.Join(runtime.GOROOT(), "bin", "go"); fileExists(p) {
		return p
	}
	return "go"
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// exportedAPI lists a package's exported identifiers in the freeze file
// format: package-level types, constants, variables and functions, and the
// exported methods of exported named types, including methods promoted from
// embedded fields. Interface methods belong to the interface type itself and
// are not listed.
func exportedAPI(name string, pkg *types.Package) []string {
	var out []string
	scope := pkg.Scope()
	for _, n := range scope.Names() {
		obj := scope.Lookup(n)
		if !obj.Exported() {
			continue
		}
		switch obj := obj.(type) {
		case *types.TypeName:
			out = append(out, "type "+n)
			if _, isIface := obj.Type().Underlying().(*types.Interface); isIface {
				continue
			}
			mset := types.NewMethodSet(types.NewPointer(obj.Type()))
			for sel := range mset.Methods() {
				if sel.Obj().Exported() {
					out = append(out, "method "+n+"."+sel.Obj().Name())
				}
			}
		case *types.Const:
			out = append(out, "const "+n)
		case *types.Var:
			out = append(out, "var "+n)
		case *types.Func:
			out = append(out, "func "+n)
		default:
			out = append(out, fmt.Sprintf("unknown %s (%T)", n, obj))
		}
	}
	for i := range out {
		out[i] = name + ": " + out[i]
	}
	return out
}

// readFreezeFile parses the freeze file into "package: kind name" entries.
func readFreezeFile(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(freezeFile)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	pkg := ""
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kind, ident, ok := strings.Cut(line, " ")
		switch {
		case !ok || strings.Contains(ident, " "):
			t.Fatalf("%s:%d: malformed line %q", freezeFile, i+1, line)
		case kind == "package":
			if _, known := frozenPackages[ident]; !known {
				t.Fatalf("%s:%d: unknown package %q", freezeFile, i+1, ident)
			}
			pkg = ident
		case pkg == "":
			t.Fatalf("%s:%d: identifier before any package line", freezeFile, i+1)
		case kind == "type" || kind == "const" || kind == "var" || kind == "func" || kind == "method":
			entry := pkg + ": " + line
			if slices.Contains(out, entry) {
				t.Fatalf("%s:%d: duplicate %q", freezeFile, i+1, line)
			}
			out = append(out, entry)
		default:
			t.Fatalf("%s:%d: unknown kind %q", freezeFile, i+1, kind)
		}
	}
	return out
}

// TestFreezePublicAPI compares the exported identifiers of the
// public packages with the approved freeze list. Removing or renaming a frozen
// identifier is a major change; adding one needs the list, and the design
// appendix it is copied from, updated first.
func TestFreezePublicAPI(t *testing.T) {
	pkgs := loadFrozen(t)
	want := readFreezeFile(t)
	var got []string
	for name, pkg := range pkgs {
		got = append(got, exportedAPI(name, pkg)...)
	}
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("frozen identifier missing from the code: %s", w)
		}
	}
	for _, g := range got {
		if !slices.Contains(want, g) {
			t.Errorf("exported identifier not on %s: %s", freezeFile, g)
		}
	}
}

// exportedSignatures lists the declarations of a package's exported
// identifiers, each prefixed with the package name: constants with their
// value, variables and functions with their type, and types with their
// definition. A struct or interface type is listed as its kind followed by
// one line per exported field or interface method, each prefixed with the
// type name; so is every exported method of a named type, promoted ones
// included, with its receiver. Types of other packages are qualified by
// their import path.
func exportedSignatures(name string, pkg *types.Package) []string {
	qual := types.RelativeTo(pkg)
	var out []string
	add := func(line string) { out = append(out, name+": "+line) }
	scope := pkg.Scope()
	for _, n := range scope.Names() {
		obj := scope.Lookup(n)
		if !obj.Exported() {
			continue
		}
		if c, isConst := obj.(*types.Const); isConst {
			add(types.ObjectString(c, qual) + " = " + c.Val().ExactString())
			continue
		}
		tn, isType := obj.(*types.TypeName)
		if !isType || tn.IsAlias() {
			add(types.ObjectString(obj, qual))
			continue
		}
		member := func(obj types.Object) { add(n + ": " + types.ObjectString(obj, qual)) }
		switch u := tn.Type().Underlying().(type) {
		case *types.Struct:
			add("type " + types.TypeString(tn.Type(), qual) + " struct")
			for f := range u.Fields() {
				if f.Exported() {
					member(f)
				}
			}
		case *types.Interface:
			// Every method of an interface, exported or not, binds the types
			// implementing it.
			add("type " + types.TypeString(tn.Type(), qual) + " interface")
			for m := range u.Methods() {
				member(m)
			}
			continue
		default:
			add(types.ObjectString(obj, qual))
		}
		for sel := range types.NewMethodSet(types.NewPointer(tn.Type())).Methods() {
			if sel.Obj().Exported() {
				member(sel.Obj())
			}
		}
	}
	return out
}

// signatureHeader opens signatureFile.
const signatureHeader = `# flowkit public API signatures, version 1.
#
# Generated from the compiled packages by
#   go test -run '^TestFreezeSignatures$' -update-freeze .
# Do not edit: api_freeze_test.go fails when the exported declarations of
# the public packages differ from this file. Which identifiers are public is
# decided by api/freeze-v1.txt; this file pins what they are.
#
`

// readSignatureFile returns the lines of signatureFile, comments and blank
// lines left out.
func readSignatureFile(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(signatureFile)
	if err != nil {
		t.Fatalf("%v (generate it with -update-freeze)", err)
	}
	var out []string
	for line := range strings.Lines(string(data)) {
		line = strings.TrimRight(line, "\n")
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// TestFreezeSignatures compares the exported declarations of the public
// packages with signatureFile, so that changing a frozen signature, a
// constant's value, an exported field or an interface's methods fails as
// removing a frozen identifier does. After an intended change, approved
// like any other change to the frozen API, regenerate the file with
// -update-freeze.
func TestFreezeSignatures(t *testing.T) {
	var got []string
	for name, pkg := range loadFrozen(t) {
		got = append(got, exportedSignatures(name, pkg)...)
	}
	slices.Sort(got)
	if *updateFreeze {
		if err := os.WriteFile(signatureFile, []byte(signatureHeader+strings.Join(got, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d declarations)", signatureFile, len(got))
		return
	}
	want := readSignatureFile(t)
	type diff struct{ line, sign string }
	var diffs []diff
	for _, w := range want {
		if !slices.Contains(got, w) {
			diffs = append(diffs, diff{w, "-"})
		}
	}
	for _, g := range got {
		if !slices.Contains(want, g) {
			diffs = append(diffs, diff{g, "+"})
		}
	}
	if len(diffs) == 0 {
		return
	}
	// Sorted by line, a changed declaration shows its old and new form
	// next to each other.
	slices.SortFunc(diffs, func(a, b diff) int {
		return cmp.Or(strings.Compare(a.line, b.line), strings.Compare(a.sign, b.sign))
	})
	lines := make([]string, len(diffs))
	for i, d := range diffs {
		lines[i] = d.sign + " " + d.line
	}
	t.Errorf("exported declarations differ from %s (- in the file, + in the code); "+
		"after an approved change, rerun with -update-freeze:\n  %s", signatureFile, strings.Join(lines, "\n  "))
}

// TestFreezeListsOnlyNonTestFiles checks that the listing comes from the
// packages as consumers compile them: helpers exported to tests from a
// _test.go file of the package are not public API.
func TestFreezeListsOnlyNonTestFiles(t *testing.T) {
	for name, pkg := range loadFrozen(t) {
		scope := pkg.Scope()
		for _, n := range scope.Names() {
			obj := scope.Lookup(n)
			if !obj.Exported() {
				continue
			}
			file := loadedFset.Position(obj.Pos()).Filename
			switch {
			case file == "":
				t.Errorf("%s.%s has no source position; cannot tell whether it is declared in a test file", name, n)
			case strings.HasSuffix(file, "_test.go"):
				t.Errorf("%s.%s is declared in test file %s", name, n, file)
			}
		}
	}
}

// TestFreezeSignaturesHideInternals walks every type reachable from an
// exported identifier (signatures, exported fields, methods, interface
// methods) and fails on any type from River or from an internal package.
func TestFreezeSignaturesHideInternals(t *testing.T) {
	pkgs := loadFrozen(t)
	for name, pkg := range pkgs {
		scope := pkg.Scope()
		for _, n := range scope.Names() {
			obj := scope.Lookup(n)
			if !obj.Exported() {
				continue
			}
			w := &leakWalker{seen: map[types.Type]bool{}}
			w.walk(obj.Type(), name+"."+n)
			for _, leak := range w.leaks {
				t.Errorf("exported API references a hidden package: %s", leak)
			}
		}
	}
}

// leakWalker collects references to forbidden packages reachable from a type.
type leakWalker struct {
	seen  map[types.Type]bool
	leaks []string
}

func (w *leakWalker) walk(typ types.Type, path string) {
	if typ == nil || w.seen[typ] {
		return
	}
	w.seen[typ] = true
	switch typ := typ.(type) {
	case *types.Alias:
		w.checkObject(typ.Obj(), path)
		w.walk(types.Unalias(typ), path)
	case *types.Named:
		obj := typ.Obj()
		if w.checkObject(obj, path) {
			return
		}
		for arg := range typ.TypeArgs().Types() {
			w.walk(arg, path+"["+arg.String()+"]")
		}
		// Only types defined in the public packages expose their structure;
		// for other packages (time, pgxpool, ...) the type name is the
		// whole contract.
		if obj.Pkg() == nil || !isFrozenPath(obj.Pkg().Path()) {
			return
		}
		w.walk(typ.Underlying(), path)
		for m := range typ.Methods() {
			if m.Exported() {
				w.walk(m.Type(), path+"."+m.Name())
			}
		}
	case *types.Pointer:
		w.walk(typ.Elem(), path)
	case *types.Slice:
		w.walk(typ.Elem(), path)
	case *types.Array:
		w.walk(typ.Elem(), path)
	case *types.Chan:
		w.walk(typ.Elem(), path)
	case *types.Map:
		w.walk(typ.Key(), path)
		w.walk(typ.Elem(), path)
	case *types.Signature:
		for i := range typ.Params().Len() {
			w.walk(typ.Params().At(i).Type(), fmt.Sprintf("%s(param %d)", path, i))
		}
		for v := range typ.Results().Variables() {
			w.walk(v.Type(), path+"(result)")
		}
		if tps := typ.TypeParams(); tps != nil {
			for tp := range tps.TypeParams() {
				w.walk(tp.Constraint(), path+"[constraint]")
			}
		}
	case *types.Struct:
		for f := range typ.Fields() {
			if f.Exported() {
				w.walk(f.Type(), path+"."+f.Name())
			}
		}
	case *types.Interface:
		for m := range typ.Methods() {
			if m.Exported() {
				w.walk(m.Type(), path+"."+m.Name())
			}
		}
		for e := range typ.EmbeddedTypes() {
			w.walk(e, path)
		}
	case *types.Union:
		for term := range typ.Terms() {
			w.walk(term.Type(), path)
		}
	}
}

// checkObject records a leak when obj belongs to a forbidden package.
func (w *leakWalker) checkObject(obj *types.TypeName, path string) bool {
	if obj.Pkg() == nil {
		return false
	}
	for _, prefix := range forbiddenPrefixes {
		if strings.HasPrefix(obj.Pkg().Path(), prefix) {
			w.leaks = append(w.leaks, fmt.Sprintf("%s uses %s.%s", path, obj.Pkg().Path(), obj.Name()))
			return true
		}
	}
	return false
}

func isFrozenPath(path string) bool {
	for _, p := range frozenPackages {
		if p == path {
			return true
		}
	}
	return false
}

// TestFreezeReservedNamesUnused keeps names reserved for later versions free, so
// they cannot be taken for another meaning in the meantime.
func TestFreezeReservedNamesUnused(t *testing.T) {
	pkg := loadFrozen(t)["flowkit"]
	if obj := pkg.Scope().Lookup("Compensable"); obj != nil {
		t.Errorf("flowkit.Compensable is reserved but declared at %s", loadedFset.Position(obj.Pos()))
	}
	for _, r := range []struct{ typeName, member string }{
		{"Runs", "Signal"},
		{"Engine", "DryRun"},
	} {
		tn, ok := pkg.Scope().Lookup(r.typeName).(*types.TypeName)
		if !ok {
			t.Errorf("flowkit.%s not found", r.typeName)
			continue
		}
		obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(tn.Type()), true, pkg, r.member)
		if obj != nil {
			t.Errorf("%s.%s is reserved but declared at %s", r.typeName, r.member, loadedFset.Position(obj.Pos()))
		}
	}
}

// TestFreezeJobKindsAndQueues pins the names stored with every River job.
// Renaming one orphans the jobs already queued under the old name.
func TestFreezeJobKindsAndQueues(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{sched.KindRouteEvent, "flowkit.route_event"},
		{sched.KindPumpRun, "flowkit.pump_run"},
		{sched.KindWaitTimeout, "flowkit.wait_timeout"},
		{sched.KindJanitor, "flowkit.janitor"},
		{sched.Queue, "flowkit"},
		{sched.IdleQueue, "flowkit_janitor_idle"},
	} {
		if c.got != c.want {
			t.Errorf("reserved job name changed: %q, want %q", c.got, c.want)
		}
	}
	for _, c := range []struct{ got, want string }{
		{sched.RouteEventArgs{}.Kind(), sched.KindRouteEvent},
		{sched.PumpRunArgs{}.Kind(), sched.KindPumpRun},
		{sched.WaitTimeoutArgs{}.Kind(), sched.KindWaitTimeout},
		{sched.JanitorArgs{}.Kind(), sched.KindJanitor},
	} {
		if c.got != c.want {
			t.Errorf("job args report kind %q, want %q", c.got, c.want)
		}
	}
}
