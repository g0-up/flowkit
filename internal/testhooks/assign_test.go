package testhooks

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// moduleRoot is the module directory, relative to this package.
const moduleRoot = "../.."

const modulePath = "github.com/g0-up/flowkit"

// hookPackage is a package of hooks and the only non-test code that may
// assign them.
type hookPackage struct {
	// dir is the package directory, relative to moduleRoot.
	dir string
	// assigners are the module directories or files, relative to
	// moduleRoot, whose non-test code may assign a hook of the package.
	assigners []string
}

var hookPackages = []hookPackage{
	// flowkittest points River at its VirtualClock.
	{dir: "internal/testhooks", assigners: []string{"flowkittest"}},
	// The flowkit package provides the simulator's Engine constructor.
	{dir: "internal/simhook", assigners: []string{"sim_engine.go"}},
}

// A hook is nil in production: only test code and the code each hook
// package names may assign one.
func TestOnlyTestSupportCodeAssignsHooks(t *testing.T) {
	if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err != nil {
		t.Fatalf("module root not found: %v", err)
	}
	type scan struct {
		hookPackage
		name    string
		hooks   map[string]bool
		allowed int
	}
	scans := make([]*scan, len(hookPackages))
	for i, hp := range hookPackages {
		name, hooks := hookNames(t, filepath.Join(moduleRoot, filepath.FromSlash(hp.dir)))
		scans[i] = &scan{hookPackage: hp, name: name, hooks: hooks}
	}
	fset := token.NewFileSet()
	scanned := 0
	err := filepath.WalkDir(moduleRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != moduleRoot && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		scanned++
		rel, err := filepath.Rel(moduleRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, s := range scans {
			found := hookAssignments(fset, f, pathpkg.Dir(rel) == s.dir, modulePath+"/"+s.dir, s.name, s.hooks)
			if slices.Contains(s.assigners, rel) || slices.Contains(s.assigners, pathpkg.Dir(rel)) {
				s.allowed += len(found)
				continue
			}
			for _, a := range found {
				t.Errorf("%s assigns a hook of %s outside the code allowed to: %s", rel, s.dir, a)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 50 {
		t.Fatalf("scanned %d Go files; the walk missed the module", scanned)
	}
	// The scan must see the assignment the allowed code does make.
	for _, s := range scans {
		if s.allowed == 0 {
			t.Errorf("found no assignment of a hook of %s in %v; the scan is broken", s.dir, s.assigners)
		}
	}
}

// hookNames returns the name of the package in dir and the variables its
// non-test files declare.
func hookNames(t *testing.T, dir string) (string, map[string]bool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	pkg := ""
	names := map[string]bool{}
	fset := token.NewFileSet()
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		pkg = f.Name.Name
		for _, decl := range f.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.VAR {
				for _, spec := range gd.Specs {
					for _, n := range spec.(*ast.ValueSpec).Names {
						names[n.Name] = true
					}
				}
			}
		}
	}
	if len(names) == 0 {
		t.Fatalf("%s declares no hook", dir)
	}
	return pkg, names
}

// hookAssignments lists where f assigns a hook of the package importPath,
// named pkgName, or takes a hook's address, which could be used to assign
// it. Outside that package a hook is a selector on the package's import
// name; inside it, own is set and a hook is a bare name, as it is in a
// file that dot-imports the package. A bare name is taken for the hook
// even where a local declaration shadows it.
func hookAssignments(fset *token.FileSet, f *ast.File, own bool, importPath, pkgName string, hooks map[string]bool) []string {
	local := ""
	bare := own
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == importPath {
			switch {
			case imp.Name == nil:
				local = pkgName
			case imp.Name.Name == ".":
				bare = true
			default:
				local = imp.Name.Name
			}
		}
	}
	if local == "" && !bare {
		return nil
	}
	isHook := func(e ast.Expr) (string, bool) {
		switch e := ast.Unparen(e).(type) {
		case *ast.SelectorExpr:
			if x, ok := e.X.(*ast.Ident); ok && local != "" && x.Name == local && hooks[e.Sel.Name] {
				return local + "." + e.Sel.Name, true
			}
		case *ast.Ident:
			if bare && hooks[e.Name] {
				return e.Name, true
			}
		}
		return "", false
	}
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if n.Tok == token.DEFINE {
				return true
			}
			for _, lhs := range n.Lhs {
				if name, ok := isHook(lhs); ok {
					found = append(found, fset.Position(n.Pos()).String()+": "+name+" "+n.Tok.String())
				}
			}
		case *ast.UnaryExpr:
			if name, ok := isHook(n.X); ok && n.Op == token.AND {
				found = append(found, fset.Position(n.Pos()).String()+": &"+name)
			}
		}
		return true
	})
	return found
}

var positionPrefix = regexp.MustCompile(`^[^:]*:\d+:\d+: `)

func TestHookAssignmentsFindsEveryForm(t *testing.T) {
	const (
		testhooksPath = modulePath + "/internal/testhooks"
		simhookPath   = modulePath + "/internal/simhook"
	)
	riverTime := map[string]bool{"RiverTime": true}
	newEngine := map[string]bool{"NewEngine": true}
	for _, tc := range []struct {
		name, src           string
		own                 bool
		importPath, pkgName string
		hooks               map[string]bool
		want                []string
	}{
		{"assignment", `package p
import "github.com/g0-up/flowkit/internal/testhooks"
func init() { testhooks.RiverTime = nil }`, false, testhooksPath, "testhooks", riverTime, []string{"testhooks.RiverTime ="}},
		{"renamed import", `package p
import th "github.com/g0-up/flowkit/internal/testhooks"
func f() { a, th.RiverTime = 1, nil; (th.RiverTime) = nil }`, false, testhooksPath, "testhooks", riverTime, []string{"th.RiverTime =", "th.RiverTime ="}},
		{"address", `package p
import "github.com/g0-up/flowkit/internal/testhooks"
var p = &testhooks.RiverTime`, false, testhooksPath, "testhooks", riverTime, []string{"&testhooks.RiverTime"}},
		{"read", `package p
import "github.com/g0-up/flowkit/internal/testhooks"
func f() { if h := testhooks.RiverTime; h != nil { x := testhooks.RiverTime; _ = x } }`, false, testhooksPath, "testhooks", riverTime, nil},
		{"other package", `package p
import "example.com/testhooks"
func f() { testhooks.RiverTime = nil }`, false, testhooksPath, "testhooks", riverTime, nil},
		{"own package", `package testhooks
var RiverTime func()
func init() { RiverTime = func() {} }`, true, testhooksPath, "testhooks", riverTime, []string{"RiverTime ="}},
		{"dot import", `package p
import . "github.com/g0-up/flowkit/internal/testhooks"
func init() { RiverTime = nil; p := &RiverTime; _ = p }`, false, testhooksPath, "testhooks", riverTime, []string{"RiverTime =", "&RiverTime"}},
		{"dot import read", `package p
import . "github.com/g0-up/flowkit/internal/testhooks"
func f() { if RiverTime != nil { h := RiverTime; _ = h } }`, false, testhooksPath, "testhooks", riverTime, nil},
		{"simhook dot import", `package p
import . "github.com/g0-up/flowkit/internal/simhook"
func init() { NewEngine = nil }`, false, simhookPath, "simhook", newEngine, []string{"NewEngine ="}},
		{"simhook assignment", `package p
import "github.com/g0-up/flowkit/internal/simhook"
func init() { simhook.NewEngine = nil; var o simhook.Options; _ = o }`, false, simhookPath, "simhook", newEngine, []string{"simhook.NewEngine ="}},
		{"simhook call", `package p
import "github.com/g0-up/flowkit/internal/simhook"
func f() { e, err := simhook.NewEngine(simhook.Options{}); _, _ = e, err }`, false, simhookPath, "simhook", newEngine, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "src.go", tc.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, a := range hookAssignments(fset, f, tc.own, tc.importPath, tc.pkgName, tc.hooks) {
				got = append(got, positionPrefix.ReplaceAllString(a, ""))
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("hookAssignments = %q, want %q", got, tc.want)
			}
		})
	}
}
