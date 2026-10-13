package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"cel.dev/cel-go/cel"

	"github.com/g0-up/flowkit/internal/expr"
	"github.com/g0-up/flowkit/internal/interpreter/model"
	"github.com/g0-up/flowkit/internal/schema"
)

// fullValidationNote ends every validate report: the CLI has no registry,
// so what it cannot check is left to Registry.ValidateDefinition.
const fullValidationNote = "flowkit validate checks structure only; run Registry.ValidateDefinition with the application's " +
	"registry for full validation (events, actions, facts, params and expression types)"

func runValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), "usage: flowkit validate <file>\n\n"+
			"Checks the structure of a definition body: the metamodel schema, step ids and\n"+
			"transitions, that every step is reachable and reaches an end step, and the\n"+
			"syntax of every expression. Problems are printed with their JSON Pointer.\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	switch fs.NArg() {
	case 0:
		fmt.Fprintln(stderr, "flowkit validate: a definition file is required")
		return exitUsage
	case 1:
	default:
		fmt.Fprintf(stderr, "flowkit validate: unexpected argument %q\n", fs.Arg(1))
		return exitUsage
	}
	file := fs.Arg(0)
	body, err := readDefinition(file)
	if err != nil {
		fmt.Fprintf(stderr, "flowkit validate: %v\n", err)
		return exitError
	}

	problems := checkStructure(body)
	if len(problems) == 0 {
		fmt.Fprintf(stdout, "%s: structure is valid\n%s\n", file, fullValidationNote)
		return exitOK
	}
	for _, p := range problems {
		if p.Path == "" {
			fmt.Fprintf(stderr, "%s: %s\n", file, p.Msg)
		} else {
			fmt.Fprintf(stderr, "%s: %s: %s\n", file, p.Path, p.Msg)
		}
	}
	noun := "problems"
	if len(problems) == 1 {
		noun = "problem"
	}
	fmt.Fprintf(stderr, "%s: %d %s\n%s\n", file, len(problems), noun, fullValidationNote)
	return exitError
}

// readDefinition reads file, reading at most one byte past the body limit
// so that an oversized file is reported without being read whole.
func readDefinition(file string) ([]byte, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, model.MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	return body, nil
}

// checkStructure returns the problems of body that need no registry,
// sorted by path: model.Parse checks the size, the JSON, the metamodel
// schema and the step graph, and every expression is parsed with the
// macros and limits of the body's metamodel version but not type-checked,
// since the types come from the registry.
func checkStructure(body []byte) []schema.Violation {
	def, problems := model.Parse(body)
	if def == nil {
		return problems
	}
	mm, err := expr.Metamodel(def.MetamodelVersion)
	if err != nil {
		return append(problems, schema.Violation{Path: "/metamodel_version", Msg: err.Error()})
	}
	env, err := cel.NewEnv(
		cel.ClearMacros(),
		cel.Macros(mm.Macros...),
		cel.ParserExpressionSizeLimit(expr.MaxExpressionBytes),
		cel.ParserRecursionLimit(expr.MaxRecursionDepth),
	)
	if err != nil {
		return append(problems, schema.Violation{Msg: "build the expression parser: " + err.Error()})
	}
	parse := func(path, src string) {
		if src == "" {
			return
		}
		_, iss := env.Parse(src)
		if iss == nil || iss.Err() == nil {
			return
		}
		for _, ce := range iss.Errors() {
			e := &expr.Error{Line: ce.Location.Line(), Column: ce.Location.Column() + 1, Msg: ce.Message}
			problems = append(problems, schema.Violation{Path: path, Msg: e.Error()})
		}
	}

	parse("/concurrency/serial_key", def.Concurrency.SerialKey)
	for _, s := range def.Steps {
		switch s.Type {
		case model.StepAction:
			parse(s.Path()+"/when", s.Action.When)
			// Parse already reported malformed params.
			placeholders, err := model.ParamExpressions(s.Action.Params)
			if err != nil {
				continue
			}
			for _, p := range placeholders {
				parse(s.Path()+"/params"+p.Pointer+"/"+model.ExprKey, p.Source)
			}
		case model.StepWait:
			parse(s.Path()+"/match", s.Wait.Match)
		case model.StepSwitch:
			for i, c := range s.Switch.Cases {
				parse(fmt.Sprintf("%s/cases/%d/when", s.Path(), i), c.When)
			}
		}
	}
	slices.SortStableFunc(problems, func(a, b schema.Violation) int { return strings.Compare(a.Path, b.Path) })
	return problems
}
