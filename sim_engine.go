package flowkit

import (
	"context"
	"errors"
	"fmt"

	"github.com/g0-up/flowkit/internal/sched"
	"github.com/g0-up/flowkit/internal/simhook"
	"github.com/g0-up/flowkit/internal/store"
)

func init() { simhook.NewEngine = newSimEngine }

// newSimEngine builds an Engine on the simulator's in-memory store and
// scheduler, with the interpreter and the executor New builds. Every action
// runs the override o.Execute instead of its registered Execute, so the
// simulator never performs a real effect.
func newSimEngine(o simhook.Options) (simhook.Engine, error) {
	cfg, ok := o.Config.(Config)
	if !ok {
		return simhook.Engine{}, fmt.Errorf("flowkit: simulator: Config is %T, want flowkit.Config", o.Config)
	}
	reg, ok := o.Registry.(*Registry)
	if !ok || reg == nil {
		return simhook.Engine{}, fmt.Errorf("flowkit: simulator: Registry is %T, want a non-nil *flowkit.Registry", o.Registry)
	}
	execute, ok := o.Execute.(func(context.Context, string, ActionInput) (ActionResult, error))
	if !ok || execute == nil {
		return simhook.Engine{}, errors.New("flowkit: simulator: an Execute override is required")
	}
	if o.Store == nil || o.Jobs == nil {
		return simhook.Engine{}, errors.New("flowkit: simulator: a store and a scheduler are required")
	}
	cfg, err := cfg.applyDefaults()
	if err != nil {
		return simhook.Engine{}, err
	}
	sim := reg.clone()
	for name, a := range sim.actions {
		a.Execute = func(ctx context.Context, in ActionInput) (ActionResult, error) {
			return execute(ctx, name, in)
		}
		sim.actions[name] = a
	}
	compiled, err := sim.compile()
	if err != nil {
		return simhook.Engine{}, err
	}
	handlers := &jobHandlers{}
	e, err := newEngine(cfg, compiled, o.Store, o.Jobs, handlers)
	if err != nil {
		return simhook.Engine{}, err
	}
	return simhook.Engine{
		Engine:   e,
		Handlers: handlers,
		Timing:   sched.NewTiming(cfg.ClaimTimeout),
		Emit: func(ctx context.Context, tx store.Tx, evt any) error {
			ev, ok := evt.(Event)
			if !ok {
				return fmt.Errorf("flowkit: simulator: event is %T, want flowkit.Event", evt)
			}
			return e.emit(ctx, tx, ev)
		},
	}, nil
}
