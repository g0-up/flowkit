// Package flowkittest provides test helpers for code that embeds flowkit: a
// virtual clock, a Postgres harness, a simulator, and matchers.
//
// PGHarness runs a real Engine with a VirtualClock in a throwaway schema of
// the Postgres database FLOWKIT_TEST_DATABASE_URL names. Time moves only
// through PGHarness.Advance, which also waits until every job due by the new
// time has run, so a test can cross a three-day wait or a retry backoff in
// seconds:
//
//	h := flowkittest.NewPGHarness(t, reg)
//	h.Publish("onboarding", body)
//	h.Emit(evt)
//	h.Advance(0)              // run what is due now
//	h.Advance(72 * time.Hour) // the wait times out
//
// Sim runs definitions in memory with stubbed actions, for fast tests of
// definition logic.
//
// Matchers check what a run did by reading its trace, so the same checks
// work on both:
//
//	flowkittest.AssertTrace(t, h.Engine().Runs(), runID,
//		flowkittest.StepSkipped("vip-slack", "when"),
//		flowkittest.RunTerminal("onboarded"),
//	)
//
// Importing the package lets an Engine whose Config.Clock is a
// *VirtualClock drive River with that clock. Engines with any other clock
// are unaffected.
package flowkittest
