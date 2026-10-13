// Package nfr holds the latency checks of the non-functional requirements:
// wake latency and timer lag of a real Engine on Postgres with the system
// clock. The tests are slow and run only when FLOWKIT_NFR=1; see
// docs/runbook-engine.md for the recorded results.
package nfr
