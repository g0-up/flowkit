# contrib

Operational assets that ship with flowkit:

- `dashboards/flowkit.json`: a Grafana dashboard over every flowkit metric.
- `alerts/flowkit-rules.yaml`: Prometheus alert rules for the conditions that
  need a person (open dead letters, effects in doubt, orphaned effects, timer
  lag, wake latency, no scheduler leader).

## Metric names

flowkit records through the OpenTelemetry API on `Config.Meter`; it ships no
exporter. Both files assume the OpenTelemetry Prometheus exporter with its
default name translation:

| OpenTelemetry instrument | Prometheus series |
|--------------------------|-------------------|
| counter `flowkit.runs_started` | `flowkit_runs_started_total` |
| histogram `flowkit.wake_latency` (unit `s`) | `flowkit_wake_latency_seconds_bucket` |
| gauge `flowkit.dead_letters_open` | `flowkit_dead_letters_open` |
| up-down counter `flowkit.effects_inflight` | `flowkit_effects_inflight` |

If your pipeline names series differently (an OTLP collector with a
namespace, or `WithoutCounterSuffixes`), rewrite the names before importing.

`flowkit.scheduler_leader` carries an `instance` attribute, the
`Config.InstanceID`. Prometheus renames it to `exported_instance` because the
scrape already sets `instance`; the dashboard reads `exported_instance`. The
default `InstanceID` is `hostname-pid`, so every restart starts a new series:
set a stable `InstanceID`, such as the pod name, so the series count does not
grow with each deploy.

## Gauges are per instance

`flowkit.dead_letters_open`, `flowkit.in_doubt_open` and `flowkit.queue_depth`
are read from the database whenever an instance's MeterProvider collects, so
every instance reports the same database-wide value. Aggregate them with
`max`, never `sum`. `flowkit.scheduler_leader` is 1 only on the leading
instance; `max` of it is 1 while some instance leads. `flowkit.effects_inflight`
is the only one each instance counts for itself: sum it.

## Import

Grafana: Dashboards, New, Import, upload `dashboards/flowkit.json`, then pick
the Prometheus data source for the `datasource` variable.

Prometheus: add the file to `rule_files` in `prometheus.yml` and reload:

```yaml
rule_files:
  - /etc/prometheus/rules/flowkit-rules.yaml
```

Check it first with `promtool check rules alerts/flowkit-rules.yaml`. The
responses to each alert are in `docs/runbook-engine.md`.
