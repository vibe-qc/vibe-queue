# Private fleet release reports

Operational report JSON belongs in an external private operations repository.
The product source preserves report parsing, evidence validation and rollout
logic; it does not distribute real fleet release records.

Set `fleet_report_repo` and, for old receipt recovery,
`fleet_report_history_repo` in the driver's external configuration. See
[site configuration](../config/README.md#fleet-release-reports). Keep the
existing logical report paths, hashes and product pin mappings unchanged.

Generate reports with `scripts/make_release_report.py` from the calculation
engine checkout, supplying explicit queue/view source checkouts and an absolute
`--output` path inside private operations storage. The generator refuses an
output inside any selected product tree or Git metadata. Validate the report
against `docs/fleet_release_report.schema.json` and follow the existing evidence
and independent-review requirements before committing it to the private report
repository. Current deployment never falls back to historical acceptance rules.

Product release tags remain immutable. Moving report storage does not authorize
a fleet update, a source-origin change or a new release cut.
