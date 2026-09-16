# Site configuration

Keep real hostnames, users, scheduler accounts, deployment keys and filesystem locations outside the code checkout. The public example contains only generic values.

vibe-queue already reads an external config.toml. The default directory is ~/.config/vq (or XDG_CONFIG_HOME/vq). Set VQ_CONFIG_DIR to another directory when needed. Multi-user installations use /etc/vq/config.toml. The repository does not need the real file to be built or tested.

```sh
mkdir -p "$HOME/.config/vq"
cp config/public-example.toml "$HOME/.config/vq/config.toml"
# Edit the external copy for your site.
```

Store real site files in your private configuration management system. Do not commit them to this public source repository. See CONTRIBUTING.md for contribution requirements.

## Fleet release reports

Report-based fleet updates additionally require an external private Git
checkout. Keep operational report JSON out of product source:

```toml
fleet_report_repo = "/srv/private-operations"
fleet_report_history_repo = "/srv/retained-queue-history"
```

The current store contains accepted reports under `vibe-queue/releases/` or
`releases/`. Historical recovery may consult the retained original Git history
to authenticate an exact report digest and its original acceptance policy.
It never uses that history to select a current deployment. Preserve the
existing `pin_source_repos` mapping: source tags and ancestry are verified in
each product repository, independently of report storage.

Configure and validate these locations before updating a managed controller.
Missing configuration refuses report-based rollout, `--accepted-report` and
`--from-report`; it does not disable ordinary queue jobs, installation or
updates using an explicit source SHA. Existing controller source and interpreter
bindings remain in force. Do not switch their origins or pins to public snapshot
commit IDs as part of this configuration migration.
