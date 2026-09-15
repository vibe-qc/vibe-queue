"""Tests for vq.web.settings — the console's one configuration resolver.

The contract under test is the precedence chain (CLI flag > environment
variable > ``[web]`` config section > built-in default), the validation
the ``[web]`` section enforces, and the derived properties every consumer
reads instead of re-deciding for itself (loopback detection, the public
bind warning, the console URL, per-field provenance).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from vq import config
from vq.web import settings
from vq.web.settings import WebSettings, describe_sources, resolve_settings

ALL_WEB_ENV = (
    settings.ENV_BIND,
    settings.ENV_PORT,
    settings.ENV_FLEET,
    settings.ENV_FLEET_INTERVAL,
    settings.ENV_LOG_LEVEL,
    settings.ENV_TITLE,
    settings.ENV_PUBLIC_BIND_ACK,
)


@pytest.fixture(autouse=True)
def isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the config loader at tmp_path and clear every VQ_WEB_* var.

    Without this the suite would resolve against the developer's real
    ~/.config/vq/config.toml and against whatever VQ_WEB_FLEET the shell
    happened to export.
    """
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path))
    for name in ALL_WEB_ENV:
        monkeypatch.delenv(name, raising=False)
    return tmp_path


def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def _cfg(**web: object) -> config.Config:
    return config.Config(web=config.WebConsoleConfig(**web))


class TestBuiltInDefaults:
    def test_empty_config_yields_documented_defaults(self) -> None:
        resolved = resolve_settings(config.Config())
        assert resolved.bind == "127.0.0.1"
        assert resolved.port == 8765
        assert resolved.fleet is False
        assert resolved.fleet_interval_seconds == 30
        assert resolved.log_level == "info"
        assert resolved.title == "vq"
        assert resolved.public_bind_ack is False

    def test_dataclass_defaults_match_resolved_defaults(self) -> None:
        assert WebSettings() == resolve_settings(config.Config())

    def test_missing_config_file_is_not_an_error(self, tmp_path: Path) -> None:
        assert not (tmp_path / "config.toml").exists()
        assert resolve_settings() == WebSettings()

    def test_default_bind_is_loopback_and_fleet_is_off(self) -> None:
        """The unauthenticated read surface must not be reachable off-host
        and an SSH fan-out must be opt-in, by default."""
        resolved = resolve_settings(config.Config())
        assert resolved.is_loopback_bind is True
        assert resolved.needs_public_bind_warning is False
        assert resolved.fleet is False


class TestPrecedence:
    def test_flag_beats_env_beats_config_beats_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _cfg(
            bind="192.0.2.10",
            port=9001,
            fleet_interval_seconds=61,
            log_level="debug",
            title="from-config",
        )
        monkeypatch.setenv(settings.ENV_BIND, "192.0.2.11")
        monkeypatch.setenv(settings.ENV_PORT, "9002")
        monkeypatch.setenv(settings.ENV_FLEET_INTERVAL, "62")
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "warning")
        monkeypatch.setenv(settings.ENV_TITLE, "from-env")

        resolved = resolve_settings(
            cfg,
            bind="192.0.2.12",
            port=9003,
            fleet_interval_seconds=63,
            log_level="error",
            title="from-flag",
        )
        assert resolved.bind == "192.0.2.12"
        assert resolved.port == 9003
        assert resolved.fleet_interval_seconds == 63
        assert resolved.log_level == "error"
        assert resolved.title == "from-flag"

    def test_env_beats_config_when_no_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = _cfg(
            bind="192.0.2.10",
            port=9001,
            fleet_interval_seconds=61,
            log_level="debug",
            title="from-config",
        )
        monkeypatch.setenv(settings.ENV_BIND, "192.0.2.11")
        monkeypatch.setenv(settings.ENV_PORT, "9002")
        monkeypatch.setenv(settings.ENV_FLEET_INTERVAL, "62")
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "warning")
        monkeypatch.setenv(settings.ENV_TITLE, "from-env")

        resolved = resolve_settings(cfg)
        assert resolved.bind == "192.0.2.11"
        assert resolved.port == 9002
        assert resolved.fleet_interval_seconds == 62
        assert resolved.log_level == "warning"
        assert resolved.title == "from-env"

    def test_config_alone_beats_default(self) -> None:
        cfg = _cfg(
            bind="0.0.0.0",
            port=9001,
            fleet=True,
            fleet_interval_seconds=61,
            log_level="debug",
            title="from-config",
            public_bind_ack=True,
        )
        resolved = resolve_settings(cfg)
        assert resolved.bind == "0.0.0.0"
        assert resolved.port == 9001
        assert resolved.fleet is True
        assert resolved.fleet_interval_seconds == 61
        assert resolved.log_level == "debug"
        assert resolved.title == "from-config"
        assert resolved.public_bind_ack is True

    def test_config_is_read_from_the_config_file(self, tmp_path: Path) -> None:
        _write_config(
            tmp_path,
            '[web]\nbind = "0.0.0.0"\nport = 9100\nfleet = true\n'
            'title = "reference fleet"\n',
        )
        resolved = resolve_settings()
        assert resolved.bind == "0.0.0.0"
        assert resolved.port == 9100
        assert resolved.fleet is True
        assert resolved.title == "reference fleet"

    def test_layers_fill_in_independently(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each layer supplies only what the layer above left unset, so a
        site can pin one field without restating the rest."""
        monkeypatch.setenv(settings.ENV_PORT, "9002")
        resolved = resolve_settings(_cfg(title="mixed"), bind="192.0.2.12")
        assert resolved.bind == "192.0.2.12"  # flag
        assert resolved.port == 9002  # env
        assert resolved.title == "mixed"  # config
        assert resolved.log_level == "info"  # default

    def test_log_level_is_lowercased_whatever_the_layer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        assert resolve_settings(config.Config(), log_level="WARNING").log_level == (
            "warning"
        )
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "DEBUG")
        assert resolve_settings(config.Config()).log_level == "debug"


class TestFleetIsTriState:
    def test_explicit_false_flag_beats_config_true(self) -> None:
        """--no-fleet must be able to turn off a console whose config file
        says fleet = true; a tri-state flag is the only way to express it."""
        cfg = _cfg(fleet=True)
        assert resolve_settings(cfg, fleet=False).fleet is False

    def test_none_flag_does_not_beat_config_true(self) -> None:
        cfg = _cfg(fleet=True)
        assert resolve_settings(cfg, fleet=None).fleet is True
        assert resolve_settings(cfg).fleet is True

    def test_explicit_true_flag_beats_config_false(self) -> None:
        assert resolve_settings(_cfg(fleet=False), fleet=True).fleet is True

    def test_env_false_beats_config_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_FLEET, "0")
        assert resolve_settings(_cfg(fleet=True)).fleet is False

    def test_flag_beats_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(settings.ENV_FLEET, "1")
        assert resolve_settings(config.Config(), fleet=False).fleet is False

    def test_public_bind_ack_is_tri_state_too(self) -> None:
        cfg = _cfg(public_bind_ack=True)
        assert resolve_settings(cfg, public_bind_ack=False).public_bind_ack is False
        assert resolve_settings(cfg, public_bind_ack=None).public_bind_ack is True


class TestEnvParsing:
    @pytest.mark.parametrize("raw", ["1", "true", "yes", "on", "TRUE", " On "])
    def test_truthy_env_spellings(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv(settings.ENV_FLEET, raw)
        assert resolve_settings(config.Config()).fleet is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", "OFF", " no "])
    def test_falsy_env_spellings(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv(settings.ENV_FLEET, raw)
        assert resolve_settings(_cfg(fleet=True)).fleet is False

    @pytest.mark.parametrize("raw", ["maybe", "2", "", "  "])
    def test_unparseable_bool_falls_through_not_off(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        """A typo'd VQ_WEB_FLEET must defer to the config, not silently
        read as 'off' — a garbled variable is absent information, not a
        request to disable fleet mode."""
        monkeypatch.setenv(settings.ENV_FLEET, raw)
        assert resolve_settings(_cfg(fleet=True)).fleet is True
        assert resolve_settings(config.Config()).fleet is False

    @pytest.mark.parametrize("raw", ["eight", "", "80.5", "  "])
    def test_non_integer_port_falls_through(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv(settings.ENV_PORT, raw)
        assert resolve_settings(_cfg(port=9100)).port == 9100
        assert resolve_settings(config.Config()).port == 8765

    def test_integer_port_with_surrounding_whitespace_is_read(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_PORT, " 9002 ")
        assert resolve_settings(config.Config()).port == 9002

    @pytest.mark.parametrize("raw", ["4", "0", "-1"])
    def test_sub_floor_interval_from_env_falls_through(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        """Below 5 s a sweep would overlap its own SSH fan-out; the
        environment has no validation layer, so the floor is re-checked
        here rather than honoured."""
        monkeypatch.setenv(settings.ENV_FLEET_INTERVAL, raw)
        assert resolve_settings(_cfg(fleet_interval_seconds=61)) \
            .fleet_interval_seconds == 61
        assert resolve_settings(config.Config()).fleet_interval_seconds == 30

    def test_interval_at_the_floor_is_honoured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            settings.ENV_FLEET_INTERVAL, str(settings.MIN_FLEET_INTERVAL_SECONDS)
        )
        assert resolve_settings(config.Config()).fleet_interval_seconds == 5

    def test_sub_floor_interval_flag_is_replaced_by_the_default(self) -> None:
        assert (
            resolve_settings(config.Config(), fleet_interval_seconds=1)
            .fleet_interval_seconds
            == 30
        )

    @pytest.mark.parametrize("name", [settings.ENV_BIND, settings.ENV_TITLE])
    def test_blank_string_env_falls_through(
        self, monkeypatch: pytest.MonkeyPatch, name: str
    ) -> None:
        monkeypatch.setenv(name, "   ")
        resolved = resolve_settings(_cfg(bind="192.0.2.10", title="from-config"))
        assert resolved.bind == "192.0.2.10"
        assert resolved.title == "from-config"

    def test_string_env_values_are_stripped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_BIND, "  192.0.2.11  ")
        monkeypatch.setenv(settings.ENV_TITLE, "  host_d  ")
        resolved = resolve_settings(config.Config())
        assert resolved.bind == "192.0.2.11"
        assert resolved.title == "host_d"

    def test_public_bind_ack_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(settings.ENV_PUBLIC_BIND_ACK, "yes")
        assert resolve_settings(config.Config()).public_bind_ack is True
        monkeypatch.setenv(settings.ENV_PUBLIC_BIND_ACK, "off")
        assert resolve_settings(_cfg(public_bind_ack=True)).public_bind_ack is False


class TestWebConsoleConfigValidation:
    def test_sub_floor_interval_rejected_at_load(self, tmp_path: Path) -> None:
        _write_config(tmp_path, "[web]\nfleet_interval_seconds = 4\n")
        with pytest.raises(config.ConfigError):
            config.load_config()

    def test_floor_interval_accepted_at_load(self, tmp_path: Path) -> None:
        _write_config(tmp_path, "[web]\nfleet_interval_seconds = 5\n")
        assert config.load_config().web.fleet_interval_seconds == 5

    @pytest.mark.parametrize("port", [0, -1, 65536, 70000])
    def test_out_of_range_port_rejected(self, tmp_path: Path, port: int) -> None:
        _write_config(tmp_path, f"[web]\nport = {port}\n")
        with pytest.raises(config.ConfigError):
            config.load_config()

    @pytest.mark.parametrize("port", [1, 8765, 65535])
    def test_in_range_port_accepted(self, tmp_path: Path, port: int) -> None:
        _write_config(tmp_path, f"[web]\nport = {port}\n")
        assert config.load_config().web.port == port

    def test_unknown_log_level_rejected(self, tmp_path: Path) -> None:
        _write_config(tmp_path, '[web]\nlog_level = "chatty"\n')
        with pytest.raises(config.ConfigError):
            config.load_config()

    @pytest.mark.parametrize(
        "level", ["critical", "error", "warning", "info", "debug", "trace"]
    )
    def test_known_log_levels_accepted(self, tmp_path: Path, level: str) -> None:
        _write_config(tmp_path, f'[web]\nlog_level = "{level.upper()}"\n')
        assert config.load_config().web.log_level == level

    @pytest.mark.parametrize("field", ["bind", "title"])
    def test_empty_string_rejected(self, tmp_path: Path, field: str) -> None:
        _write_config(tmp_path, f'[web]\n{field} = "   "\n')
        with pytest.raises(config.ConfigError):
            config.load_config()

    def test_unknown_key_rejected(self, tmp_path: Path) -> None:
        """extra='forbid': a misspelled key must fail loudly rather than be
        silently ignored, which is how a console ends up not doing what its
        config file says."""
        _write_config(tmp_path, "[web]\nfleet_interval = 60\n")
        with pytest.raises(config.ConfigError):
            config.load_config()

    def test_absent_section_leaves_every_field_unset(self, tmp_path: Path) -> None:
        _write_config(tmp_path, 'default_host = "host_d"\n')
        section = config.load_config().web
        assert section.bind is None
        assert section.port is None
        assert section.fleet is None
        assert section.fleet_interval_seconds is None
        assert section.log_level is None
        assert section.title is None
        assert section.public_bind_ack is None


class TestIsLoopbackBind:
    @pytest.mark.parametrize(
        "bind", ["localhost", "127.0.0.1", "127.1.2.3", "127.255.255.254", "::1"]
    )
    def test_loopback_binds(self, bind: str) -> None:
        assert WebSettings(bind=bind).is_loopback_bind is True

    @pytest.mark.parametrize(
        "bind",
        ["0.0.0.0", "::", "192.0.2.15", "192.0.2.13", "203.0.113.8"],
    )
    def test_non_loopback_addresses(self, bind: str) -> None:
        assert WebSettings(bind=bind).is_loopback_bind is False

    @pytest.mark.parametrize("bind", ["compute.example.com", "host_d", ""])
    def test_hostnames_are_never_loopback(self, bind: str) -> None:
        """vq will not resolve a name to decide whether to warn: a name that
        resolves to loopback today may not tomorrow."""
        assert WebSettings(bind=bind).is_loopback_bind is False


class TestNeedsPublicBindWarning:
    def test_loopback_never_warns(self) -> None:
        assert WebSettings(bind="127.0.0.1").needs_public_bind_warning is False
        assert WebSettings(bind="::1").needs_public_bind_warning is False

    def test_public_bind_warns_when_unacked(self) -> None:
        assert WebSettings(bind="0.0.0.0").needs_public_bind_warning is True
        assert WebSettings(bind="192.0.2.15").needs_public_bind_warning is True

    def test_ack_suppresses_the_warning(self) -> None:
        assert (
            WebSettings(bind="0.0.0.0", public_bind_ack=True)
            .needs_public_bind_warning
            is False
        )

    def test_ack_from_config_section_suppresses_the_warning(self) -> None:
        cfg = _cfg(bind="0.0.0.0", public_bind_ack=True)
        assert resolve_settings(cfg).needs_public_bind_warning is False
        assert resolve_settings(_cfg(bind="0.0.0.0")).needs_public_bind_warning is (
            True
        )

    def test_ack_alone_does_not_warn_on_loopback(self) -> None:
        assert (
            WebSettings(bind="127.0.0.1", public_bind_ack=True)
            .needs_public_bind_warning
            is False
        )


class TestUrl:
    def test_renders_bind_and_port(self) -> None:
        assert WebSettings(bind="127.0.0.1", port=8765).url() == (
            "http://127.0.0.1:8765/"
        )
        assert WebSettings(bind="localhost", port=9000).url() == (
            "http://localhost:9000/"
        )
        assert WebSettings(bind="192.0.2.15", port=80).url() == (
            "http://192.0.2.15:80/"
        )

    @pytest.mark.parametrize("bind", ["0.0.0.0", "::", ""])
    def test_wildcard_binds_render_as_loopback(self, bind: str) -> None:
        """A wildcard is not a reachable address; the operator on the box
        finds the console on loopback."""
        assert WebSettings(bind=bind, port=8765).url() == "http://127.0.0.1:8765/"

    def test_bare_ipv6_literal_is_bracket_wrapped(self) -> None:
        assert WebSettings(bind="::1", port=8765).url() == "http://[::1]:8765/"
        assert WebSettings(bind="fd00::1", port=9000).url() == (
            "http://[fd00::1]:9000/"
        )

    def test_already_bracketed_ipv6_is_not_double_wrapped(self) -> None:
        assert WebSettings(bind="[::1]", port=8765).url() == "http://[::1]:8765/"


class TestDescribeSources:
    def test_reports_one_row_per_setting(self) -> None:
        rows = describe_sources(config.Config())
        assert [row["name"] for row in rows] == [
            "bind",
            "port",
            "fleet",
            "fleet_interval_seconds",
            "log_level",
            "title",
            "public_bind_ack",
        ]

    def test_all_default_when_nothing_is_set(self) -> None:
        rows = describe_sources(config.Config())
        assert {row["source"] for row in rows} == {"default"}

    def test_config_section_is_labelled_config(self) -> None:
        cfg = _cfg(
            bind="0.0.0.0",
            port=9100,
            fleet=True,
            fleet_interval_seconds=61,
            log_level="debug",
            title="fleet",
            public_bind_ack=True,
        )
        rows = {row["name"]: row for row in describe_sources(cfg)}
        assert {row["source"] for row in rows.values()} == {"config [web]"}
        assert rows["port"]["value"] == 9100
        assert rows["fleet"]["value"] is True

    def test_config_false_still_counts_as_configured(self) -> None:
        """fleet = false is a decision the operator wrote down, so it must
        not be reported as the built-in default."""
        rows = {row["name"]: row for row in describe_sources(_cfg(fleet=False))}
        assert rows["fleet"]["source"] == "config [web]"
        assert rows["fleet"]["value"] is False

    def test_env_is_labelled_with_the_variable_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_PORT, "9002")
        monkeypatch.setenv(settings.ENV_FLEET, "1")
        rows = {row["name"]: row for row in describe_sources(_cfg(bind="0.0.0.0"))}
        assert rows["port"]["source"] == "env VQ_WEB_PORT"
        assert rows["port"]["value"] == 9002
        assert rows["fleet"]["source"] == "env VQ_WEB_FLEET"
        assert rows["fleet"]["value"] is True
        assert rows["bind"]["source"] == "config [web]"
        assert rows["title"]["source"] == "default"

    def test_env_outranks_config_in_the_label(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_TITLE, "from-env")
        rows = {row["name"]: row for row in describe_sources(_cfg(title="from-cfg"))}
        assert rows["title"]["source"] == "env VQ_WEB_TITLE"
        assert rows["title"]["value"] == "from-env"

    def test_values_agree_with_resolve_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """describe_sources must not be a second, drifting resolver."""
        monkeypatch.setenv(settings.ENV_PORT, "9002")
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "warning")
        cfg = _cfg(bind="0.0.0.0", fleet=True, title="fleet", public_bind_ack=True)
        resolved = resolve_settings(cfg)
        rows = {row["name"]: row["value"] for row in describe_sources(cfg)}
        assert rows == {
            "bind": resolved.bind,
            "port": resolved.port,
            "fleet": resolved.fleet,
            "fleet_interval_seconds": resolved.fleet_interval_seconds,
            "log_level": resolved.log_level,
            "title": resolved.title,
            "public_bind_ack": resolved.public_bind_ack,
        }

    def test_reads_the_config_file_when_none_is_passed(
        self, tmp_path: Path
    ) -> None:
        _write_config(tmp_path, "[web]\nport = 9100\n")
        rows = {row["name"]: row for row in describe_sources()}
        assert rows["port"]["source"] == "config [web]"
        assert rows["port"]["value"] == 9100


class TestBrokenConfigIsNotFatal:
    def test_unparseable_config_still_resolves_defaults(
        self, tmp_path: Path
    ) -> None:
        """A console must stay startable when the config is what is broken:
        the console is one of the things an operator reaches for to find
        out why."""
        _write_config(tmp_path, "[web\nthis is not toml = = =\n")
        with pytest.raises(config.ConfigError):
            config.load_config()
        assert resolve_settings() == WebSettings()

    def test_schema_invalid_config_still_resolves_defaults(
        self, tmp_path: Path
    ) -> None:
        _write_config(tmp_path, "[web]\nport = 70000\n")
        with pytest.raises(config.ConfigError):
            config.load_config()
        assert resolve_settings().port == 8765

    def test_flags_and_env_still_apply_over_a_broken_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(tmp_path, "[web\nbroken\n")
        monkeypatch.setenv(settings.ENV_FLEET, "1")
        resolved = resolve_settings(bind="0.0.0.0", port=9003)
        assert (resolved.bind, resolved.port, resolved.fleet) == (
            "0.0.0.0",
            9003,
            True,
        )

    def test_describe_sources_survives_a_broken_config(
        self, tmp_path: Path
    ) -> None:
        _write_config(tmp_path, "[web\nbroken\n")
        rows = describe_sources()
        assert {row["source"] for row in rows} == {"default"}


class TestWithOverrides:
    def test_none_overrides_are_ignored(self) -> None:
        base = WebSettings(bind="0.0.0.0", port=9100, fleet=True)
        assert settings.with_overrides(base, bind=None, port=None) == base

    def test_concrete_overrides_are_applied(self) -> None:
        base = WebSettings(bind="0.0.0.0", port=9100, fleet=True)
        narrowed = settings.with_overrides(base, port=9200, fleet=False)
        assert narrowed.port == 9200
        assert narrowed.fleet is False
        assert narrowed.bind == "0.0.0.0"

    def test_base_is_not_mutated(self) -> None:
        base = WebSettings(bind="0.0.0.0", port=9100)
        settings.with_overrides(base, port=9200)
        assert base.port == 9100


class TestCliDelegation:
    def test_cli_helper_delegates_to_web_settings(self) -> None:
        """vq.cli._is_loopback_bind is pinned by an older suite; it must
        stay a thin wrapper over WebSettings so the CLI, app factory and
        installer cannot drift on what counts as 'exposed'."""
        from vq.cli import _is_loopback_bind

        assert _is_loopback_bind("127.0.0.1") is True
        assert _is_loopback_bind("localhost") is True
        assert _is_loopback_bind("127.1.2.3") is True
        assert _is_loopback_bind("::1") is True
        assert _is_loopback_bind("0.0.0.0") is False
        assert _is_loopback_bind("192.0.2.15") is False
        assert _is_loopback_bind("compute.example.com") is False

    @pytest.mark.parametrize(
        "host", ["localhost", "127.0.0.1", "::1", "0.0.0.0", "::", "192.0.2.13"]
    )
    def test_cli_helper_agrees_with_the_property(self, host: str) -> None:
        from vq.cli import _is_loopback_bind

        assert _is_loopback_bind(host) is WebSettings(bind=host).is_loopback_bind


class TestInvalidLayerValuesFallThrough:
    """Every layer is validated by the same rules as ``[web]``.

    Before this, the ``[web]`` section was the only layer vq validated:
    ``port = 70000`` was rejected at config load while
    ``VQ_WEB_PORT=70000`` sailed through, and ``log_level = "chatty"``
    was rejected in the file but reached ``uvicorn.run()`` from the
    environment and crashed the console at startup.
    """

    def test_out_of_range_env_port_falls_through_to_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(tmp_path, "[web]\nport = 9100\n")
        monkeypatch.setenv(settings.ENV_PORT, "70000")
        assert resolve_settings(config.load_config()).port == 9100

    def test_unknown_env_log_level_falls_through_to_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "chatty")
        assert resolve_settings(config.Config()).log_level == (
            settings.DEFAULT_LOG_LEVEL
        )

    @pytest.mark.parametrize("bad", ["", "   "])
    def test_blank_cli_bind_falls_through_rather_than_disabling_the_warning(
        self, bad: str
    ) -> None:
        """An empty bind is not a bind. Accepting one produced a settings
        object that claimed a non-loopback exposure while url() rendered
        it as loopback."""
        resolved = resolve_settings(config.Config(), bind=bad)
        assert resolved.bind == settings.DEFAULT_BIND
        assert resolved.needs_public_bind_warning is False

    def test_sub_floor_cli_interval_falls_through(self) -> None:
        resolved = resolve_settings(config.Config(), fleet_interval_seconds=1)
        assert resolved.fleet_interval_seconds == (
            settings.DEFAULT_FLEET_INTERVAL_SECONDS
        )

    def test_out_of_range_cli_port_falls_through(self) -> None:
        assert resolve_settings(config.Config(), port=0).port == (
            settings.DEFAULT_PORT
        )


class TestSourceAttributionIsHonest:
    """``vq web config`` must not blame a layer for a value it did not set.

    The regression: ``_source()`` asked only whether an environment
    variable was *set*, so a typo'd ``VQ_WEB_PORT=notanint`` reported the
    built-in default 8765 under the label ``env VQ_WEB_PORT`` -- pointing
    the operator away from the very typo they were debugging.
    """

    def test_unparseable_env_is_not_credited_with_the_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_PORT, "notanint")
        row = next(r for r in describe_sources() if r["name"] == "port")
        assert row["value"] == settings.DEFAULT_PORT
        assert row["source"] == "default"

    def test_rejected_env_is_not_credited_over_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(tmp_path, "[web]\nport = 9100\n")
        monkeypatch.setenv(settings.ENV_PORT, "70000")
        row = next(r for r in describe_sources() if r["name"] == "port")
        assert row["value"] == 9100
        assert row["source"] == "config [web]"

    def test_garbage_env_bool_is_not_credited(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_FLEET, "maybe")
        row = next(r for r in describe_sources() if r["name"] == "fleet")
        assert row["value"] is False
        assert row["source"] == "default"

    def test_valid_env_is_credited(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(settings.ENV_PORT, "9200")
        row = next(r for r in describe_sources() if r["name"] == "port")
        assert row["value"] == 9200
        assert row["source"] == f"env {settings.ENV_PORT}"

    def test_described_values_match_resolve_settings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The two must share one resolution pass, so they cannot drift."""
        _write_config(tmp_path, '[web]\nbind = "0.0.0.0"\nfleet = true\n')
        monkeypatch.setenv(settings.ENV_PORT, "9300")
        monkeypatch.setenv(settings.ENV_LOG_LEVEL, "bogus")
        resolved = resolve_settings(config.load_config())
        for row in describe_sources():
            assert row["value"] == getattr(resolved, str(row["name"]))


class TestABrokenConfigIsNotSilent:
    """#40: the fallback above is deliberate, and must say it was taken.

    With an invalid config, `vq web config` used to exit 0 and present every
    built-in default as "the resolved configuration", under a precedence line
    naming `[web] in config`, and `vq web run` started a console that ignored
    `[web]` without a word.
    """

    INVALID = 'default_pool = "nonexistent"\n[web]\nfleet = true\nport = 9111\n'

    def test_a_loadable_config_reports_no_failure(self, tmp_path: Path) -> None:
        _write_config(tmp_path, "[web]\nport = 9111\n")
        assert settings.config_load_failure() is None

    @pytest.mark.parametrize(
        "body", [INVALID, "[web\nbroken\n"], ids=["schema-invalid", "unparseable"],
    )
    def test_a_broken_config_names_why(self, tmp_path: Path, body: str) -> None:
        _write_config(tmp_path, body)
        failure = settings.config_load_failure()
        assert failure is not None
        # "invalid config in <path>: …" or "failed to parse <path>: …".
        assert "config.toml" in failure

    def test_an_unreadable_config_names_why(self, tmp_path: Path) -> None:
        import os

        if os.geteuid() == 0:
            pytest.skip("root reads a mode-000 file")
        path = _write_config(tmp_path, "[web]\nport = 9111\n")
        path.chmod(0)
        try:
            assert settings.config_load_failure() is not None
        finally:
            path.chmod(0o644)

    def test_vq_web_config_warns_and_still_shows_the_values(
        self, tmp_path: Path,
    ) -> None:
        from click.testing import CliRunner

        from vq.cli import main

        _write_config(tmp_path, self.INVALID)

        result = CliRunner().invoke(main, ["web", "config"])

        assert result.exit_code == 0, result.output
        assert "resolved configuration" in result.stdout
        assert "could not be loaded" in result.stderr
        assert "[web] was ignored" in result.stderr
        assert "default_pool" in result.stderr

    def test_vq_web_config_json_carries_the_failure(self, tmp_path: Path) -> None:
        import json

        from click.testing import CliRunner

        from vq.cli import main

        _write_config(tmp_path, self.INVALID)

        result = CliRunner().invoke(main, ["web", "config", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["config_error"].startswith("invalid config in ")
        assert {row["source"] for row in payload["settings"]} == {"default"}

    def test_vq_web_config_json_has_a_null_failure_when_it_loads(
        self, tmp_path: Path,
    ) -> None:
        import json

        from click.testing import CliRunner

        from vq.cli import main

        _write_config(tmp_path, "[web]\nport = 9111\n")

        result = CliRunner().invoke(main, ["web", "config", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["config_error"] is None
        assert "could not be loaded" not in result.stderr

    def test_vq_web_run_says_it_is_ignoring_the_config(self, tmp_path: Path) -> None:
        from unittest.mock import patch

        from click.testing import CliRunner

        from vq.cli import main

        pytest.importorskip("uvicorn")
        _write_config(tmp_path, self.INVALID)

        with patch("uvicorn.run") as run, patch("vq.web.app", new=object()):
            result = CliRunner().invoke(main, ["web", "run"])

        assert result.exit_code == 0, result.output
        assert run.called
        assert "could not be loaded" in result.stderr
        assert "[web] is ignored" in result.stderr
