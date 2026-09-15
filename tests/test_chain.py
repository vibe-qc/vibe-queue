"""v0.8.7 *Hoare's Triple* — `vq submit --chain N` tests.

Pins the v0.8.7 contract:

1. **Spec gains chain_index / chain_total / chain_group_id fields.**
2. **`submit_local_chain` spawns N specs with linked depends_on.**
3. **Element 0 has no chain dep; element k (k>0) depends on k-1.**
4. **Cascade-fail propagates** (inherits depends_on semantic).
5. **CLI `--chain N`** spawns the chain; prints N jobids.
6. **`--chain` and `--array` are mutually exclusive.**
7. **`--chain` composes with user `--depends-on`** (chain element 0
   carries both the user dep + no chain dep; element k carries
   user deps + the k-1 chain dep).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config as _config
from vq import paths
from vq.cli import main
from vq.spec import JobSpec, JobState
from vq.submit import submit_local_chain


@pytest.fixture
def chain_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Path:
    """Hermetic state + config dir for chain tests."""
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    (cfg / "config.toml").write_text(
        "default_host = \"localhost\"\n"
        "[hosts.localhost]\nssh = \"localhost\"\n"
    )
    monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))
    return tmp_path


# ----------------------------------------------------------------------
# Spec field roundtrip
# ----------------------------------------------------------------------


class TestSpecChainFields:
    def test_default_none(self) -> None:
        s = JobSpec(id="abc", command=["python", "x.py"], cwd="/tmp", cpus=1)
        assert s.chain_index is None
        assert s.chain_total is None
        assert s.chain_group_id is None

    def test_roundtrip(self, tmp_path: Path) -> None:
        s = JobSpec(
            id="abc", command=["python", "x.py"], cwd="/tmp", cpus=1,
            chain_index=0, chain_total=5, chain_group_id="cafef00d",
        )
        path = tmp_path / "spec.json"
        s.write(path)
        loaded = JobSpec.read(path)
        assert loaded.chain_index == 0
        assert loaded.chain_total == 5
        assert loaded.chain_group_id == "cafef00d"

    def test_negative_chain_index_rejected(self) -> None:
        with pytest.raises(ValueError):
            JobSpec(
                id="abc", command=["python", "x.py"], cwd="/tmp", cpus=1,
                chain_index=-1, chain_total=5, chain_group_id="x",
            )


# ----------------------------------------------------------------------
# submit_local_chain
# ----------------------------------------------------------------------


class TestSubmitLocalChain:
    def test_spawns_n_specs(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        jobids = submit_local_chain(
            chain=4, host="localhost", input_file=str(script),
        )
        assert len(jobids) == 4
        # Each unique.
        assert len(set(jobids)) == 4

    def test_each_element_has_chain_fields(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        jobids = submit_local_chain(
            chain=3, host="localhost", input_file=str(script),
        )
        specs = [
            JobSpec.read(paths.queue_dir() / f"{j}.json")
            for j in jobids
        ]
        for idx, spec in enumerate(specs):
            assert spec.chain_index == idx
            assert spec.chain_total == 3
            assert spec.chain_group_id is not None
        # All share the same group id.
        gids = {s.chain_group_id for s in specs}
        assert len(gids) == 1

    def test_element_0_has_no_chain_dep(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('first')\n")
        jobids = submit_local_chain(
            chain=3, host="localhost", input_file=str(script),
        )
        spec0 = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        # No user-supplied deps either, so element 0 should be empty.
        assert spec0.depends_on == []

    def test_element_k_depends_on_k_minus_1(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('iter')\n")
        jobids = submit_local_chain(
            chain=4, host="localhost", input_file=str(script),
        )
        for k in range(1, 4):
            spec_k = JobSpec.read(
                paths.queue_dir() / f"{jobids[k]}.json",
            )
            assert spec_k.depends_on == [jobids[k - 1]], (
                f"element {k} should depend on element {k - 1}"
            )

    def test_user_depends_on_merged_into_every_element(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        # Seed a fake predecessor in the queue so submit-time
        # validation doesn't reject it.
        script = tmp_path / "neb.py"
        script.write_text("print('a')\n")
        pred_jobid = "deadbeef0000"
        pred_spec = JobSpec(
            id=pred_jobid, command=["true"], cwd=str(tmp_path), cpus=1,
            state=JobState.PENDING,
        )
        pred_spec.write(paths.queue_dir() / f"{pred_jobid}.json")

        jobids = submit_local_chain(
            chain=3, host="localhost", input_file=str(script),
            depends_on=[pred_jobid],
        )
        # Element 0: just the user dep.
        spec0 = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        assert spec0.depends_on == [pred_jobid]
        # Element k: user dep + previous chain element.
        for k in range(1, 3):
            spec_k = JobSpec.read(
                paths.queue_dir() / f"{jobids[k]}.json",
            )
            assert pred_jobid in spec_k.depends_on
            assert jobids[k - 1] in spec_k.depends_on

    def test_chain_1_is_single_submit(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        """chain=1 still spawns one element with chain_total=1 — the
        operator explicitly asked for chain semantics; we honour it."""
        script = tmp_path / "neb.py"
        script.write_text("print('one')\n")
        jobids = submit_local_chain(
            chain=1, host="localhost", input_file=str(script),
        )
        assert len(jobids) == 1
        spec = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        assert spec.chain_index == 0
        assert spec.chain_total == 1

    def test_scheduler_target_recorded_on_each_element(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        jobids = submit_local_chain(
            chain=3,
            host="localhost",
            input_file=str(script),
            # Cluster-side interpreter: a scheduler-target single-file submit
            # rejects the driver's own (see TestSchedulerTargetInterpreter in
            # tests/test_submit.py).
            python="/home/USER/venv/bin/python",
            scheduler_target="host_f",
        )

        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.scheduler_target for spec in specs} == {"host_f"}
        assert specs[1].depends_on == [jobids[0]]
        assert specs[2].depends_on == [jobids[1]]

    def test_rerun_until_recorded_on_each_element(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        jobids = submit_local_chain(
            chain=3,
            host="localhost",
            input_file=str(script),
            rerun_until_file_exists="$VQ_WORKDIR/DONE",
            rerun_max=6,
        )

        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.rerun_until_file_exists for spec in specs} == {
            "$VQ_WORKDIR/DONE"
        }
        assert {spec.rerun_max for spec in specs} == {6}
        assert specs[1].depends_on == [jobids[0]]
        assert specs[2].depends_on == [jobids[1]]

    def test_chain_0_rejected(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('?')\n")
        with pytest.raises(ValueError, match="--chain must be >= 1"):
            submit_local_chain(
                chain=0, host="localhost", input_file=str(script),
            )


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


class TestSubmitChainCLI:
    def test_chain_flag_spawns_n_jobids(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        runner = CliRunner()
        result = runner.invoke(
            main, ["submit", "--chain", "3", str(script)],
        )
        assert result.exit_code == 0, result.output
        lines = [
            ln.strip() for ln in result.output.splitlines() if ln.strip()
        ]
        # Should print 3 jobids (12-hex each).
        assert len(lines) == 3
        for ln in lines:
            assert len(ln) == 12

    def test_chain_rerun_until_reaches_each_spec(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('image')\n")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--chain",
                "2",
                "--rerun-until",
                "$VQ_WORKDIR/DONE",
                "--rerun-max",
                "7",
                str(script),
            ],
        )
        assert result.exit_code == 0, result.output
        jobids = [line.strip() for line in result.output.splitlines() if line.strip()]
        assert len(jobids) == 2
        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.rerun_until_file_exists for spec in specs} == {
            "$VQ_WORKDIR/DONE"
        }
        assert {spec.rerun_max for spec in specs} == {7}
        assert specs[1].depends_on == [jobids[0]]

    def test_chain_and_array_mutually_exclusive(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        script = tmp_path / "neb.py"
        script.write_text("print('?')\n")
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["submit", "--chain", "3", "--array", "2", str(script)],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_chain_to_remote_host_rejected(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        # REMOTE-1: --chain to a remote host must fail loudly, not silently
        # drop the chain linkage and run a single link.
        script = tmp_path / "neb.py"
        script.write_text("print('?')\n")
        runner = CliRunner()
        result = runner.invoke(
            main, ["submit", "--chain", "3", "--host", "host_d", str(script)],
        )
        assert result.exit_code != 0
        assert "local-only" in result.output

    def test_rerun_until_to_remote_host_rejected(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        # REMOTE-1: --rerun-until to a remote host must fail loudly, not
        # silently run once and report COMPLETED without converging.
        script = tmp_path / "dft_u.py"
        script.write_text("print('?')\n")
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["submit", "--rerun-until", "CONVERGED", "--host", "host_d", str(script)],
        )
        assert result.exit_code != 0
        assert "local-only" in result.output


# ----------------------------------------------------------------------
# Daemon env injection (smoke — full dispatch tested in test_daemon)
# ----------------------------------------------------------------------


class TestChainEnvFieldsPresent:
    def test_chain_fields_carry_to_spec(
        self, chain_state: Path, tmp_path: Path,
    ) -> None:
        """Sanity: the chain fields the daemon expects to inject as
        VQ_CHAIN_* env vars are present on the spec after a chain
        submit. The actual subprocess-env injection lives in
        daemon._start_job; this just pins the spec carries the
        data."""
        script = tmp_path / "iter.py"
        script.write_text("print('go')\n")
        jobids = submit_local_chain(
            chain=2, host="localhost", input_file=str(script),
        )
        spec0 = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        # These three are exactly what daemon._start_job's v0.8.7
        # block reads to populate VQ_CHAIN_INDEX/TOTAL/GROUP_ID.
        assert spec0.chain_index is not None
        assert spec0.chain_total is not None
        assert spec0.chain_group_id is not None
