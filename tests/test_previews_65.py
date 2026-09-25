"""Calculation previews from a bounded output tail (#65, V0).

Fixtures follow vibe-qc's golden output shapes (tests/golden/*.out in vibe-qc,
whose digits are masked) with plausible values filled in.
"""
from __future__ import annotations

from pathlib import Path

from vq import previews

RKS = """\
  Job: RKS / pbe  basis=def2-svp
  Atoms (bohr)
  ------------------------------------------------------------
     0  Z=  8       0.00000000      0.00000000      0.22143139
     1  Z=  1       0.00000000      1.43042809     -0.88572555
     2  Z=  1       0.00000000     -1.43042809     -0.88572555
  charge=0  multiplicity=1  n_electrons=10
  IUPAC name: water
  Threads: 8  (OpenMP shared-memory parallelism)

  iter     energy (Ha)            dE          ||[F,DS]||   DIIS
  -------------------------------------------------------------
     1      -76.1234567890        --     4.215e-01    0
     2      -76.3012345678  -1.778e-01  8.113e-02    2
     3      -76.3291234567  -2.789e-02  1.402e-02    3
     4      -76.3298765432  -7.531e-04  9.874e-05    4
  -------------------------------------------------------------
  converged in 4 iterations; E = -76.3298765432 Ha

  Energy components (Ha)
  ---------------------------------------------------
  Total energy                         -76.3298765432
  ---------------------------------------------------
  HOMO: -0.231456 Ha (-6.2982 eV); LUMO: 0.021234 Ha (0.5778 eV); gap: 0.252690 Ha (6.8760 eV)
"""


def test_a_molecular_run_yields_identity_series_and_headline() -> None:
    value = previews.preview("vibeqc-release", RKS)

    assert value is not None
    assert value["schema"] == "vq.calc-preview/1" and value["parser"] == "vibe-qc"
    assert (value["method"], value["basis"], value["name"]) == ("RKS / pbe", "def2-svp", "water")
    assert value["formula"] == "H2O" and value["atoms"] == 3
    assert [p["iter"] for p in value["scf"]] == [1, 2, 3, 4]
    assert value["scf"][0]["delta"] is None
    assert value["scf"][-1]["residual"] == 9.874e-05
    assert value["scf_energies"] == [-76.3298765432]
    assert value["headline"] == {"total_energy": -76.3298765432, "gap_ev": 6.876}
    assert value["optimisation"] is None


def test_a_tail_without_the_header_still_yields_the_series() -> None:
    tail = RKS[RKS.index("  iter"):]

    value = previews.preview("vibe-qc", tail)

    assert value is not None
    assert value["method"] is None and value["formula"] is None
    assert len(value["scf"]) == 4


def test_an_optimisation_keeps_step_energies_and_only_the_latest_scf() -> None:
    step = RKS[RKS.index("  iter"):RKS.index("\n  Energy components")]
    text = (RKS[:RKS.index("  iter")] + step
            + step.replace("-76.3298765432", "-76.3301000000")
            + "\n  Geometry optimization: 2 iters, E = -76.33010000 Ha, "
              "max|grad| = 3.2100e-04 Ha/bohr, converged=True\n")

    value = previews.preview("vibeqc-dev", text)

    assert value is not None
    assert value["scf_energies"] == [-76.3298765432, -76.3301]
    assert len(value["scf"]) == 4
    assert value["optimisation"] == {
        "iterations": 2, "energy": -76.3301, "max_gradient": 3.21e-04, "converged": True,
    }


def test_periodic_header_and_carbon_first_formula() -> None:
    text = """\
  Job: PERIODIC RHF  basis=sto-3g
  Periodicity
  -----------------------------------
    dimensionality = 3D
  Atoms (bohr) -- 3 in unit cell
  --------------------------------------------------------------
       0  Z=  6       0.00000000      0.00000000      0.00000000
       1  Z=  1       1.00000000      0.00000000      0.00000000
       2  Z=  8       2.00000000      0.00000000      0.00000000
    n_electrons = 15  multiplicity = 2
"""
    value = previews.preview("vibeqc-release", text)

    assert value is not None
    assert value["periodic_dimensions"] == 3
    assert value["formula"] == "CHO" and value["atoms"] == 3


def test_series_are_capped_to_the_most_recent_points() -> None:
    rows = "".join(
        f"  {i:4d}      -1.{i:010d}  -1.000e-05  1.000e-03    1\n" for i in range(1, 501)
    )
    value = previews.preview("vibeqc", "  iter     energy (Ha)   dE\n" + rows)

    assert value is not None
    assert len(value["scf"]) == previews.MAX_SERIES
    assert value["scf"][-1]["iter"] == 500


def test_unknown_programs_and_garbage_are_harmless() -> None:
    assert previews.preview("cp2k", RKS) is None
    assert previews.preview(None, RKS) is None
    value = previews.preview("vibeqc", "\x00\xff garbage\n  iter energy\n  1 x y z\n")
    assert value is not None and value["scf"] == [] and value["method"] is None


def test_read_tail_is_bounded_and_drops_the_partial_first_line(tmp_path: Path) -> None:
    out = tmp_path / "output.out"
    out.write_text("A" * 100 + "\n" + "second line\n" + "third line\n")

    assert previews.read_tail(out, max_bytes=30) == "second line\nthird line\n"
    assert previews.read_tail(out, max_bytes=23) == "second line\nthird line\n"
    assert previews.read_tail(out, max_bytes=22) == "third line\n"
    assert previews.read_tail(out) == out.read_text()


def test_hill_formula_orders_and_rejects_unknown_numbers() -> None:
    assert previews.hill_formula([8, 1, 1]) == "H2O"
    assert previews.hill_formula([6, 6, 1, 1, 1, 1, 8]) == "C2H4O"
    assert previews.hill_formula([3, 9]) == "FLi"
    assert previews.hill_formula([0]) is None


def test_a_fatal_message_is_surfaced_and_bounded() -> None:
    text = "  Job: MSINDO  basis=sto-3g\n  FATAL: " + "optimize is not supported " * 30 + "\n"

    value = previews.preview("vibeqc", text)

    assert value is not None
    assert value["error"].startswith("optimize is not supported")
    assert len(value["error"]) == previews.MAX_MESSAGE


ORCA_CYCLE = """\
---------------------------------
CARTESIAN COORDINATES (ANGSTROEM)
---------------------------------
  O      0.000000    0.000000    0.117300
  H      0.000000    0.757200   -0.469200
  H      0.000000   -0.757200   -0.469200

Iteration    Energy (Eh)           Delta-E    RMSDP     MaxDP     DIISErr   Damp  Time(sec)
-------------------------------------------------------------------------------------------
    1    -75.9500000000000000     0.00e+00  3.18e-03  1.77e-02  9.33e-02  0.700   0.1
    2    -75.9800000000000000    -3.00e-02  2.52e-03  1.45e-02  6.77e-02  0.700   0.1
Iteration    Energy (Eh)           Delta-E    RMSDP     MaxDP     MaxGrad    Time(sec)
--------------------------------------------------------------------------------------
    3    -75.9851000000000000    -5.10e-03  5.58e-04  5.42e-03  1.81e-03     0.1
    4    -75.9851200000000000    -2.00e-05  7.88e-07  5.73e-06  3.03e-06     0.1
FINAL SINGLE POINT ENERGY       -75.985120000000
"""

ORCA = (
    "|  1> ! RKS B3LYP def2-SVP Opt\n|  2> * xyz 0 1\n"
    "Your calculation utilizes the basis: def2-SVP\n"
    "                        *                GEOMETRY OPTIMIZATION CYCLE   1            *\n"
    + ORCA_CYCLE
    + "          MAX gradient        0.0023456789            0.0003000000      NO\n"
    "                        *                GEOMETRY OPTIMIZATION CYCLE   2            *\n"
    + ORCA_CYCLE.replace("-75.985120000000", "-75.986000000000")
    + "          MAX gradient        0.0001000000            0.0003000000      YES\n"
    "                    ***********************HURRAY********************\n"
    "                    ***        THE OPTIMIZATION HAS CONVERGED     ***\n"
)


def test_an_orca_optimisation_yields_identity_series_and_state() -> None:
    value = previews.preview("orca", ORCA)

    assert value is not None and value["parser"] == "orca"
    assert (value["method"], value["basis"]) == ("RKS B3LYP def2-SVP Opt", "def2-SVP")
    assert value["formula"] == "H2O" and value["atoms"] == 3
    assert [p["iter"] for p in value["scf"]] == [1, 2, 3, 4]
    assert value["scf"][-1]["residual"] == 7.88e-07
    assert value["scf_energies"] == [-75.98512, -75.986]
    assert value["headline"]["total_energy"] == -75.986
    assert value["optimisation"] == {
        "iterations": 2, "energy": -75.986, "max_gradient": 1e-04, "converged": True,
    }


def test_an_orca_error_termination_is_surfaced() -> None:
    value = previews.preview("orca-6", "|  1> ! RHF STO-3G\n"
                             "ORCA finished by error termination in Startup\n")

    assert value is not None
    assert value["error"] == "ORCA error termination in Startup"
    assert value["optimisation"] is None and value["scf"] == []
