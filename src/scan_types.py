"""
scan_types.py — Scan-type definitions for the AMBER/HiRRIXS DAQ tab.

Each scan type provides:
  LABEL            str          display name shown in the combo box
  build_widget()   QWidget      parameter panel swapped into QStackedWidget
  build_positions() list[dict]  ordered step sequence
  plot_axes()      (str, str)   (x_label, y_label) for the live plot
  scan_label()     str          one-liner written to the acquisition log
  n_outer()        int          outer loop depth (1 for 1D/Time, N for 2D)
  outer_index()    int          which outer step a flat index belongs to
  outer_motor()    str|None     outer motor name (2D only)

Position dict keys
──────────────────
  "_x"       float   value used as x-coordinate in the live plot
  "_time_s"  float   elapsed-time marker (TimeScan only; no motor to move)
  <name>     float   motor setpoint — move every key that is not "_"-prefixed
"""

import math
import random
from abc import ABC, abstractmethod

from PySide6.QtWidgets import (
    QWidget, QGridLayout, QLabel, QComboBox,
    QSpinBox, QDoubleSpinBox, QCheckBox,
)

from common import PAL, COMBO_STYLE

# ── Widget style helpers (local, avoids circular import from daq_tab) ─────────
_SPIN = f"""
    QSpinBox, QDoubleSpinBox {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        padding:3px 6px; font-family:monospace;
    }}
    QSpinBox:focus, QDoubleSpinBox:focus {{ border-color:{PAL['accent']}; }}
    QSpinBox::up-button,   QDoubleSpinBox::up-button,
    QSpinBox::down-button, QDoubleSpinBox::down-button {{
        background:{PAL['surface']}; border:none; width:16px;
    }}
"""
_CHECK = f"""
    QCheckBox {{ color:{PAL['text']}; spacing:6px; }}
    QCheckBox::indicator {{
        width:14px; height:14px; border:1px solid #2a3a5e;
        border-radius:3px; background:{PAL['bg']};
    }}
    QCheckBox::indicator:checked {{
        background:{PAL['accent']}; border-color:{PAL['accent']};
    }}
"""


def _ql(text: str) -> QLabel:
    lb = QLabel(text)
    lb.setStyleSheet(f"color:{PAL['subtext']};")
    return lb


def _dbl(val, lo=-1e6, hi=1e6, dec=4) -> QDoubleSpinBox:
    sp = QDoubleSpinBox()
    sp.setRange(lo, hi); sp.setValue(val); sp.setDecimals(dec)
    sp.setStyleSheet(_SPIN)
    return sp


def _int(val, lo=2, hi=10000) -> QSpinBox:
    sp = QSpinBox()
    sp.setRange(lo, hi); sp.setValue(val); sp.setStyleSheet(_SPIN)
    return sp


# ── Detector kind ─────────────────────────────────────────────────────────────
DET_SCALAR = "scalar"
DET_AREA   = "area"

_AREA_LABELS = {"scalar": "scalar",  "area": "area detector"}
_AREA_COLORS = {"scalar": PAL["ok"], "area": PAL["warn"]}


def detector_kind(name: str, signal_pvs: dict, det_pvs: dict) -> str:
    """Return DET_SCALAR or DET_AREA based on which config section owns name."""
    if name in signal_pvs: return DET_SCALAR
    if name in det_pvs:    return DET_AREA
    return DET_SCALAR


# ── Base class ────────────────────────────────────────────────────────────────
class BaseScan(ABC):
    """Abstract base for all scan types."""

    LABEL: str = "Scan"

    @abstractmethod
    def build_widget(self, motor_names: list[str]) -> QWidget:
        """Return the parameter panel for this scan type."""

    @abstractmethod
    def build_positions(self) -> list[dict]:
        """Return ordered list of position dicts (see module docstring)."""

    @abstractmethod
    def plot_axes(self) -> tuple[str, str]:
        """Return (x_label, y_label) for the live plot."""

    @abstractmethod
    def scan_label(self) -> str:
        """One-line description for the acquisition log."""

    def n_total(self) -> int:
        return len(self.build_positions())

    def n_outer(self) -> int:
        """Number of outer loop iterations (1 for flat scans)."""
        return 1

    def outer_index(self, flat_idx: int) -> int:
        """Which outer step does flat_idx belong to."""
        return 0

    def outer_motor(self) -> "str | None":
        """Outer motor name; None if not a 2-D scan."""
        return None

    @abstractmethod
    def to_plan(self, motor_dev: str, det_dev: str) -> "tuple[str, list, dict]":
        """Return (plan_name, args, kwargs) for queue server submission.

        Parameters
        ----------
        motor_dev : ophyd device name of the scan motor (string identifier
                    that exists in the RE Manager worker namespace).
        det_dev   : ophyd device name of the detector / signal.

        Returns
        -------
        plan_name : str    e.g. "scan", "grid_scan", "count"
        args      : list   positional arguments
        kwargs    : dict   keyword arguments
        """

# ══════════════════════════════════════════════════════════════════════════════
# 1-D Scan
# ══════════════════════════════════════════════════════════════════════════════
class _Scan1DWidget(QWidget):
    def __init__(self, motor_names: list[str], parent=None):
        super().__init__(parent)
        gl = QGridLayout(self)
        gl.setContentsMargins(0, 4, 0, 4); gl.setSpacing(6)

        gl.addWidget(_ql("Motor"), 0, 0)
        self._motor = QComboBox(); self._motor.setStyleSheet(COMBO_STYLE)
        for n in motor_names: self._motor.addItem(n)
        gl.addWidget(self._motor, 0, 1, 1, 3)

        gl.addWidget(_ql("Start"),  1, 0)
        self._start = _dbl(-10.0); gl.addWidget(self._start, 1, 1, 1, 3)

        gl.addWidget(_ql("Stop"),   2, 0)
        self._stop  = _dbl( 10.0); gl.addWidget(self._stop,  2, 1, 1, 3)

        gl.addWidget(_ql("Steps"),  3, 0)
        self._steps = _int(21);    gl.addWidget(self._steps, 3, 1, 1, 3)

        self._rel = QCheckBox("Relative (from current position)")
        self._rel.setStyleSheet(_CHECK)
        gl.addWidget(self._rel, 4, 0, 1, 4)

    def params(self) -> dict:
        return dict(motor   = self._motor.currentText(),
                    start   = self._start.value(),
                    stop    = self._stop.value(),
                    steps   = self._steps.value(),
                    relative= self._rel.isChecked())

    def set_motor_names(self, names: list[str]):
        cur = self._motor.currentText()
        self._motor.blockSignals(True)
        self._motor.clear()
        for n in names: self._motor.addItem(n)
        idx = self._motor.findText(cur)
        if idx >= 0: self._motor.setCurrentIndex(idx)
        self._motor.blockSignals(False)


class Scan1D(BaseScan):
    LABEL = "1-D Scan"

    def __init__(self):
        self._widget: "_Scan1DWidget | None" = None

    def build_widget(self, motor_names):
        self._widget = _Scan1DWidget(motor_names)
        return self._widget

    def build_positions(self) -> list[dict]:
        if self._widget is None: return []
        p = self._widget.params()
        pts = _linspace(p["start"], p["stop"], p["steps"])
        m   = p["motor"]
        return [{"_x": x, m: x} for x in pts]

    def plot_axes(self):
        if self._widget is None: return ("motor", "detector")
        return (self._widget.params()["motor"], "detector")

    def scan_label(self):
        if self._widget is None: return "1-D Scan"
        p = self._widget.params()
        return (f"1-D  {p['motor']}  [{p['start']:.4g} → {p['stop']:.4g}]"
                f"  {p['steps']} pts"
                + ("  (relative)" if p["relative"] else ""))

    def to_plan(self, motor_dev: str, det_dev: str) -> tuple[str, list, dict]:
        if self._widget is None:
            raise RuntimeError("Scan1D widget not built yet")
        p = self._widget.params()
        # num must be positional — the worker's custom scan plan takes
        # (detectors, motor, start, stop, num) all as positional args
        return (
            "scan",
            [[det_dev], motor_dev, p["start"], p["stop"], p["steps"]],
            {},
        )

# ══════════════════════════════════════════════════════════════════════════════
# 2-D Scan
# ══════════════════════════════════════════════════════════════════════════════
class _Scan2DWidget(QWidget):
    def __init__(self, motor_names: list[str], parent=None):
        super().__init__(parent)
        gl = QGridLayout(self)
        gl.setContentsMargins(0, 4, 0, 4); gl.setSpacing(6)

        # Column headers
        for col, txt in enumerate(("", "Motor", "Start", "Stop", "Steps")):
            h = QLabel(txt)
            h.setStyleSheet(f"color:{PAL['accent']}; font-size:8pt;")
            gl.addWidget(h, 0, col)

        self._rows: dict[str, tuple] = {}
        for row_i, axis in enumerate(("Outer", "Inner"), 1):
            gl.addWidget(_ql(axis), row_i, 0)
            cb = QComboBox(); cb.setStyleSheet(COMBO_STYLE)
            for n in motor_names: cb.addItem(n)
            # Default inner to second motor so both axes differ
            if axis == "Inner" and len(motor_names) > 1:
                cb.setCurrentIndex(1)
            gl.addWidget(cb, row_i, 1)
            st = _dbl(-10.0); gl.addWidget(st, row_i, 2)
            sp = _dbl( 10.0); gl.addWidget(sp, row_i, 3)
            ns = _int(11, lo=2, hi=500); gl.addWidget(ns, row_i, 4)
            self._rows[axis] = (cb, st, sp, ns)

        self._snake = QCheckBox("Snake scan (alternate inner direction)")
        self._snake.setStyleSheet(_CHECK); self._snake.setChecked(True)
        gl.addWidget(self._snake, 3, 0, 1, 5)

        self._rel = QCheckBox("Relative (from current position)")
        self._rel.setStyleSheet(_CHECK)
        gl.addWidget(self._rel, 4, 0, 1, 5)

    def _row_params(self, key: str) -> dict:
        cb, st, sp, ns = self._rows[key]
        return dict(motor=cb.currentText(),
                    start=st.value(), stop=sp.value(), steps=ns.value())

    def params(self) -> dict:
        return dict(outer   = self._row_params("Outer"),
                    inner   = self._row_params("Inner"),
                    snake   = self._snake.isChecked(),
                    relative= self._rel.isChecked())

    def set_motor_names(self, names: list[str]):
        for axis, (cb, *_) in self._rows.items():
            cur = cb.currentText()
            cb.blockSignals(True); cb.clear()
            for n in names: cb.addItem(n)
            idx = cb.findText(cur)
            if idx >= 0: cb.setCurrentIndex(idx)
            cb.blockSignals(False)


class Scan2D(BaseScan):
    LABEL = "2-D Scan"

    def __init__(self):
        self._widget: "_Scan2DWidget | None" = None

    def build_widget(self, motor_names):
        self._widget = _Scan2DWidget(motor_names)
        return self._widget

    def build_positions(self) -> list[dict]:
        if self._widget is None: return []
        p = self._widget.params()
        o, i_, snake = p["outer"], p["inner"], p["snake"]
        o_pts = _linspace(o["start"], o["stop"], o["steps"])
        i_pts = _linspace(i_["start"], i_["stop"], i_["steps"])
        positions = []
        for oi, xo in enumerate(o_pts):
            row = i_pts if (not snake or oi % 2 == 0) else list(reversed(i_pts))
            for xi in row:
                positions.append({"_x": xi, o["motor"]: xo, i_["motor"]: xi})
        return positions

    def plot_axes(self):
        if self._widget is None: return ("inner motor", "detector")
        return (self._widget.params()["inner"]["motor"], "detector")

    def n_outer(self) -> int:
        if self._widget is None: return 1
        return self._widget.params()["outer"]["steps"]

    def outer_index(self, flat_idx: int) -> int:
        if self._widget is None: return 0
        return flat_idx // self._widget.params()["inner"]["steps"]

    def outer_motor(self) -> "str | None":
        if self._widget is None: return None
        return self._widget.params()["outer"]["motor"]

    def scan_label(self):
        if self._widget is None: return "2-D Scan"
        p = self._widget.params()
        o, i_ = p["outer"], p["inner"]
        n_tot  = o["steps"] * i_["steps"]
        return (f"2-D  {o['motor']} ({o['steps']} pts)"
                f" × {i_['motor']} ({i_['steps']} pts)"
                f" = {n_tot} total"
                f"  {'snake' if p['snake'] else 'raster'}"
                + ("  (relative)" if p["relative"] else ""))
 
    def to_plan(self, motor_dev: str, det_dev: str) -> tuple[str, list, dict]:
        """motor_dev here is the *inner* motor (matches plot_axes convention).
        The outer motor device name is looked up from the widget directly.
        Caller must pass the inner motor dev name; outer is read internally.
        """
        if self._widget is None:
            raise RuntimeError("Scan2D widget not built yet")
        p  = self._widget.params()
        o  = p["outer"]
        i_ = p["inner"]
        return (
            "grid_scan",
            [
                [det_dev],
                f"__outer__:{o['motor']}",   # resolved by DAQTab
                motor_dev,
                o["start"], i_["start"],
                o["stop"],  i_["stop"],
                o["steps"], i_["steps"],
                p["snake"],
            ],
            {},
        )

# ══════════════════════════════════════════════════════════════════════════════
# Time Scan
# ══════════════════════════════════════════════════════════════════════════════
class _TimeScanWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        gl = QGridLayout(self)
        gl.setContentsMargins(0, 4, 0, 4); gl.setSpacing(6)

        gl.addWidget(_ql("Duration (s)"),  0, 0)
        self._dur = _dbl(60.0, lo=1, hi=86400, dec=1)
        gl.addWidget(self._dur, 0, 1)

        gl.addWidget(_ql("Interval (s)"), 1, 0)
        self._ivl = _dbl(1.0, lo=0.1, hi=3600, dec=2)
        gl.addWidget(self._ivl, 1, 1)

        self._info = QLabel("")
        self._info.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        gl.addWidget(self._info, 2, 0, 1, 2)

        self._dur.valueChanged.connect(self._update_info)
        self._ivl.valueChanged.connect(self._update_info)
        self._update_info()

    def _update_info(self):
        n = self._n_pts()
        self._info.setText(f"{n} points  ({self._dur.value():.1f} s total)")

    def _n_pts(self) -> int:
        return max(1, int(self._dur.value() / max(self._ivl.value(), 1e-6)))

    def params(self) -> dict:
        return dict(duration=self._dur.value(), interval=self._ivl.value(),
                    n_pts=self._n_pts())


class TimeScan(BaseScan):
    LABEL = "Time Scan"

    def __init__(self):
        self._widget: "_TimeScanWidget | None" = None

    def build_widget(self, motor_names):          # motor_names not used
        self._widget = _TimeScanWidget()
        return self._widget

    def build_positions(self) -> list[dict]:
        if self._widget is None: return []
        p = self._widget.params()
        return [{"_x": i * p["interval"], "_time_s": i * p["interval"]}
                for i in range(p["n_pts"])]

    def plot_axes(self):
        return ("Time (s)", "detector")

    def scan_label(self):
        if self._widget is None: return "Time Scan"
        p = self._widget.params()
        return (f"Time  {p['duration']:.1f} s  @  "
                f"{p['interval']:.2f} s/pt  ({p['n_pts']} pts)")

    def to_plan(self, motor_dev: str, det_dev: str) -> tuple[str, list, dict]:
        # motor_dev is unused for time scans — no motor to move
        if self._widget is None:
            raise RuntimeError("TimeScan widget not built yet")
        p = self._widget.params()
        return (
            "count",
            [[det_dev]],
            {"num": p["n_pts"], "delay": p["interval"]},
        )

# ── Registry ──────────────────────────────────────────────────────────────────
ALL_SCAN_TYPES: list[type[BaseScan]] = [Scan1D, Scan2D, TimeScan]


# ── Simulation helper ─────────────────────────────────────────────────────────
def sim_scalar(positions: list[dict], idx: int) -> float:
    """Gaussian + noise centred on the scan range, for simulation mode."""
    xs  = [p["_x"] for p in positions]
    x   = xs[idx]
    mid = (xs[0] + xs[-1]) / 2
    rng = abs(xs[-1] - xs[0]) or 1.0
    return 500 * math.exp(-0.5 * ((x - mid) / (rng * 0.15)) ** 2) + random.gauss(0, 5)


# ── Utility ───────────────────────────────────────────────────────────────────
def _linspace(start: float, stop: float, n: int) -> list[float]:
    if n <= 1: return [float(start)]
    step = (stop - start) / (n - 1)
    return [start + i * step for i in range(n)]
