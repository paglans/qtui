"""
daq_tab.py — Tab 3: Data Acquisition
Scan configuration, run control, live detector readouts, file-writing status.
"""
import time
import random
import math
from pathlib import Path
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QSplitter,
    QGroupBox, QGridLayout, QLineEdit, QPushButton, QComboBox,
    QProgressBar, QScrollArea, QSizePolicy, QFrame, QCheckBox,
    QSpinBox, QDoubleSpinBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QTextEdit, QFileDialog, QRadioButton, QButtonGroup,
    QStackedWidget, QDialog, QDialogButtonBox, QListWidget, QListWidgetItem,
    QMessageBox,
)
from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QFont, QColor

from common import (
    PAL, COMBO_STYLE, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE,
    MPL_AVAILABLE, EPICS_AVAILABLE, PVMonitor, PVLabel, CameraPanel,
)
from scan_types import XASScanDef, XASSubrange, sim_scalar
from devices import device_name, all_device_map

if MPL_AVAILABLE:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure

# ── Styles ────────────────────────────────────────────────────────────────────
SPIN_STYLE = f"""
    QSpinBox, QDoubleSpinBox {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        padding:3px 6px; font-family:monospace;
    }}
    QSpinBox:focus, QDoubleSpinBox:focus {{ border-color:{PAL['accent']}; }}
    QSpinBox::up-button, QDoubleSpinBox::up-button,
    QSpinBox::down-button, QDoubleSpinBox::down-button {{
        background:{PAL['surface']}; border:none; width:16px;
    }}
"""
TABLE_STYLE = f"""
    QTableWidget {{
        background:{PAL['bg']}; color:{PAL['text']};
        gridline-color:#2a3a5e; border:1px solid #2a3a5e;
        border-radius:4px; font-family:monospace; font-size:8pt;
    }}
    QTableWidget::item:selected {{ background:#2a3a5e; color:{PAL['accent']}; }}
    QHeaderView::section {{
        background:{PAL['surface']}; color:{PAL['accent']};
        border:none; border-right:1px solid #2a3a5e;
        border-bottom:1px solid #2a3a5e; padding:4px; font-size:8pt;
    }}
"""
LOG_STYLE = f"""
    QTextEdit {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        font-family:monospace; font-size:8pt;
    }}
"""
CHECK_STYLE = f"""
    QCheckBox {{ color:{PAL['text']}; spacing:6px; }}
    QCheckBox::indicator {{
        width:14px; height:14px; border:1px solid #2a3a5e;
        border-radius:3px; background:{PAL['bg']};
    }}
    QCheckBox::indicator:checked {{
        background:{PAL['accent']}; border-color:{PAL['accent']};
    }}
"""
RADIO_STYLE = f"""
    QRadioButton {{ color:{PAL['text']}; spacing:6px; }}
    QRadioButton::indicator {{
        width:14px; height:14px; border:1px solid #2a3a5e;
        border-radius:7px; background:{PAL['bg']};
    }}
    QRadioButton::indicator:checked {{
        background:{PAL['accent']}; border-color:{PAL['accent']};
    }}
"""

# ── Scan state machine ────────────────────────────────────────────────────────
class _ScanState:
    IDLE     = "IDLE"
    RUNNING  = "RUNNING"
    PAUSED   = "PAUSED"
    ABORTING = "ABORTING"

# ── Live detector table ───────────────────────────────────────────────────────
class DetectorTable(QTableWidget):
    """Periodically refreshes readback values for a list of detector PVs."""

    COLS = ["Detector", "PV", "Counts / Value", "Status"]

    def __init__(self, detector_pvs: dict, parent=None):
        super().__init__(0, len(self.COLS), parent)
        self._det_pvs = detector_pvs
        self.setHorizontalHeaderLabels(self.COLS)
        self.setStyleSheet(TABLE_STYLE)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.setAlternatingRowColors(False)
        self._row_map: dict = {}   # pv → row index
        self._populate()
        PVMonitor().value_changed.connect(self._on_pv)

    def _populate(self):
        for name, pv in self._det_pvs.items():
            r = self.rowCount(); self.insertRow(r)
            self._row_map[pv] = r
            self._set(r, 0, name,  PAL["text"])
            self._set(r, 1, pv,    PAL["subtext"])
            self._set(r, 2, "…",   PAL["subtext"])
            self._set(r, 3, "N/C", PAL["nc"])
            PVMonitor().subscribe(pv)

    def _set(self, row, col, text, color):
        item = QTableWidgetItem(text)
        item.setForeground(QColor(color))
        self.setItem(row, col, item)

    def _on_pv(self, name, value):
        r = self._row_map.get(name)
        if r is None: return
        if value is None:
            self._set(r, 2, "N/C", PAL["nc"])
            self._set(r, 3, "N/C", PAL["nc"])
        else:
            try:
                self._set(r, 2, f"{float(value):.6g}", PAL["ok"])
                self._set(r, 3, "OK ✔",               PAL["ok"])
            except Exception:
                self._set(r, 2, str(value)[:20], PAL["warn"])
                self._set(r, 3, "?",             PAL["warn"])

    def add_detector(self, name, pv):
        if pv in self._row_map: return
        self._det_pvs[name] = pv
        r = self.rowCount(); self.insertRow(r)
        self._row_map[pv] = r
        self._set(r, 0, name, PAL["text"]); self._set(r, 1, pv, PAL["subtext"])
        self._set(r, 2, "…",  PAL["subtext"]); self._set(r, 3, "N/C", PAL["nc"])
        PVMonitor().subscribe(pv)

# ── Scan plot ─────────────────────────────────────────────────────────────────
class ScanPlot(QWidget):
    """Live scan plot — always displays the accumulated mean spectrum."""

    def __init__(self, parent=None):
        super().__init__(parent)
        vl = QVBoxLayout(self); vl.setContentsMargins(0,0,0,0); vl.setSpacing(2)
        if MPL_AVAILABLE:
            self._fig    = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax     = self._fig.add_subplot(111)
            # _line kept hidden — used only for cursor snap data storage
            self._line,  = self._ax.plot([], [], visible=False)
            # _accum_line is the only visible trace
            self._accum_line, = self._ax.plot([], [], color=PAL["accent"],
                                              lw=1.8, marker=".", ms=4)
            self._ch_v = self._ax.axvline(x=0, color=PAL["text"],
                                          lw=0.8, ls="--", alpha=0.6, visible=False)
            self._ch_h = self._ax.axhline(y=0, color=PAL["text"],
                                          lw=0.8, ls="--", alpha=0.6, visible=False)
            self._snap_pt, = self._ax.plot([], [], "o",
                                           color=PAL["ok"], ms=6,
                                           zorder=5, visible=False)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            self._canvas.mpl_connect("motion_notify_event", self._on_mouse_move)
            self._canvas.mpl_connect("axes_leave_event",    self._on_axes_leave)
            from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar
            self._toolbar = NavToolbar(self._canvas, self)
            self._toolbar.setStyleSheet(f"""
                QToolBar {{
                    background:{PAL["surface"]}; border:none; spacing:2px;
                }}
                QToolButton {{
                    background:{PAL["surface"]}; color:{PAL["text"]};
                    border:1px solid transparent; border-radius:3px;
                    padding:3px; font-size:8pt;
                }}
                QToolButton:hover {{
                    background:#1e3a5e; border-color:#2a5a8e;
                }}
                QToolButton:checked {{
                    background:{PAL["accent"]}; color:{PAL["bg"]};
                    border-color:{PAL["accent"]};
                }}
            """)
            self._auto_scale = True
            self._canvas.mpl_connect("button_release_event", self._on_btn_release)
            _orig_home = self._toolbar.home
            def _home(*a, **kw):
                _orig_home(*a, **kw)
                self._auto_scale = True
                self._fit_to_data()
            self._toolbar.home = _home
            vl.addWidget(self._toolbar)
            vl.addWidget(self._canvas)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph)

        readout_row = QHBoxLayout(); readout_row.setSpacing(16)
        readout_row.setContentsMargins(8, 0, 8, 2)
        def _ro_pair(label):
            lbl = QLabel(label)
            lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
            val = QLabel("—")
            val.setStyleSheet(
                f"color:{PAL['text']}; font-family:monospace; font-size:8pt; "
                f"min-width:90px;")
            readout_row.addWidget(lbl); readout_row.addWidget(val)
            return val
        self._ro_pos = _ro_pair("Position:")
        self._ro_int = _ro_pair("Intensity:")
        readout_row.addStretch()
        vl.addLayout(readout_row)

        # Accumulator state
        self._xs: list  = []          # mirror for cursor snap
        self._ys: list  = []
        self._xs_accum: list = []     # canonical x positions (set on first rep)
        self._xs_build: list = []     # current rep y positions being built
        self._ys_build: list = []     # current rep y values being built
        self._ys_sum          = None  # numpy array: sum of completed reps
        self._n_accum: int    = 0     # number of completed reps
        self._n_scans_total: int = 1

    # ── Axis styling ──────────────────────────────────────────────────────────
    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.grid(True, color="#2a3a5e", lw=0.5, ls="--")
        ax.set_xlabel("Motor position", color=PAL["subtext"], fontsize=8)
        ax.set_ylabel("Counts",         color=PAL["subtext"], fontsize=8)
        ax.set_title("No scan yet",     color=PAL["text"],    fontsize=9)

    # ── Public API ────────────────────────────────────────────────────────────
    def reset(self, motor_lbl="Motor position", signal_lbl="Counts", title=""):
        """Start a new scan: clear all data and reset accumulators."""
        self._xs.clear(); self._ys.clear()
        self._xs_accum = []; self._xs_build = []; self._ys_build = []
        self._ys_sum = None; self._n_accum = 0
        if not MPL_AVAILABLE: return
        self._accum_line.set_data([], [])
        self._ax.set_xlabel(motor_lbl,  color=PAL["subtext"], fontsize=8)
        self._ax.set_ylabel(signal_lbl, color=PAL["subtext"], fontsize=8)
        self._ax.set_title(title or "Scan in progress …",
                           color=PAL["text"], fontsize=9)
        self._ax.set_xlim(0, 1); self._ax.set_ylim(0, 1)
        self._ax.autoscale(enable=True, axis="both")
        self._auto_scale = True
        self._canvas.draw_idle()
        self._ro_pos.setText("—"); self._ro_int.setText("—")

    def start_accumulation(self, n_scans: int):
        """Initialise for a multi-rep scan series."""
        self._n_scans_total = n_scans
        # Data cleared in reset(); just store the target count

    def add_point_to_accum(self, x: float, y: float, n_rep: int = 0):
        """Append point to current rep build buffer and update display."""
        import numpy as np

        # Auto-finalise if build buffer is full (rep boundary detected by count)
        if n_rep > 0 and len(self._xs_build) >= n_rep:
            self.finish_accum_rep()
            # Don't call redraw_accum here — the append below will redraw

        self._xs_build.append(x)
        self._ys_build.append(y)
        if not MPL_AVAILABLE:
            return
        n_built = len(self._xs_build)

        if self._ys_sum is not None:
            # Show completed mean for all positions, blended with current build
            # for positions already measured in this rep
            mean_full = self._ys_sum / max(self._n_accum, 1)  # completed mean
            display = mean_full.copy()
            n_show = min(n_built, len(display))
            # Update positions already measured in this rep with running mean
            display[:n_show] = (
                self._ys_sum[:n_show] + np.asarray(self._ys_build[:n_show])
            ) / (self._n_accum + 1)
            xs_show = self._xs_accum
        else:
            # First rep — just show what we have so far
            display = np.asarray(self._ys_build)
            xs_show = self._xs_build

        self._accum_line.set_data(xs_show, display.tolist())
        self._xs = list(xs_show)
        self._ys = display.tolist()
        if getattr(self, "_auto_scale", True) and len(xs_show) >= 2:
            self._fit_to_data()
        self._canvas.draw_idle()

    def finish_accum_rep(self):
        """Add current build buffer to the running sum and reset for next rep."""
        import numpy as np
        if not self._ys_build:
            return
        ys = np.asarray(self._ys_build)
        if self._ys_sum is None:
            self._ys_sum   = ys.copy()
            self._xs_accum = list(self._xs_build)
        else:
            n = min(len(self._ys_sum), len(ys))
            self._ys_sum   = self._ys_sum[:n] + ys[:n]
            self._xs_accum = self._xs_accum[:n]
        self._n_accum += 1
        # Clear build buffer for next rep
        self._xs_build = []
        self._ys_build = []

    def redraw_accum(self):
        """Redraw the mean line from the completed sum."""
        if self._ys_sum is None or not self._xs_accum or not MPL_AVAILABLE:
            return
        import numpy as np
        mean = self._ys_sum / max(self._n_accum, 1)
        self._accum_line.set_data(self._xs_accum, mean.tolist())
        self._xs = list(self._xs_accum)
        self._ys = mean.tolist()
        if getattr(self, "_auto_scale", True):
            self._fit_to_data()
        self._canvas.draw_idle()

    def finish(self, title=""):
        if not MPL_AVAILABLE: return
        self._ax.set_title(title or "Scan complete",
                           color=PAL["ok"], fontsize=9)
        self._canvas.draw_idle()

    def save_accumulated(self, path: str):
        """Write the accumulated mean spectrum to HDF5."""
        if self._ys_sum is None or not self._xs_accum:
            return
        import numpy as np
        try:
            import h5py
            p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
            n = max(self._n_accum, 1)
            with h5py.File(p, "w") as f:
                f.create_dataset("entry/motor_positions",
                                 data=np.asarray(self._xs_accum))
                f.create_dataset("entry/mean_counts",
                                 data=self._ys_sum / n)
                f.create_dataset("entry/sum_counts",
                                 data=self._ys_sum)
                f["entry"].attrs["n_accumulated"] = self._n_accum
        except Exception as e:
            print(f"[ScanPlot] save_accumulated error: {e}")

    # ── Axis scaling ──────────────────────────────────────────────────────────
    def _fit_to_data(self, extra_ys=None):
        if not MPL_AVAILABLE:
            return
        import numpy as np
        # Scale from the currently visible data
        xd, yd = self._accum_line.get_data()
        if not len(xd):
            return
        xs, ys = list(xd), list(yd)
        if not xs or not ys:
            return
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        xspan = xmax - xmin; yspan = ymax - ymin
        xpad = xspan * 0.02 if xspan else abs(xmax) * 0.01 or 0.5
        ypad = yspan * 0.05 if yspan else abs(ymax) * 0.05 or abs(ymax) * 0.01 or 1e-10
        self._ax.set_xlim(xmin - xpad, xmax + xpad)
        self._ax.set_ylim(ymin - ypad, ymax + ypad)

    def _on_btn_release(self, ev):
        if hasattr(self, "_toolbar") and self._toolbar.mode:
            self._auto_scale = False

    # legacy — keep for local scan engine compatibility
    def add_point(self, x, y):
        self.add_point_to_accum(x, y)

    # ── Cursor ────────────────────────────────────────────────────────────────
    def _on_mouse_move(self, ev):
        if not MPL_AVAILABLE or ev.inaxes is not self._ax or not self._xs:
            return
        # Don't interfere when the toolbar's pan or zoom tool is active
        if hasattr(self, "_toolbar") and self._toolbar.mode:
            return
        # Only track while left mouse button is held
        if ev.button != 1:
            return
        import numpy as np
        xs = np.asarray(self._xs); ys = np.asarray(self._ys)
        idx  = int(np.argmin(np.abs(xs - ev.xdata)))
        snap_x, snap_y = float(xs[idx]), float(ys[idx])

        self._ch_v.set_xdata([snap_x]); self._ch_v.set_visible(True)
        self._ch_h.set_ydata([snap_y]); self._ch_h.set_visible(True)
        self._snap_pt.set_data([snap_x], [snap_y]); self._snap_pt.set_visible(True)
        self._canvas.draw_idle()

        self._ro_pos.setText(f"{snap_x:.6g}")
        self._ro_int.setText(f"{snap_y:.6g}")

    def _on_axes_leave(self, _ev):
        # Cursor stays where it is when the mouse leaves — do nothing
        pass

# ── DAQ Tab ───────────────────────────────────────────────────────────────────
SCAN_DWELL_MS  = 200
XAS_SCANS_PATH = Path(__file__).parent.parent / "config" / "xas_scans.json"

_TSPIN = f"""
    QDoubleSpinBox {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        padding:3px 6px; font-family:monospace; font-size:8pt;
    }}
    QDoubleSpinBox:focus {{ border-color:{PAL['accent']}; }}
    QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{
        background:{PAL['surface']}; border:none; width:16px;
    }}
"""


class XASScanDialog(QDialog):
    """
    Dialog for creating or editing a named XAS scan definition.

    Presents a table of subranges (Start / Stop / Step Size / Points),
    a scan name field, and a motor selector.  Saved scans are persisted
    to *XAS_SCANS_PATH* as JSON.
    """

    _HDR = ("Start (eV)", "Stop (eV)", "Step Size (eV)", "Points")

    def __init__(self, motor_names: list[str],
                 default_motor: str = "MonoEnergy",
                 existing: "XASScanDef | None" = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("Define XAS Scan")
        self.resize(560, 440)
        self.setStyleSheet(f"background:{PAL['bg']}; color:{PAL['text']};")

        self._motor_names   = motor_names
        self._default_motor = default_motor
        self._result_def: "XASScanDef | None" = None

        vl = QVBoxLayout(self); vl.setSpacing(10); vl.setContentsMargins(14,14,14,14)

        # ── Name row ──────────────────────────────────────────────────────────
        name_row = QHBoxLayout()
        name_lbl = QLabel("Scan name")
        name_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        name_row.addWidget(name_lbl)
        self._name_edit = QLineEdit(existing.name if existing else "New XAS Scan")
        self._name_edit.setStyleSheet(INPUT_STYLE)
        name_row.addWidget(self._name_edit, 1)
        vl.addLayout(name_row)

        # ── Motor row ─────────────────────────────────────────────────────────
        motor_row = QHBoxLayout()
        motor_lbl = QLabel("Motor")
        motor_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        motor_row.addWidget(motor_lbl)
        self._motor_combo = QComboBox(); self._motor_combo.setStyleSheet(COMBO_STYLE)
        for n in motor_names: self._motor_combo.addItem(n)
        target = (existing.motor if existing else default_motor)
        idx = self._motor_combo.findText(target)
        if idx >= 0: self._motor_combo.setCurrentIndex(idx)
        motor_row.addWidget(self._motor_combo, 1)
        vl.addLayout(motor_row)

        # ── Exposure time row ─────────────────────────────────────────────────
        exp_row = QHBoxLayout(); exp_row.setSpacing(6)
        exp_lbl = QLabel("Exposure time (s)")
        exp_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        exp_row.addWidget(exp_lbl)
        self._exp_spin = QDoubleSpinBox()
        self._exp_spin.setRange(0.05, 3600.0); self._exp_spin.setDecimals(2)
        self._exp_spin.setSingleStep(0.1)
        self._exp_spin.setValue(existing.exposure_time if existing else 1.0)
        self._exp_spin.setStyleSheet(_TSPIN)
        exp_row.addWidget(self._exp_spin)
        exp_row.addStretch()
        vl.addLayout(exp_row)

        # ── Subrange table ────────────────────────────────────────────────────
        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(self._HDR)
        self._table.setStyleSheet(f"""
            QTableWidget {{
                background:{PAL['surface']}; color:{PAL['text']};
                gridline-color:#2a3a5e; border:1px solid #2a3a5e;
                font-size:8pt;
            }}
            QHeaderView::section {{
                background:{PAL['bg']}; color:{PAL['accent']};
                border:none; padding:4px; font-size:8pt;
            }}
            QTableWidget::item:selected {{ background:#1e3a5e; }}
        """)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.verticalHeader().setVisible(False)
        vl.addWidget(self._table, 1)

        # ── Summary label (created before table population so _add_row can use it)
        self._summary = QLabel("")
        self._summary.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")

        # Populate from existing definition or add one default row
        if existing and existing.subranges:
            for sr in existing.subranges:
                self._add_row(sr.start, sr.stop, sr.step_size)
        else:
            self._add_row(250.0, 270.0, 0.5)

        # ── Table buttons ─────────────────────────────────────────────────────
        tbl_btns = QHBoxLayout(); tbl_btns.setSpacing(6)
        add_row_btn = QPushButton("＋ Add subrange"); add_row_btn.setStyleSheet(BTN_STYLE)
        add_row_btn.clicked.connect(self._on_add_row)
        del_row_btn = QPushButton("− Remove selected"); del_row_btn.setStyleSheet(BTN_STYLE)
        del_row_btn.clicked.connect(self._on_del_row)
        tbl_btns.addWidget(add_row_btn); tbl_btns.addWidget(del_row_btn)
        tbl_btns.addStretch()
        vl.addLayout(tbl_btns)

        vl.addWidget(self._summary)
        # ── Dialog buttons ────────────────────────────────────────────────────
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.setStyleSheet(f"""
            QPushButton {{ {BTN_STYLE} padding:5px 18px; }}
        """)
        btns.accepted.connect(self._on_save)
        btns.rejected.connect(self.reject)
        vl.addWidget(btns)

    # ── Internal helpers ──────────────────────────────────────────────────────
    def _spin(self, val: float, lo: float = 0.0,
              hi: float = 100000.0, dec: int = 4) -> QDoubleSpinBox:
        sp = QDoubleSpinBox()
        sp.setRange(lo, hi); sp.setValue(val); sp.setDecimals(dec)
        sp.setStyleSheet(_TSPIN); sp.setFrame(False)
        sp.valueChanged.connect(self._update_summary)
        return sp

    def _add_row(self, start=250.0, stop=270.0, step=0.5):
        r = self._table.rowCount(); self._table.insertRow(r)
        self._table.setCellWidget(r, 0, self._spin(start))
        self._table.setCellWidget(r, 1, self._spin(stop))
        self._table.setCellWidget(r, 2, self._spin(step, lo=1e-6, dec=4))
        pts_lbl = QLabel("—")
        pts_lbl.setAlignment(Qt.AlignCenter)
        pts_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        self._table.setCellWidget(r, 3, pts_lbl)
        self._table.setRowHeight(r, 32)
        self._update_summary()

    def _subranges(self) -> list[XASSubrange]:
        result = []
        for r in range(self._table.rowCount()):
            start = self._table.cellWidget(r, 0).value()
            stop  = self._table.cellWidget(r, 1).value()
            step  = self._table.cellWidget(r, 2).value()
            result.append(XASSubrange(start, stop, step))
        return result

    def _update_summary(self):
        total = 0
        for r in range(self._table.rowCount()):
            w0 = self._table.cellWidget(r, 0)
            w1 = self._table.cellWidget(r, 1)
            w2 = self._table.cellWidget(r, 2)
            if not (w0 and w1 and w2): continue
            sr  = XASSubrange(w0.value(), w1.value(), w2.value())
            n   = sr.n_points()
            pts_lbl = self._table.cellWidget(r, 3)
            if pts_lbl: pts_lbl.setText(str(n))
            total += n
        # Subtract shared boundary points between consecutive subranges
        if self._table.rowCount() > 1:
            total -= (self._table.rowCount() - 1)
        self._summary.setText(f"Total: {max(total, 0)} points")

    def _on_add_row(self):
        # Default next row continues from previous stop
        if self._table.rowCount():
            prev_stop = self._table.cellWidget(self._table.rowCount()-1, 1).value()
            prev_step = self._table.cellWidget(self._table.rowCount()-1, 2).value()
            self._add_row(prev_stop, prev_stop + 20.0, prev_step)
        else:
            self._add_row()

    def _on_del_row(self):
        rows = sorted({idx.row() for idx in self._table.selectedIndexes()}, reverse=True)
        for r in rows: self._table.removeRow(r)
        if self._table.rowCount() == 0:
            self._add_row()
        self._update_summary()

    def _on_save(self):
        name = self._name_edit.text().strip() or "XAS Scan"
        motor = self._motor_combo.currentText()
        exp   = self._exp_spin.value()
        srs = self._subranges()
        if not srs:
            return
        self._result_def = XASScanDef(name=name, motor=motor,
                                      exposure_time=exp, subranges=srs)
        self.accept()

    def result_def(self) -> "XASScanDef | None":
        return self._result_def


class DAQTab(QWidget):
    def __init__(self, amber_cfg: dict, hirrixs_cfg: dict,
                 config_tab=None, app_cfg: dict | None = None, parent=None):
        super().__init__(parent)
        self._config_tab = config_tab
        self._app_cfg    = app_cfg or {}
        self._xas_default_motor = amber_cfg.get("xas_default_motor", "MonoEnergy")
        self._xas_scan_defs: list[XASScanDef] = []
        self._load_xas_scans()
        self.setStyleSheet(f"background:{PAL['bg']};")
        self._state = _ScanState.IDLE
        self._scan_idx = 0; self._scan_positions: list = []
        self._scan_timer = QTimer(self); self._scan_timer.timeout.connect(self._scan_step)

        # Build motor / signal / detector dicts from configs
        self._motor_pvs  = self._collect_pvs(amber_cfg,   "motor")
        self._motor_pvs.update(self._collect_pvs(hirrixs_cfg, "motor"))
        self._signal_pvs = {}
        self._signal_pvs.update(amber_cfg.get("signal", {}))
        self._signal_pvs.update(hirrixs_cfg.get("signal", {}))
        self._det_pvs: dict = {}
        for n, p in hirrixs_cfg.get("detector", {}).items():
            self._det_pvs[n] = p + ":Acquire_RBV"
        self._camera_pvs: dict = dict(hirrixs_cfg.get("camera", {}))

        # ── Device name map (friendly → ophyd) for QS live display ───────────
        self._device_map: dict = all_device_map(amber_cfg, hirrixs_cfg)

        # ── Queue-server live display state ───────────────────────────────────
        self._qs_active   = False
        self._qs_callback_connected = False
        self._qs_scan_started       = False   # True only after first item signal
        self._qs_motor_pv = ""
        self._qs_det_pv   = ""
        self._qs_n_steps  = 0
        self._qs_t0       = 0.0
        self._qs_last_x   = None   # last recorded motor position (float or None)
        self._qs_last_t   = 0.0    # timestamp of last recorded point (monotonic)
        self._qs_min_dt   = 0.5
        self._qs_pending: dict = {}

        # ── RIXS excitation energies (From XAS list) ──────────────────────────
        self._rixs_energies: list[float] = []

        # ── Multiple-scan accumulation state ──────────────────────────────────
        self._xas_rep_total   = 1    # total repetitions requested
        self._xas_rep_idx     = 0    # current repetition (0-based)
        self._xas_rep_files: list[str] = []   # per-rep filenames submitted

        # ── Scan counter persistence ──────────────────────────────────────────
        self._counter_path = Path.home() / ".config" / "amber_qtui" / "scan_counter.json"
        self._counter_path.parent.mkdir(parents=True, exist_ok=True)

        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0); outer.setSpacing(0)
        hdr = QLabel("  💾  Data Acquisition")
        hdr.setFont(QFont("Sans Serif",9,QFont.Bold))
        hdr.setStyleSheet(f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        outer.addWidget(hdr)

        self._qs_elapsed_timer = QTimer(self)
        self._qs_elapsed_timer.setInterval(1000)
        self._qs_elapsed_timer.timeout.connect(self._qs_update_elapsed)

        # ── Main area: three-column horizontal splitter ───────────────────────
        hsplit = QSplitter(Qt.Horizontal); hsplit.setStyleSheet(SPLITTER_STYLE)

        # ── Left column: File Output / Scan Params / Run Control / QS Status / Camera ──
        left = QWidget(); left.setStyleSheet(f"background:{PAL['bg']};")
        lv = QVBoxLayout(left); lv.setContentsMargins(8,8,8,8); lv.setSpacing(10)
        lv.addWidget(self._build_file_group())
        lv.addWidget(self._build_scan_group())
        lv.addWidget(self._build_run_group())
        lv.addWidget(self._build_status_group())

        cam_grp = QGroupBox("Camera"); cam_grp.setStyleSheet(GRP_STYLE)
        cam_grp.setMinimumHeight(260)
        cg_vl = QVBoxLayout(cam_grp); cg_vl.setContentsMargins(4,18,4,4)
        self._camera_panel = CameraPanel(self._camera_pvs)
        cg_vl.addWidget(self._camera_panel)
        lv.addWidget(cam_grp)

        # Left column stretches to the bottom — no addStretch()
        left.setMinimumWidth(320); left.setMaximumWidth(480)
        scroll_left = QScrollArea(); scroll_left.setWidgetResizable(True)
        scroll_left.setStyleSheet(f"background:{PAL['bg']}; border:none;")
        scroll_left.setWidget(left)
        hsplit.addWidget(scroll_left)

        # ── Centre + log (vertical) ───────────────────────────────────────────
        centre_log_widget = QWidget(); centre_log_widget.setStyleSheet(f"background:{PAL['bg']};")
        centre_log_vl = QVBoxLayout(centre_log_widget)
        centre_log_vl.setContentsMargins(0, 0, 0, 0); centre_log_vl.setSpacing(4)

        centre_vsplit = QSplitter(Qt.Vertical); centre_vsplit.setStyleSheet(SPLITTER_STYLE)

        xas_grp = QGroupBox("XAS"); xas_grp.setStyleSheet(GRP_STYLE)
        xg_vl = QVBoxLayout(xas_grp); xg_vl.setContentsMargins(4,18,4,4)
        self._xas_plot = ScanPlot()
        xg_vl.addWidget(self._xas_plot)
        centre_vsplit.addWidget(xas_grp)

        rixs_grp = QGroupBox("RIXS"); rixs_grp.setStyleSheet(GRP_STYLE)
        rg_vl = QVBoxLayout(rixs_grp); rg_vl.setContentsMargins(4,18,4,4)
        self._rixs_plot = ScanPlot()
        rg_vl.addWidget(self._rixs_plot)
        centre_vsplit.addWidget(rixs_grp)

        centre_vsplit.setSizes([500, 500])
        centre_log_vl.addWidget(centre_vsplit, 1)

        # Acquisition log — same width as XAS/RIXS panels
        log_grp = QGroupBox("Acquisition Log"); log_grp.setStyleSheet(GRP_STYLE)
        lg_v = QVBoxLayout(log_grp); lg_v.setContentsMargins(6,18,6,6)
        self._log = QTextEdit(); self._log.setReadOnly(True)
        self._log.setStyleSheet(LOG_STYLE); self._log.setFixedHeight(130)
        lg_v.addWidget(self._log)
        clr_log_btn = QPushButton("Clear log"); clr_log_btn.setStyleSheet(BTN_STYLE)
        clr_log_btn.setFixedWidth(80); clr_log_btn.clicked.connect(self._log.clear)
        lg_v.addWidget(clr_log_btn, alignment=Qt.AlignRight)
        centre_log_vl.addWidget(log_grp)

        hsplit.addWidget(centre_log_widget)

        # ── Right column: XAS controls + RIXS controls + RE controls ─────────
        right_widget = QWidget(); right_widget.setStyleSheet(f"background:{PAL['bg']};")
        right_vl = QVBoxLayout(right_widget)
        right_vl.setContentsMargins(0, 0, 0, 0); right_vl.setSpacing(4)

        right_vsplit = QSplitter(Qt.Vertical); right_vsplit.setStyleSheet(SPLITTER_STYLE)
        right_vsplit.addWidget(self._build_xas_panel())
        right_vsplit.addWidget(self._build_rixs_panel())
        right_vsplit.setSizes([500, 500])
        right_vl.addWidget(right_vsplit, 1)

        right_vl.addWidget(self._build_re_controls())
        hsplit.addWidget(right_widget)

        hsplit.setSizes([380, 900, 240])
        outer.addWidget(hsplit, 1)
        # Initialise filename display after all widgets exist
        QTimer.singleShot(0, self._update_filename_display)

    # ── Queue tab wiring ──────────────────────────────────────────────────────
    def set_queue_tab(self, qt) -> None:
        """Wire in the QueueTab for plan submission and live scan display."""
        self._queue_tab = qt
        qt.running_item_changed.connect(self._on_qs_running_item)
        self._log_msg("Queue server tab connected.", PAL["subtext"])

    # ── Queue-server live display ─────────────────────────────────────────────

    def _qs_arm(self, motor_pv: str, det_pv: str,
                motor_lbl: str, det_lbl: str,
                n_steps: int, plan_name: str) -> None:
        """Subscribe PVs and connect the motor callback so data collection
        starts immediately — called from _on_xas_queue (before the scan
        starts) and from _on_qs_running_item (safety net).
        Safe to call multiple times; UniqueConnection prevents duplicates.
        """
        if motor_pv:
            PVMonitor().subscribe(motor_pv)
        if det_pv:
            PVMonitor().subscribe(det_pv)

        self._qs_motor_pv = motor_pv
        self._qs_det_pv   = det_pv
        self._qs_n_steps  = n_steps
        self._qs_last_x   = None
        self._qs_last_t   = 0.0
        exp = self._p_exp.value() if hasattr(self, "_p_exp") else 0.5
        self._qs_min_dt = max(exp * 0.5, 0.1)

        if not self._qs_active:
            self._qs_active       = True
            self._qs_scan_started = False   # wait for item signal before accumulating
            self._qs_raw_xs: list = []
            self._qs_wait_total   = 0.0
            self._qs_t0           = time.monotonic()
            self._xas_plot.reset(motor_lbl, det_lbl,
                                 f"QServer: {plan_name}  (waiting…)")
            # Initialise accumulator for the full series
            self._xas_plot.start_accumulation(
                getattr(self, "_xas_rep_total", 1))
            self._progress.setMaximum(n_steps if n_steps else 0)
            self._progress.setValue(0)
            self._set_state(_ScanState.RUNNING)
            self._st_file.setText(f"{plan_name} · pending")
            self._st_file.setStyleSheet(
                f"color:{PAL['accent']}; font-family:monospace; font-size:8pt;")
            self._st_point.setText("0" + (f" / {n_steps}" if n_steps else ""))
            self._st_elapsed.setText("0.0 s")
            if self._qs_callback_connected:
                PVMonitor().value_changed.disconnect(self._on_qs_motor_callback)
            PVMonitor().value_changed.connect(
                self._on_qs_motor_callback, Qt.UniqueConnection)
            self._qs_callback_connected = True
            self._qs_elapsed_timer.start()

    def _on_qs_running_item(self, item: dict) -> None:
        """Called when the queue server starts or finishes a scan."""

        # ── Queue went idle ───────────────────────────────────────────────────
        if not item:
            if self._qs_active:
                self._qs_elapsed_timer.stop()
                self._qs_final_t = time.monotonic() - self._qs_t0
                # Don't disconnect callback yet — late callbacks may still arrive.
                # Disconnect inside _finish_rep_and_save after the delay.
                QTimer.singleShot(1500, self._finish_rep_and_save)
            return

        # ── New scan / rep starting ───────────────────────────────────────────
        plan      = item.get("name", "?")
        args      = item.get("args", [])
        uid_short = item.get("item_uid", "")[:8]
        rep_total = getattr(self, "_xas_rep_total", 1)

        if self._qs_active:
            self._qs_scan_started = True
            rep_now = self._xas_plot._n_accum + 1
            rep_now = self._xas_rep_idx + 1
            self._log_msg(
                f"🗂 QServer: rep {rep_now}/{rep_total}  uid=…{uid_short}",
                PAL["accent"])
            title = (f"QServer: {plan}  rep {rep_now}/{rep_total}  (…{uid_short})"
                     if rep_total > 1 else
                     f"QServer: {plan}  (…{uid_short})")
            self._xas_plot._ax.set_title(title, color=PAL["text"], fontsize=9)
            if hasattr(self._xas_plot, "_canvas"):
                self._xas_plot._canvas.draw_idle()
            self._st_file.setText(f"{plan} · rep {rep_now}/{rep_total}")
            self._qs_active = True
            self._qs_t0     = time.monotonic()
            self._qs_elapsed_timer.start()
            return

        # ── First scan starting (safety net / external client) ────────────────
        self._log_msg(
            f"🗂 QServer running: plan={plan!r}  uid=…{uid_short}", PAL["accent"])

        inv_map = {v: k for k, v in self._device_map.items()}

        def _resolve(devname):
            friendly = inv_map.get(str(devname), str(devname))
            pv = (self._motor_pvs.get(friendly)
                  or self._signal_pvs.get(friendly)
                  or self._det_pvs.get(friendly)
                  or "")
            return friendly, pv

        det_arg   = args[0] if args else ""
        if isinstance(det_arg, list):
            det_arg = det_arg[0] if det_arg else ""
        motor_arg = args[1] if len(args) > 1 else ""
        n_steps   = len(args[2]) if len(args) > 2 and isinstance(args[2], list) else 0

        motor_lbl, motor_pv = _resolve(motor_arg)
        det_lbl,   det_pv   = _resolve(det_arg)

        if not motor_pv:
            motor_lbl = self._motor_combo.currentText()
            motor_pv  = self._motor_pvs.get(motor_lbl, "")
        if not det_pv:
            det_lbl = self._det_combo.currentText()
            det_pv  = ({**self._signal_pvs, **self._det_pvs}).get(det_lbl, "")

        self._qs_arm(motor_pv, det_pv, motor_lbl, det_lbl, n_steps, plan)
        self._xas_plot._ax.set_title(
            f"QServer: {plan}  (…{uid_short})", color=PAL["text"], fontsize=9)
        if hasattr(self._xas_plot, "_canvas"):
            self._xas_plot._canvas.draw_idle()
        self._st_file.setText(f"{plan} · …{uid_short}")

    def _finish_rep_and_save(self):
        """Finalise accumulation and save. Retries every 200ms until all points
        have arrived or a 5s timeout is reached."""
        plot  = self._xas_plot
        n_rep = self._qs_n_steps

        # For rep N>0, _xs_accum_ptr tracks how many points were written this rep.
        # For rep 0 (n_accum==0), len(_xs_accum) is the point count.
        if n_rep > 0:
            pts_this_rep = len(plot._xs_build)
            wait_total   = getattr(self, "_qs_wait_total", 0.0)
            if pts_this_rep < n_rep and wait_total < 5.0:
                self._qs_wait_total = wait_total + 0.2
                QTimer.singleShot(200, self._finish_rep_and_save)
                return

        # All points received (or timeout) — disconnect and finalise
        if self._qs_callback_connected:
            PVMonitor().value_changed.disconnect(self._on_qs_motor_callback)
            self._qs_callback_connected = False
        self._qs_active       = False
        self._qs_scan_started = False
        self._qs_wait_total   = 0.0
        elapsed    = time.monotonic() - getattr(self, "_qs_t0", time.monotonic())
        rep_total  = getattr(self, "_xas_rep_total", 1)
        accum_file = getattr(self, "_xas_accum_file", "")

        # Finalise the last rep
        plot.finish_accum_rep()
        self._xas_rep_idx += 1

        # Redraw and save
        plot.redraw_accum()
        if accum_file:
            plot.save_accumulated(accum_file)

        if rep_total > 1:
            self._log_msg(
                f"✔ All {rep_total} scans complete  ({elapsed:.1f} s)  "
                f"accum → {Path(accum_file).name}", PAL["ok"])
            self._xas_plot.finish(
                f"Complete  ({rep_total}×)  —  {Path(accum_file).name}")
        else:
            self._log_msg(
                f"✔ QServer scan finished  ({elapsed:.1f} s)", PAL["ok"])
            self._xas_plot.finish("QServer scan complete")

        self._set_state(_ScanState.IDLE)
        self._st_file.setText("—")
        self._st_file.setStyleSheet(
            f"color:{PAL['subtext']}; font-family:monospace; font-size:8pt;")
        self._qs_pending = {}
        self._advance_counter()

    def _on_qs_motor_callback(self, pv_name: str, value) -> None:
        """Fires on every PVMonitor value_changed while a QS scan is active."""
        if not self._qs_active:
            return
        if pv_name != self._qs_motor_pv:
            return
        if value is None:
            return
        try:
            x = float(value)
        except (TypeError, ValueError):
            return

        now      = time.monotonic()
        same_pos = (self._qs_last_x is not None
                    and abs(x - self._qs_last_x) < 1e-6)
        if same_pos:
            return   # motor hasn't moved — ignore regardless of timing
        too_soon = (now - self._qs_last_t) < self._qs_min_dt
        if too_soon:
            return
        self._qs_last_x = x
        self._qs_last_t = now

        if EPICS_AVAILABLE and self._qs_det_pv:
            raw = PVMonitor().get(self._qs_det_pv)
            y = float(raw) if raw is not None else float("nan")
        else:
            y = sim_scalar([{"_x": x}], len(self._xas_plot._xs_accum))

        self._xas_plot.add_point_to_accum(x, y, n_rep=self._qs_n_steps)
        if hasattr(self, "_qs_raw_xs"):
            self._qs_raw_xs.append(x)
        n_pts = len(self._xas_plot._xs_build)
        n     = self._qs_n_steps
        self._st_point.setText(f"{n_pts}" + (f" / {n}" if n else ""))
        if n:
            self._progress.setValue(min(n_pts, n))

    def _qs_update_elapsed(self) -> None:
        """Update the elapsed-time label once per second."""
        if self._qs_active:
            self._st_elapsed.setText(f"{time.monotonic() - self._qs_t0:.1f} s")

    def _qs_record_now(self) -> None:
        """Sample motor and detector PVs immediately and record a point.

        Called at scan-start confirmation to capture the first position,
        which may not generate a motor callback if the motor was already there.
        """
        if not self._qs_active or not self._qs_motor_pv:
            return
        if EPICS_AVAILABLE:
            raw_x = PVMonitor().get(self._qs_motor_pv)
            x = float(raw_x) if raw_x is not None else None
        else:
            idx  = self._xas_scan_combo.currentIndex()
            defn = (self._xas_scan_defs[idx]
                    if 0 <= idx < len(self._xas_scan_defs) else None)
            positions = defn.all_positions() if defn else []
            x = positions[0] if positions else None

        if x is None:
            return
        if self._qs_last_x is not None and abs(x - self._qs_last_x) < 1e-6:
            return
        self._qs_last_x = x
        self._qs_last_t = time.monotonic()

        if EPICS_AVAILABLE and self._qs_det_pv:
            raw_y = PVMonitor().get(self._qs_det_pv)
            y = float(raw_y) if raw_y is not None else float("nan")
        else:
            y = sim_scalar([{"_x": x}], len(self._xas_plot._xs_accum))

        self._xas_plot.add_point_to_accum(x, y, n_rep=self._qs_n_steps)
        if hasattr(self, "_qs_raw_xs"):
            self._qs_raw_xs.append(x)
        n_pts = len(self._xas_plot._xs_accum)
        n     = self._qs_n_steps
        self._st_point.setText(f"{n_pts}" + (f" / {n}" if n else ""))
        if n:
            self._progress.setValue(min(n_pts, n))

    # ── Live config updates ───────────────────────────────────────────────────
    def apply_config(self, key: str, value):
        """Receive a config_changed signal from ConfigurationTab."""
        if key == "data_acquisition.default_output_dir" and value:
            self._dir_edit.setText(str(Path(str(value)).expanduser()))
        if key == "data_acquisition.filename_mode":
            self._update_filename_display()

    # ── Config helpers ────────────────────────────────────────────────────────
    @staticmethod
    def _collect_pvs(cfg: dict, section: str) -> dict:
        out = {}
        for k, v in cfg.get(section, {}).items():
            if isinstance(v, str):
                out[k] = v
            elif isinstance(v, dict):
                for sk, sv in v.items():
                    if isinstance(sv, str):
                        out[f"{k}:{sk}"] = sv
        return out

    # ── UI builders ───────────────────────────────────────────────────────────
    def _build_file_group(self):
        grp = QGroupBox("File Output"); grp.setStyleSheet(GRP_STYLE)
        gl = QGridLayout(grp); gl.setContentsMargins(8,20,8,8); gl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']};"); return lb

        gl.addWidget(ql("Directory"), 0, 0)
        default_dir = str(Path.home() / "Data")
        if self._config_tab is not None:
            cfg_dir = self._config_tab.get("data_acquisition.default_output_dir")
            if cfg_dir:
                default_dir = str(Path(cfg_dir).expanduser())
        self._dir_edit = QLineEdit(default_dir)
        self._dir_edit.setStyleSheet(INPUT_STYLE)
        gl.addWidget(self._dir_edit, 0, 1)
        browse_btn = QPushButton("…"); browse_btn.setStyleSheet(BTN_STYLE)
        browse_btn.setFixedWidth(28); browse_btn.clicked.connect(self._browse_dir)
        gl.addWidget(browse_btn, 0, 2)

        gl.addWidget(ql("File prefix"), 1, 0)
        self._prefix_edit = QLineEdit("scan")
        self._prefix_edit.setStyleSheet(INPUT_STYLE)
        gl.addWidget(self._prefix_edit, 1, 1, 1, 2)

        gl.addWidget(ql("Format"), 2, 0)
        self._fmt_combo = QComboBox(); self._fmt_combo.setStyleSheet(COMBO_STYLE)
        for fmt in ("HDF5 (.h5)", "CSV (.csv)", "SPEC (.dat)"):
            self._fmt_combo.addItem(fmt)
        gl.addWidget(self._fmt_combo, 2, 1, 1, 2)

        gl.addWidget(ql("File name"), 3, 0)
        self._filename_display = QLineEdit()
        self._filename_display.setReadOnly(True)
        self._filename_display.setStyleSheet(
            INPUT_STYLE + "color: " + PAL["subtext"] + ";")
        self._filename_display.setPlaceholderText("—")
        gl.addWidget(self._filename_display, 3, 1, 1, 2)

        self._dated_folders = QCheckBox("Dated folders  (YYYYMMDD/)")
        self._dated_folders.setStyleSheet(CHECK_STYLE)
        self._dated_folders.setChecked(False)
        gl.addWidget(self._dated_folders, 4, 0, 1, 3)

        # Wire live filename preview
        self._prefix_edit.textChanged.connect(self._update_filename_display)
        self._fmt_combo.currentIndexChanged.connect(self._update_filename_display)
        self._dated_folders.stateChanged.connect(self._update_filename_display)
        self._dir_edit.textChanged.connect(self._update_filename_display)

        # Dummy _scan_num and _auto_inc kept for compatibility with scan engine
        self._scan_num = QSpinBox(); self._scan_num.setRange(1, 99999)
        self._scan_num.setValue(self._load_counter()); self._scan_num.setVisible(False)
        self._auto_inc = QCheckBox(); self._auto_inc.setChecked(True)
        self._auto_inc.setVisible(False)

        return grp

    def _build_scan_group(self):
        grp = QGroupBox("1-D Scan Parameters"); grp.setStyleSheet(GRP_STYLE)
        gl = QGridLayout(grp); gl.setContentsMargins(8,20,8,8); gl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']};"); return lb

        gl.addWidget(ql("Motor"), 0, 0)
        self._motor_combo = QComboBox(); self._motor_combo.setStyleSheet(COMBO_STYLE)
        for n in self._motor_pvs: self._motor_combo.addItem(n)
        gl.addWidget(self._motor_combo, 0, 1, 1, 3)

        gl.addWidget(ql("Detector"), 1, 0)
        self._det_combo = QComboBox(); self._det_combo.setStyleSheet(COMBO_STYLE)
        for n in {**self._signal_pvs, **self._det_pvs}: self._det_combo.addItem(n)
        gl.addWidget(self._det_combo, 1, 1, 1, 3)

        for row, (lbl, attr, val, lo, hi, dec) in enumerate([
            ("Start",    "_p_start", -10.0, -1e6, 1e6, 4),
            ("Stop",     "_p_stop",   10.0, -1e6, 1e6, 4),
            ("Steps",    None,        None, None, None, None),
            ("Exposure", "_p_exp",    0.5,  0.001, 3600.0, 3),
        ], 2):
            gl.addWidget(ql(lbl), row, 0)
            if lbl == "Steps":
                self._p_steps = QSpinBox(); self._p_steps.setRange(2, 10000)
                self._p_steps.setValue(21); self._p_steps.setStyleSheet(SPIN_STYLE)
                gl.addWidget(self._p_steps, row, 1, 1, 3)
            else:
                sp = QDoubleSpinBox(); sp.setRange(lo, hi); sp.setValue(val)
                sp.setDecimals(dec); sp.setStyleSheet(SPIN_STYLE)
                setattr(self, attr, sp); gl.addWidget(sp, row, 1, 1, 3)

        self._rel_scan = QCheckBox("Relative scan (from current position)")
        self._rel_scan.setStyleSheet(CHECK_STYLE)
        gl.addWidget(self._rel_scan, 6, 0, 1, 4)
        return grp

    def _build_run_group(self):
        grp = QGroupBox("Run Control"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8,20,8,8); vl.setSpacing(8)

        btn_row = QHBoxLayout(); btn_row.setSpacing(6)
        self._run_btn   = QPushButton("▶  Run");   self._run_btn.setStyleSheet(BTN_STYLE)
        self._pause_btn = QPushButton("⏸  Pause"); self._pause_btn.setStyleSheet(BTN_STYLE)
        self._abort_btn = QPushButton("■  Abort"); self._abort_btn.setStyleSheet(BTN_STYLE)
        self._pause_btn.setEnabled(False); self._abort_btn.setEnabled(False)
        self._run_btn.clicked.connect(self._start_scan)
        self._pause_btn.clicked.connect(self._pause_scan)
        self._abort_btn.clicked.connect(self._abort_scan)
        for b in (self._run_btn, self._pause_btn, self._abort_btn): btn_row.addWidget(b)
        vl.addLayout(btn_row)

        self._progress = QProgressBar(); self._progress.setValue(0)
        self._progress.setTextVisible(True); self._progress.setFixedHeight(14)
        self._progress.setFormat("%v / %m  (%p%)")
        self._progress.setStyleSheet(f"""
            QProgressBar {{ background:{PAL['bg']}; border:1px solid #2a3a5e;
                            border-radius:3px; color:{PAL['text']}; font-size:7pt; }}
            QProgressBar::chunk {{ background:{PAL['accent']}; border-radius:2px; }}
        """)
        vl.addWidget(self._progress)
        return grp

    def _build_status_group(self):
        grp = QGroupBox("Status"); grp.setStyleSheet(GRP_STYLE)
        gl = QGridLayout(grp); gl.setContentsMargins(8,20,8,8); gl.setSpacing(6)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;"); return lb

        def vl(text="#9e9e9e"):
            lb = QLabel("—"); lb.setStyleSheet(f"color:{text}; font-family:monospace; font-size:8pt;")
            return lb

        self._st_state   = vl(); self._st_file = vl()
        self._st_point   = vl(); self._st_elapsed = vl()
        for row, (k, v) in enumerate([
            ("State",   self._st_state),
            ("File",    self._st_file),
            ("Point",   self._st_point),
            ("Elapsed", self._st_elapsed),
        ]):
            gl.addWidget(ql(k), row, 0); gl.addWidget(v, row, 1)
        self._set_state(_ScanState.IDLE)
        return grp

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _browse_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Select output directory",
                                             self._dir_edit.text())
        if d: self._dir_edit.setText(d)

    def _add_detector(self):
        name = self._det_name_edit.text().strip()
        pv   = self._det_pv_edit.text().strip()
        if not name or not pv:
            self._log_msg("⚠  Enter both a name and a PV string.", PAL["warn"]); return
        self._det_table.add_detector(name, pv)
        self._det_name_edit.clear(); self._det_pv_edit.clear()
        self._log_msg(f"Added detector '{name}'  →  {pv}", PAL["ok"])

    def _log_msg(self, text: str, color: str = ""):
        ts = datetime.now().strftime("%H:%M:%S")
        if color:
            self._log.append(f'<span style="color:{PAL["subtext"]}">[{ts}]</span> '
                             f'<span style="color:{color}">{text}</span>')
        else:
            self._log.append(f'<span style="color:{PAL["subtext"]}">[{ts}]</span> {text}')

    # ── Scan counter persistence ──────────────────────────────────────────────
    def _load_counter(self) -> int:
        """Read the next scan number for the current prefix from disk."""
        try:
            import json as _json
            data = _json.loads(self._counter_path.read_text())
            prefix = self._prefix_edit.text().strip() if hasattr(self, "_prefix_edit") else "scan"
            return int(data.get(prefix, 1))
        except Exception:
            return 1

    def _save_counter(self, num: int):
        """Persist the next scan number for the current prefix."""
        try:
            import json as _json
            data: dict = {}
            try:
                data = _json.loads(self._counter_path.read_text())
            except Exception:
                pass
            prefix = self._prefix_edit.text().strip() or "scan"
            data[prefix] = num
            self._counter_path.write_text(_json.dumps(data, indent=2))
        except Exception as e:
            self._log_msg(f"[warn] Could not save scan counter: {e}", PAL.get("warn", "#ffaa00"))

    def _filename_mode(self) -> str:
        """Return 'number' or 'timestamp' from config; default 'timestamp'."""
        if self._config_tab is not None:
            mode = self._config_tab.get("data_acquisition.filename_mode")
            if mode in ("number", "timestamp"):
                return mode
        return "timestamp"

    def _update_filename_display(self, *_):
        """Refresh the read-only File name field to show the next filename stem."""
        if not hasattr(self, "_filename_display"):
            return
        fname = self._next_filename(peek=True)
        self._filename_display.setText(Path(fname).name)

    def _next_filename(self, peek: bool = False) -> str:
        """Return the full path for the next file.

        In 'number' mode, reads the counter from disk.  When *peek* is False
        the counter is NOT incremented (call _save_counter after a scan
        completes to advance it).
        """
        base   = Path(self._dir_edit.text().strip() or ".")
        if self._dated_folders.isChecked():
            dated = base / datetime.now().strftime("%Y%m%d")
            try:
                dated.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                self._log_msg(f"[warn] Could not create dated folder: {e}",
                              PAL.get("warn", "#ffaa00"))
                dated = base
            d = dated
        else:
            d = base
        prefix = self._prefix_edit.text().strip() or "scan"
        ext    = {0: ".h5", 1: ".csv", 2: ".dat"}.get(
            self._fmt_combo.currentIndex(), ".h5")
        mode = self._filename_mode()
        if mode == "number":
            num  = self._load_counter()
            stem = f"{prefix}_{num:04d}"
        else:
            stem = datetime.now().strftime("%Y%m%dT%H%M%S")
        return str(d / f"{stem}{ext}")

    def _current_filename(self) -> str:
        """Return the filename for the current scan and advance the counter."""
        path = self._next_filename(peek=False)
        # Update hidden spinbox for legacy compatibility
        if self._filename_mode() == "number":
            num = self._load_counter()
            self._scan_num.setValue(num)
        self._update_filename_display()
        return path

    def _advance_counter(self):
        """Increment and persist the scan counter (number mode only)."""
        if self._filename_mode() == "number":
            num = self._load_counter() + 1
            self._scan_num.setValue(num)
            self._save_counter(num)
            self._update_filename_display()

    def _set_state(self, state: str):
        self._state = state
        colors = {
            _ScanState.IDLE:     PAL["subtext"],
            _ScanState.RUNNING:  PAL["ok"],
            _ScanState.PAUSED:   PAL["warn"],
            _ScanState.ABORTING: PAL["nc"],
        }
        self._st_state.setText(state)
        self._st_state.setStyleSheet(
            f"color:{colors.get(state, PAL['text'])}; font-family:monospace; font-size:8pt;")

    # ── Scan engine ───────────────────────────────────────────────────────────
    def _start_scan(self):
        if self._state == _ScanState.RUNNING: return
        n     = self._p_steps.value()
        start = self._p_start.value()
        stop  = self._p_stop.value()
        step  = (stop - start) / max(n - 1, 1)
        self._scan_positions = [start + i*step for i in range(n)]
        self._scan_idx       = 0
        self._scan_t0        = time.monotonic()
        fname = self._current_filename()
        motor = self._motor_combo.currentText()
        det   = self._det_combo.currentText()
        self._xas_plot.reset(motor, det, f"{det}  vs  {motor}")
        self._progress.setMaximum(n); self._progress.setValue(0)
        self._st_file.setText(Path(fname).name)
        self._st_file.setStyleSheet(f"color:{PAL['text']}; font-family:monospace; font-size:8pt;")
        self._log_msg(f"▶ Scan started  →  {fname}", PAL["ok"])
        self._log_msg(f"   Motor: {motor}  |  Det: {det}  |  {n} points  "
                      f"[{start:.4g} → {stop:.4g}]")
        self._set_state(_ScanState.RUNNING)
        self._run_btn.setEnabled(False)
        self._pause_btn.setEnabled(True); self._abort_btn.setEnabled(True)
        self._scan_timer.start(SCAN_DWELL_MS)

    def _pause_scan(self):
        if self._state == _ScanState.RUNNING:
            self._scan_timer.stop()
            self._set_state(_ScanState.PAUSED)
            self._pause_btn.setText("▶  Resume")
            self._log_msg("⏸ Scan paused.", PAL["warn"])
        elif self._state == _ScanState.PAUSED:
            self._set_state(_ScanState.RUNNING)
            self._pause_btn.setText("⏸  Pause")
            self._log_msg("▶ Scan resumed.", PAL["ok"])
            self._scan_timer.start(SCAN_DWELL_MS)

    def _abort_scan(self):
        self._scan_timer.stop()
        self._set_state(_ScanState.ABORTING)
        self._log_msg("■ Scan aborted.", PAL["nc"])
        self._xas_plot.finish("Scan aborted")
        self._reset_run_btns()

    def _scan_step(self):
        if self._state != _ScanState.RUNNING:
            self._scan_timer.stop(); return
        if self._scan_idx >= len(self._scan_positions):
            self._finish_scan(); return

        x   = self._scan_positions[self._scan_idx]
        pv  = self._motor_pvs.get(self._motor_combo.currentText(), "")
        if pv: PVMonitor().put(pv, x)

        # Read detector (simulated if no EPICS)
        sig_pv = ({**self._signal_pvs, **self._det_pvs}
                  .get(self._det_combo.currentText(), ""))
        if EPICS_AVAILABLE and sig_pv:
            raw = PVMonitor().get(sig_pv)
            y   = float(raw) if raw is not None else float("nan")
        else:
            mid = (self._scan_positions[0] + self._scan_positions[-1]) / 2
            rng = abs(self._scan_positions[-1] - self._scan_positions[0]) or 1
            y   = (500 * math.exp(-0.5*((x-mid)/(rng*0.15))**2)
                   + random.gauss(0, 5))

        self._xas_plot.add_point(x, y)
        self._scan_idx += 1
        self._progress.setValue(self._scan_idx)
        elapsed = time.monotonic() - self._scan_t0
        self._st_elapsed.setText(f"{elapsed:.1f} s")
        self._st_point.setText(f"{self._scan_idx} / {len(self._scan_positions)}")

        if self._scan_idx >= len(self._scan_positions):
            self._finish_scan()

    def _finish_scan(self):
        self._scan_timer.stop()
        fname = self._current_filename()
        self._xas_plot.finish(f"Scan complete — {Path(fname).name}")
        self._log_msg(f"✔ Scan complete.  File: {fname}", PAL["ok"])
        self._advance_counter()
        self._set_state(_ScanState.IDLE)
        self._reset_run_btns()

    def _reset_run_btns(self):
        self._run_btn.setEnabled(True)
        self._pause_btn.setEnabled(False); self._pause_btn.setText("⏸  Pause")
        self._abort_btn.setEnabled(False)

    # ── RE controls panel ─────────────────────────────────────────────────────
    def _build_re_controls(self) -> QGroupBox:
        grp = QGroupBox("RE Controls"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8,20,8,8); vl.setSpacing(6)

        def _btn(label, slot):
            b = QPushButton(label); b.setStyleSheet(BTN_STYLE)
            b.clicked.connect(slot); return b

        row1 = QHBoxLayout(); row1.setSpacing(4)
        self._re_start_btn  = _btn("▶  Start Queue", self._re_start)
        self._re_stop_btn   = _btn("⏹  Stop Queue",  self._re_stop)
        row1.addWidget(self._re_start_btn)
        row1.addWidget(self._re_stop_btn)
        vl.addLayout(row1)

        row2 = QHBoxLayout(); row2.setSpacing(4)
        self._re_pause_btn  = _btn("⏸  Pause",  self._re_pause)
        self._re_resume_btn = _btn("▶  Resume", self._re_resume)
        self._re_abort_btn  = _btn("■  Abort",  self._re_abort)
        for b in (self._re_pause_btn, self._re_resume_btn, self._re_abort_btn):
            row2.addWidget(b)
        vl.addLayout(row2)
        return grp

    def _re_call(self, method: str, colour: str):
        qt = getattr(self, "_queue_tab", None)
        if qt is None:
            self._log_msg(f"[warn] Queue tab not connected — cannot call {method}", PAL["nc"])
            return
        r = qt._api(method)
        from queue_tab import _result_str, _ts
        self._log_msg(f"[{_ts()}] {method} → {_result_str(r)}", colour)

    def _re_start(self):  self._re_call("queue_start", PAL["ok"])
    def _re_stop(self):   self._re_call("queue_stop",  PAL["warn"])
    def _re_pause(self):  self._re_call("re_pause",    PAL["warn"])
    def _re_resume(self): self._re_call("re_resume",   PAL["ok"])
    def _re_abort(self):  self._re_call("re_abort",    PAL["nc"])

    # ── Queue submission ──────────────────────────────────────────────────────
    def _on_xas_queue(self):
        qt = getattr(self, "_queue_tab", None)
        if qt is None:
            self._log_msg("[warn] Queue tab not connected", PAL["nc"]); return
        idx = self._xas_scan_combo.currentIndex()
        if idx < 0 or idx >= len(self._xas_scan_defs):
            self._log_msg("[warn] No XAS scan selected", PAL["nc"]); return
        defn = self._xas_scan_defs[idx]
        from scan_types import XASScan
        scan          = XASScan(defn)
        det_friendly  = self._det_combo.currentText()
        det_ophyd     = device_name(det_friendly)
        motor_ophyd   = device_name(defn.motor)
        plan_name, args, kwargs = scan.to_plan(
            motor_ophyd, det_ophyd,
            exposure_time=getattr(defn, "exposure_time", 1.0))

        n_scans   = self._xas_n_scans.value()
        base_file = self._current_filename()   # e.g. ~/Data/20260506/scan_0001.h5
        base_path = Path(base_file)
        stem      = base_path.stem             # e.g. scan_0001
        suffix    = base_path.suffix           # e.g. .h5
        parent    = base_path.parent

        # Accumulated file — same name as base (no rep suffix)
        accum_file = str(base_path)

        rep_files = []
        for rep in range(1, n_scans + 1):
            if n_scans > 1:
                rep_file = str(parent / f"{stem}_rep{rep:02d}{suffix}")
            else:
                rep_file = accum_file
            rep_files.append(rep_file)
            meta = {
                "scan_name"     : defn.name,
                "scan_type"     : "XAS",
                "n_points"      : defn.n_points(),
                "repetition"    : rep,
                "n_repetitions" : n_scans,
                "file"          : rep_file,
                "accumulate_file": accum_file,
            }
            ok = qt.add_plan(plan_name, args, kwargs, meta)
            if not ok:
                self._log_msg(
                    f"✘ Failed to add rep {rep}/{n_scans} to queue", PAL["nc"])
                return

        self._xas_rep_total = n_scans
        self._xas_rep_idx   = 0
        self._xas_rep_files = rep_files
        self._xas_accum_file = accum_file

        if n_scans > 1:
            self._log_msg(
                f"✔ XAS '{defn.name}'  ×{n_scans} added to queue  "
                f"({defn.n_points()} pts/scan)  accum → {Path(accum_file).name}",
                PAL["ok"])
        else:
            self._log_msg(
                f"✔ XAS '{defn.name}' added to queue  ({defn.n_points()} pts)",
                PAL["ok"])

        motor_pv = self._motor_pvs.get(defn.motor, "")
        det_pv   = ({**self._signal_pvs, **self._det_pvs}).get(det_friendly, "")
        self._qs_arm(motor_pv, det_pv, defn.motor, det_friendly,
                     defn.n_points(), plan_name)

    def _on_rixs_queue(self):
        # Placeholder — RIXS plan submission will be implemented with RIXSScanPlot
        self._log_msg("[info] RIXS queue submission not yet implemented", PAL["subtext"])

    # ── XAS scan persistence ──────────────────────────────────────────────────
    def _load_xas_scans(self):
        try:
            import json
            data = json.loads(XAS_SCANS_PATH.read_text())
            self._xas_scan_defs = [XASScanDef.from_dict(d) for d in data]
        except Exception:
            self._xas_scan_defs = []

    def _save_xas_scans(self):
        try:
            import json
            XAS_SCANS_PATH.parent.mkdir(parents=True, exist_ok=True)
            XAS_SCANS_PATH.write_text(
                json.dumps([d.to_dict() for d in self._xas_scan_defs], indent=2))
        except Exception as e:
            self._log_msg(f"[warn] Could not save XAS scans: {e}", PAL["warn"])

    def _refresh_xas_combo(self):
        self._xas_scan_combo.blockSignals(True)
        self._xas_scan_combo.clear()
        for d in self._xas_scan_defs:
            self._xas_scan_combo.addItem(d.name)
        self._xas_scan_combo.blockSignals(False)
        self._on_xas_scan_selected(self._xas_scan_combo.currentIndex())

    def _on_new_xas_scan(self):
        dlg = XASScanDialog(
            motor_names=list(self._motor_pvs.keys()),
            default_motor=self._xas_default_motor,
            parent=self,
        )
        if dlg.exec() == QDialog.Accepted and dlg.result_def():
            defn = dlg.result_def()
            self._xas_scan_defs.append(defn)
            self._save_xas_scans()
            self._refresh_xas_combo()
            self._xas_scan_combo.setCurrentIndex(len(self._xas_scan_defs) - 1)

    def _on_edit_xas_scan(self):
        idx = self._xas_scan_combo.currentIndex()
        if idx < 0 or idx >= len(self._xas_scan_defs):
            return
        dlg = XASScanDialog(
            motor_names=list(self._motor_pvs.keys()),
            default_motor=self._xas_default_motor,
            existing=self._xas_scan_defs[idx],
            parent=self,
        )
        if dlg.exec() == QDialog.Accepted and dlg.result_def():
            self._xas_scan_defs[idx] = dlg.result_def()
            self._save_xas_scans()
            self._refresh_xas_combo()
            self._xas_scan_combo.setCurrentIndex(idx)

    def _on_delete_xas_scan(self):
        idx = self._xas_scan_combo.currentIndex()
        if idx < 0 or idx >= len(self._xas_scan_defs):
            return
        name = self._xas_scan_defs[idx].name
        reply = QMessageBox.question(
            self, "Delete scan",
            f"Delete '{name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            del self._xas_scan_defs[idx]
            self._save_xas_scans()
            self._refresh_xas_combo()

    def _on_xas_scan_selected(self, idx: int):
        if idx < 0 or idx >= len(self._xas_scan_defs):
            self._xas_info_lbl.setText("No scan selected")
            return
        d = self._xas_scan_defs[idx]
        lo  = min(sr.start for sr in d.subranges)
        hi  = max(sr.stop  for sr in d.subranges)
        exp = getattr(d, "exposure_time", 1.0)
        self._xas_info_lbl.setText(
            f"{d.motor}  [{lo:.3f} → {hi:.3f} eV]  "
            f"{len(d.subranges)} range(s)  ·  {d.n_points()} pts  ·  "
            f"{exp:.2f} s/pt")

    # ── RIXS excitation energy list management ────────────────────────────────
    def _refresh_rixs_energy_lists(self):
        """Sync both the XAS-panel list and the RIXS-panel read-only list."""
        for lw in (self._rixs_energy_list,
                   getattr(self, "_rixs_from_xas_list", None)):
            if lw is None:
                continue
            lw.clear()
            for e in self._rixs_energies:
                lw.addItem(QListWidgetItem(f"{e:.4f} eV"))

    def _on_add_rixs_point(self):
        """Add the current XAS cursor x-position to the RIXS energy list."""
        txt = self._xas_plot._ro_pos.text().strip()
        if txt == "—" or not txt:
            self._log_msg("[warn] No XAS cursor position — hold left mouse button "
                          "over the XAS plot to set cursor.", PAL["warn"])
            return
        try:
            energy = float(txt)
        except ValueError:
            self._log_msg(f"[warn] Could not parse cursor position: {txt!r}", PAL["warn"])
            return
        if energy not in self._rixs_energies:
            self._rixs_energies.append(energy)
            self._rixs_energies.sort()
            self._refresh_rixs_energy_lists()
            self._log_msg(f"RIXS point added: {energy:.4f} eV  "
                          f"({len(self._rixs_energies)} total)", PAL["ok"])
        else:
            self._log_msg(f"[info] {energy:.4f} eV already in RIXS list", PAL["subtext"])

    def _on_delete_rixs_point(self):
        """Delete the selected energy from the RIXS excitation energy list."""
        row = self._rixs_energy_list.currentRow()
        if row < 0 or row >= len(self._rixs_energies):
            return
        energy = self._rixs_energies.pop(row)
        self._refresh_rixs_energy_lists()
        self._log_msg(f"RIXS point removed: {energy:.4f} eV", PAL["subtext"])

    # ── XAS scan control panel ────────────────────────────────────────────────
    def _build_xas_panel(self):
        grp = QGroupBox("XAS Scan"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8,20,8,8); vl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
            return lb

        # ── New / Edit / Delete scan buttons ─────────────────────────────────
        scan_mgmt_row = QHBoxLayout(); scan_mgmt_row.setSpacing(4)
        new_scan_btn  = QPushButton("New");    new_scan_btn.setStyleSheet(BTN_STYLE)
        edit_scan_btn = QPushButton("Edit");   edit_scan_btn.setStyleSheet(BTN_STYLE)
        del_scan_btn  = QPushButton("Delete"); del_scan_btn.setStyleSheet(BTN_STYLE)
        new_scan_btn.clicked.connect(self._on_new_xas_scan)
        edit_scan_btn.clicked.connect(self._on_edit_xas_scan)
        del_scan_btn.clicked.connect(self._on_delete_xas_scan)
        for b in (new_scan_btn, edit_scan_btn, del_scan_btn):
            scan_mgmt_row.addWidget(b)
        vl.addLayout(scan_mgmt_row)

        # ── Scan selector combo ───────────────────────────────────────────────
        vl.addWidget(ql("Saved scans"))
        self._xas_scan_combo = QComboBox(); self._xas_scan_combo.setStyleSheet(COMBO_STYLE)
        self._xas_scan_combo.currentIndexChanged.connect(self._on_xas_scan_selected)
        vl.addWidget(self._xas_scan_combo)

        # Info label showing selected scan summary
        self._xas_info_lbl = QLabel("No scan selected")
        self._xas_info_lbl.setStyleSheet(
            f"color:{PAL['subtext']}; font-size:7pt; padding:2px 0;")
        self._xas_info_lbl.setWordWrap(True)
        vl.addWidget(self._xas_info_lbl)

        # Populate combo from already-loaded scans
        self._refresh_xas_combo()

        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep)

        # Multiple scans spinbox
        rep_row = QHBoxLayout(); rep_row.setSpacing(6)
        rep_row.addWidget(ql("Multiple scans"))
        self._xas_n_scans = QSpinBox()
        self._xas_n_scans.setRange(1, 999); self._xas_n_scans.setValue(1)
        self._xas_n_scans.setStyleSheet(SPIN_STYLE); self._xas_n_scans.setFixedWidth(70)
        rep_row.addWidget(self._xas_n_scans); rep_row.addStretch()
        vl.addLayout(rep_row)

        # Queue / Pause / Abort
        btn_row = QHBoxLayout(); btn_row.setSpacing(4)
        self._xas_queue_btn = QPushButton("+ Queue"); self._xas_queue_btn.setStyleSheet(BTN_STYLE)
        self._xas_pause_btn = QPushButton("Pause");   self._xas_pause_btn.setStyleSheet(BTN_STYLE)
        self._xas_abort_btn = QPushButton("Abort");   self._xas_abort_btn.setStyleSheet(BTN_STYLE)
        self._xas_queue_btn.clicked.connect(self._on_xas_queue)
        for b in (self._xas_queue_btn, self._xas_pause_btn, self._xas_abort_btn):
            btn_row.addWidget(b)
        vl.addLayout(btn_row)

        sep2 = QFrame(); sep2.setFrameShape(QFrame.HLine)
        sep2.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep2)

        # ── RIXS excitation energy list ───────────────────────────────────────
        vl.addWidget(ql("RIXS excitation energies"))

        self._rixs_energy_list = QListWidget()
        self._rixs_energy_list.setStyleSheet(f"""
            QListWidget {{
                background:{PAL['surface']}; color:{PAL['text']};
                border:1px solid #2a3a5e; font-family:monospace; font-size:8pt;
            }}
            QListWidget::item:selected {{ background:#1e3a5e; }}
        """)
        self._rixs_energy_list.setMaximumHeight(100)
        self._rixs_energy_list.setSelectionMode(QListWidget.SingleSelection)
        vl.addWidget(self._rixs_energy_list)

        rixs_pt_row = QHBoxLayout(); rixs_pt_row.setSpacing(4)
        add_rixs_btn = QPushButton("Add RIXS Point")
        add_rixs_btn.setStyleSheet(BTN_STYLE)
        add_rixs_btn.setToolTip("Add current XAS cursor position to RIXS excitation energy list")
        add_rixs_btn.clicked.connect(self._on_add_rixs_point)
        del_rixs_btn = QPushButton("Delete RIXS Point")
        del_rixs_btn.setStyleSheet(BTN_STYLE)
        del_rixs_btn.setToolTip("Delete selected energy from RIXS excitation energy list")
        del_rixs_btn.clicked.connect(self._on_delete_rixs_point)
        rixs_pt_row.addWidget(add_rixs_btn)
        rixs_pt_row.addWidget(del_rixs_btn)
        vl.addLayout(rixs_pt_row)

        vl.addStretch()
        return grp

    # ── RIXS scan control panel ───────────────────────────────────────────────
    def _build_rixs_panel(self):
        grp = QGroupBox("RIXS Scan"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8,20,8,8); vl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
            return lb

        # Excitation energy button
        exc_btn = QPushButton("Excitation energy"); exc_btn.setStyleSheet(BTN_STYLE)
        vl.addWidget(exc_btn)

        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep)

        # Mode radio buttons (mutually exclusive)
        self._rixs_mode_grp = QButtonGroup(self)
        mode_row = QHBoxLayout(); mode_row.setSpacing(10)
        for label, attr in (("One", "_rixs_one"), ("Map", "_rixs_map"), ("From XAS", "_rixs_from_xas")):
            rb = QRadioButton(label); rb.setStyleSheet(RADIO_STYLE)
            self._rixs_mode_grp.addButton(rb)
            setattr(self, attr, rb)
            mode_row.addWidget(rb)
        self._rixs_one.setChecked(True)
        vl.addLayout(mode_row)

        # From XAS energy list — shown only when "From XAS" is selected
        self._rixs_from_xas_list = QListWidget()
        self._rixs_from_xas_list.setStyleSheet(f"""
            QListWidget {{
                background:{PAL['surface']}; color:{PAL['text']};
                border:1px solid #2a3a5e; font-family:monospace; font-size:8pt;
            }}
            QListWidget::item:selected {{ background:#1e3a5e; }}
        """)
        self._rixs_from_xas_list.setMaximumHeight(90)
        self._rixs_from_xas_list.setSelectionMode(QListWidget.NoSelection)
        self._rixs_from_xas_list.setVisible(False)
        vl.addWidget(self._rixs_from_xas_list)

        def _on_mode_changed():
            self._rixs_from_xas_list.setVisible(self._rixs_from_xas.isChecked())

        self._rixs_from_xas.toggled.connect(_on_mode_changed)

        sep2 = QFrame(); sep2.setFrameShape(QFrame.HLine)
        sep2.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep2)

        # Start / Stop / Size spinboxes
        gl = QGridLayout(); gl.setSpacing(6)
        for row, (lbl, attr, val, lo, hi, dec) in enumerate([
            ("Start", "_rixs_start", 250.0, 0.0, 10000.0, 3),
            ("Stop",  "_rixs_stop",  270.0, 0.0, 10000.0, 3),
            ("Size",  "_rixs_size",   0.5,  0.0,  1000.0, 4),
        ]):
            gl.addWidget(ql(lbl), row, 0)
            sp = QDoubleSpinBox(); sp.setRange(lo, hi); sp.setValue(val)
            sp.setDecimals(dec); sp.setStyleSheet(SPIN_STYLE)
            setattr(self, attr, sp); gl.addWidget(sp, row, 1)
        vl.addLayout(gl)

        sep3 = QFrame(); sep3.setFrameShape(QFrame.HLine)
        sep3.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep3)

        # Queue / Pause / Abort
        btn_row = QHBoxLayout(); btn_row.setSpacing(4)
        self._rixs_queue_btn = QPushButton("+ Queue"); self._rixs_queue_btn.setStyleSheet(BTN_STYLE)
        self._rixs_pause_btn = QPushButton("Pause");   self._rixs_pause_btn.setStyleSheet(BTN_STYLE)
        self._rixs_abort_btn = QPushButton("Abort");   self._rixs_abort_btn.setStyleSheet(BTN_STYLE)
        self._rixs_queue_btn.clicked.connect(self._on_rixs_queue)
        for b in (self._rixs_queue_btn, self._rixs_pause_btn, self._rixs_abort_btn):
            btn_row.addWidget(b)
        vl.addLayout(btn_row)

        vl.addStretch()
        return grp
