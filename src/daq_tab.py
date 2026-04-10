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
    QHeaderView, QTextEdit, QFileDialog, QRadioButton,
)
from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QFont, QColor

from common import (
    PAL, COMBO_STYLE, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE,
    MPL_AVAILABLE, EPICS_AVAILABLE, TILED_AVAILABLE, TiledWriter, PVMonitor, PVLabel,
)

if MPL_AVAILABLE:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure

import csv
import numpy as np

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import h5py
    H5_AVAILABLE = True
except ImportError:
    H5_AVAILABLE = False

from scan_types import (
        ALL_SCAN_TYPES, DET_SCALAR, DET_AREA,
        detector_kind, sim_scalar,
        _AREA_LABELS, _AREA_COLORS,
    )

from devices import all_device_map

# ── Constants ─────────────────────────────────────────────────────────────────
SCAN_ACQ_TIMEOUT_S = 30.0   # max seconds to wait for area-detector acquire

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
    """Live scan plot — shows the most recent 1-D scan as it executes."""

    def __init__(self, parent=None):
        super().__init__(parent)
        vl = QVBoxLayout(self); vl.setContentsMargins(0,0,0,0)
        if MPL_AVAILABLE:
            self._fig    = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax     = self._fig.add_subplot(111)
            self._line,  = self._ax.plot([], [], color=PAL["accent"],
                                         lw=1.4, marker=".", ms=4)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            vl.addWidget(self._canvas)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph)
        self._xs: list = []; self._ys: list = []
        self.ys: list[float] = []

    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.grid(True, color="#2a3a5e", lw=0.5, ls="--")
        ax.set_xlabel("Motor position", color=PAL["subtext"], fontsize=8)
        ax.set_ylabel("Counts",         color=PAL["subtext"], fontsize=8)
        ax.set_title("No scan yet",     color=PAL["text"],    fontsize=9)

    def reset(self, motor_lbl="Motor position", signal_lbl="Counts", title=""):
        self._xs.clear(); self._ys.clear()
        if not MPL_AVAILABLE: return
        self._line.set_data([], [])
        self._ax.set_xlabel(motor_lbl,  color=PAL["subtext"], fontsize=8)
        self._ax.set_ylabel(signal_lbl, color=PAL["subtext"], fontsize=8)
        self._ax.set_title(title or "Scan in progress …",
                           color=PAL["text"], fontsize=9)
        self._ax.relim(); self._canvas.draw_idle()
        self.ys = []

    def add_point(self, x, y):
        self._xs.append(x); self._ys.append(y)
        if not MPL_AVAILABLE: return
        self._line.set_data(self._xs, self._ys)
        self._ax.relim(); self._ax.autoscale_view(); self._canvas.draw_idle()
        self.ys.append(y)

    def finish(self, title=""):
        if not MPL_AVAILABLE: return
        self._ax.set_title(title or "Scan complete",
                           color=PAL["ok"], fontsize=9)
        self._canvas.draw_idle()

class ScanFileWriter:
    """Write 1-D or multi-column scan data to HDF5, CSV, or SPEC (.dat).

    Column layout
    ─────────────
      col 0          : scan motor (or "_time_s" for time scans)
      col 1          : selected detector  (y-axis / plotted signal)
      cols 2..N      : remaining signals then motors, sorted alphabetically

    Lifecycle:  open() → write_point() × N → close()
    For area detectors, write_image() can be called after each write_point().
    """

    FMT_HDF5 = 0
    FMT_CSV  = 1
    FMT_SPEC = 2

    _ACQ_RBV = ":cam1:Acquire_RBV"   # mirrors DetectorImageViewer constant

    def __init__(self):
        self._fmt       = None
        self._path      = None
        self._meta: dict = {}
        self._columns: list = []
        self._xs:   list = []
        self._data: dict = {}
        self._img_idx   = 0
        # handles
        self._h5file    = None
        self._csvfile   = None
        self._csvwriter = None
        self._specfile  = None

    # ── Public API ────────────────────────────────────────────────────────────

    def open(self, path: str, meta: dict, columns: list) -> bool:
        """Open file and write header.

        meta keys: motor, detector, start, stop, steps,
                   exposure, scan_num, prefix, scan_type, det_kind
        columns  : ordered extra channel names; selected detector must be first.
        """
        self._path, self._meta, self._columns = path, meta, list(columns)
        self._xs = []; self._data = {c: [] for c in self._columns}
        self._img_idx = 0
        ext = Path(path).suffix.lower()
        self._fmt = {".h5": self.FMT_HDF5,
                     ".csv": self.FMT_CSV,
                     ".dat": self.FMT_SPEC}.get(ext, self.FMT_CSV)
        try:
            if   self._fmt == self.FMT_HDF5: return self._open_hdf5()
            elif self._fmt == self.FMT_CSV:  return self._open_csv()
            else:                            return self._open_spec()
        except Exception as exc:
            print(f"[ScanFileWriter] open error: {exc}")
            return False

    def write_point(self, x: float, y: float, extras: dict):
        """Append one row; extras = {channel_name: float} for all columns."""
        self._xs.append(x)
        row = []
        for c in self._columns:
            v = extras.get(c, float("nan"))
            self._data[c].append(v)
            row.append(v)
        try:
            if self._fmt == self.FMT_CSV and self._csvwriter:
                self._csvwriter.writerow(
                    [f"{x:.6g}"] + [_fmt_val(v) for v in row])
                self._csvfile.flush()
            elif self._fmt == self.FMT_SPEC and self._specfile:
                self._specfile.write(
                    "  " + "  ".join([f"{x:.6g}"] +
                                     [_fmt_val(v) for v in row]) + "\n")
                self._specfile.flush()
        except Exception as exc:
            print(f"[ScanFileWriter] write_point error: {exc}")

    def write_image(self, img: "np.ndarray"):
        """Store one area-detector frame (HDF5 only).
        Call immediately after the matching write_point().
        """
        if self._fmt != self.FMT_HDF5 or self._h5file is None:
            return
        try:
            grp = self._h5file["scan"].require_group("images")
            grp.create_dataset(f"img_{self._img_idx:05d}", data=img,
                               compression="gzip", compression_opts=4)
            self._img_idx += 1
        except Exception as exc:
            print(f"[ScanFileWriter] write_image error: {exc}")

    def close(self, aborted: bool = False):
        try:
            if   self._fmt == self.FMT_HDF5: self._close_hdf5(aborted)
            elif self._fmt == self.FMT_CSV:  self._close_csv(aborted)
            elif self._fmt == self.FMT_SPEC: self._close_spec(aborted)
        except Exception as exc:
            print(f"[ScanFileWriter] close error: {exc}")

    # ── HDF5 ──────────────────────────────────────────────────────────────────

    def _open_hdf5(self) -> bool:
        if not H5_AVAILABLE:
            raise RuntimeError("h5py not installed — run: pip install h5py")
        self._h5file = h5py.File(self._path, "w")
        grp  = self._h5file.create_group("scan")
        meta = grp.create_group("metadata")
        for k, v in self._meta.items():
            meta.attrs[k] = str(v)
        meta.attrs["timestamp"]   = datetime.now().isoformat()
        meta.attrs["file_format"] = "AMBER_HiRRIXS_HDF5_v1"
        meta.attrs["beamline"]    = "ALS BL601 AMBER"
        meta.attrs["columns"]     = (
            [self._meta.get("motor", "motor")] + self._columns)
        return True

    def _close_hdf5(self, aborted: bool):
        if not self._h5file: return
        grp  = self._h5file["scan"]
        data = grp.create_group("data")
        motor_name = self._meta.get("motor", "motor")
        ds_x = data.create_dataset(motor_name,
                                   data=np.array(self._xs, dtype=np.float64))
        ds_x.attrs["role"] = "scan_motor"; ds_x.attrs["units"] = "user"
        for i, col in enumerate(self._columns):
            arr  = np.array(self._data[col], dtype=np.float64)
            ds_c = data.create_dataset(col, data=arr)
            ds_c.attrs["role"]  = "detector" if i == 0 else "channel"
            ds_c.attrs["units"] = "counts"
        grp.attrs["n_points"] = len(self._xs)
        grp.attrs["aborted"]  = aborted
        grp.attrs["n_images"] = self._img_idx
        self._h5file.close(); self._h5file = None

    # ── CSV ───────────────────────────────────────────────────────────────────

    def _open_csv(self) -> bool:
        m, now = self._meta, datetime.now()
        self._csvfile = open(self._path, "w", newline="")
        hdr_cols = [m.get("motor", "motor")] + self._columns
        for line in [
            f"# AMBER HiRRIXS scan — {Path(self._path).stem}",
            f"# Date:     {now.strftime('%Y-%m-%d %H:%M:%S')}",
            f"# Beamline: ALS BL601 AMBER",
            f"# Scan type:{m.get('scan_type','?')}",
            f"# Motor:    {m.get('motor','?')}",
            f"# Detector: {m.get('detector','?')}  [{m.get('det_kind','?')}]  (plotted column)",
            f"# Range:    {m.get('start','?')} → {m.get('stop','?')}  ({m.get('steps','?')} pts)",
            f"# Exposure: {m.get('exposure','?')} s",
            f"# Scan #:   {m.get('scan_num','?')}",
            f"# Columns:  {', '.join(hdr_cols)}",
        ]:
            self._csvfile.write(line + "\n")
        self._csvwriter = csv.writer(self._csvfile)
        self._csvwriter.writerow(hdr_cols)
        self._csvfile.flush()
        return True

    def _close_csv(self, aborted: bool):
        if not self._csvfile: return
        if aborted:
            self._csvfile.write(f"# Aborted after {len(self._xs)} points\n")
        self._csvfile.close()
        self._csvfile = None; self._csvwriter = None

    # ── SPEC (.dat) ───────────────────────────────────────────────────────────

    def _open_spec(self) -> bool:
        m, now = self._meta, datetime.now()
        motor    = m.get("motor",    "motor")
        det      = m.get("detector", "detector")
        start    = m.get("start",    0)
        stop     = m.get("stop",     1)
        steps    = m.get("steps",    1)
        exposure = m.get("exposure", 1.0)
        scan_num = m.get("scan_num", 1)
        scan_cmd = m.get("scan_type", "ascan").lower().replace(" ", "_")
        n_cols   = 1 + len(self._columns)

        self._specfile = open(self._path, "w")
        f = self._specfile
        f.write(f"#F {Path(self._path).name}\n")
        f.write(f"#E {int(now.timestamp())}\n")
        f.write(f"#D {now.strftime('%a %b %d %H:%M:%S %Y')}\n")
        f.write(f"#C AMBER HiRRIXS  ALS BL601  User={m.get('prefix','scan')}\n\n")
        f.write(f"#S {scan_num} {scan_cmd} {motor} {start} {stop} "
                f"{steps - 1} {exposure}\n")
        f.write(f"#D {now.strftime('%a %b %d %H:%M:%S %Y')}\n")
        f.write(f"#T {exposure}  (Seconds)\n")
        f.write(f"#C Plotted detector: {det}  [{m.get('det_kind','?')}]\n")
        f.write(f"#N {n_cols}\n")
        f.write(f"#L {motor}  " + "  ".join(self._columns) + "\n")
        f.flush()
        return True

    def _close_spec(self, aborted: bool):
        if not self._specfile: return
        if aborted:
            self._specfile.write(f"#C Aborted after {len(self._xs)} points\n")
        self._specfile.write("\n")
        self._specfile.close(); self._specfile = None

def _fmt_val(v: float) -> str:
    return "nan" if (v != v) else f"{v:.6g}"

# ── DAQ Tab ───────────────────────────────────────────────────────────────────
SCAN_DWELL_MS = 200

class DAQTab(QWidget):
    def __init__(self, amber_cfg: dict, hirrixs_cfg: dict, config_tab=None, app_cfg=None, parent=None):
        # Store for apply_config use
        self._config_tab = config_tab
        self._app_cfg = app_cfg or {}
        tiled_cfg     = self._app_cfg.get("tiled", {})
        self._tiled   = TiledWriter(tiled_cfg)
        super().__init__(parent)
        self.setStyleSheet(f"background:{PAL['bg']};")
        self._state = _ScanState.IDLE
        self._writer: ScanFileWriter | None = None
        self._scan_columns: list = []   # ordered extra channel names
        # active scan type (BaseScan instance, one per type kept alive)
        self._scan_instances = [ST() for ST in ALL_SCAN_TYPES]
        self._device_map: dict = all_device_map(amber_cfg, hirrixs_cfg)
        self._queue_tab = None   # set later via set_queue_tab()
        self._active_scan_idx = 0        # index into _scan_instances
 
        # area-detector state
        self._det_kind   = DET_SCALAR
        self._acquiring  = False
        self._acq_deadline = 0.0
        self._last_outer:      int   = -1
        self._last_area_scalar:float = float("nan")
        # store base PV prefix for each area detector (needed to trigger)
        self._det_pvs:  dict = {}
        self._det_base: dict = {}
        for n, p in hirrixs_cfg.get("detector", {}).items():
            self._det_base[n] = p                  # base prefix, e.g. "6013SIM1"
            self._det_pvs[n]  = p + ":Acquire_RBV" # CA readback PV
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

        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0); outer.setSpacing(0)
        hdr = QLabel("  💾  Data Acquisition")
        hdr.setFont(QFont("Sans Serif",9,QFont.Bold))
        hdr.setStyleSheet(f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        outer.addWidget(hdr)

        # Main horizontal splitter: left controls | right plot+log
        hsplit = QSplitter(Qt.Horizontal); hsplit.setStyleSheet(SPLITTER_STYLE)

        # ── Left panel ────────────────────────────────────────────────────────
        left = QWidget(); left.setStyleSheet(f"background:{PAL['bg']};")
        lv = QVBoxLayout(left); lv.setContentsMargins(8,8,8,8); lv.setSpacing(10)
        lv.addWidget(self._build_file_group())
        lv.addWidget(self._build_scan_group())
        lv.addWidget(self._build_run_group())
        lv.addWidget(self._build_status_group())
        lv.addStretch()
        left.setMinimumWidth(320); left.setMaximumWidth(480)

        scroll_left = QScrollArea(); scroll_left.setWidgetResizable(True)
        scroll_left.setStyleSheet(f"background:{PAL['bg']}; border:none;")
        scroll_left.setWidget(left)
        hsplit.addWidget(scroll_left)

        if config_tab is not None:
            self._seed_from_config(config_tab)

        # ── Right panel ───────────────────────────────────────────────────────
        vsplit = QSplitter(Qt.Vertical); vsplit.setStyleSheet(SPLITTER_STYLE)

        self._scan_plot = ScanPlot()
        vsplit.addWidget(self._scan_plot)

        det_grp = QGroupBox("Live Detector Readouts"); det_grp.setStyleSheet(GRP_STYLE)
        dg_v = QVBoxLayout(det_grp); dg_v.setContentsMargins(6,18,6,6)
        self._det_table = DetectorTable(dict(self._det_pvs))
        dg_v.addWidget(self._det_table)

        # Add-detector row
        add_row = QHBoxLayout(); add_row.setSpacing(6)
        self._det_name_edit = QLineEdit(); self._det_name_edit.setPlaceholderText("Name")
        self._det_name_edit.setStyleSheet(INPUT_STYLE); self._det_name_edit.setFixedWidth(110)
        self._det_pv_edit = QLineEdit(); self._det_pv_edit.setPlaceholderText("PV string")
        self._det_pv_edit.setStyleSheet(INPUT_STYLE)
        add_det_btn = QPushButton("＋ Add"); add_det_btn.setStyleSheet(BTN_STYLE)
        add_det_btn.setFixedWidth(64)
        add_det_btn.clicked.connect(self._add_detector)
        add_row.addWidget(QLabel("Add:")); add_row.addWidget(self._det_name_edit)
        add_row.addWidget(self._det_pv_edit); add_row.addWidget(add_det_btn)
        dg_v.addLayout(add_row)
        vsplit.addWidget(det_grp)

        log_grp = QGroupBox("Acquisition Log"); log_grp.setStyleSheet(GRP_STYLE)
        lg_v = QVBoxLayout(log_grp); lg_v.setContentsMargins(6,18,6,6)
        self._log = QTextEdit(); self._log.setReadOnly(True)
        self._log.setStyleSheet(LOG_STYLE); self._log.setMaximumHeight(160)
        lg_v.addWidget(self._log)
        clr_log_btn = QPushButton("Clear log"); clr_log_btn.setStyleSheet(BTN_STYLE)
        clr_log_btn.setFixedWidth(80); clr_log_btn.clicked.connect(self._log.clear)
        lg_v.addWidget(clr_log_btn, alignment=Qt.AlignRight)
        vsplit.addWidget(log_grp)

        vsplit.setSizes([420, 280, 160])
        hsplit.addWidget(vsplit)
        hsplit.setSizes([360, 1200])
        outer.addWidget(hsplit, 1)

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

    def _seed_from_config(self, config_tab):
        """Populate file-output widgets from configuration.json at startup."""
        out_dir = config_tab.get("data_acquisition.default_output_dir")
        if out_dir:
            import os
            self._dir_edit.setText(os.path.expanduser(out_dir))

        prefix = config_tab.get("data_acquisition.default_prefix")
        if prefix:
            self._prefix_edit.setText(prefix)

        mode = config_tab.get("data_acquisition.filename_mode")
        if mode == "timestamp":
            self._rb_ts.setChecked(True)
        else:
            self._rb_num.setChecked(True)

        auto_inc = config_tab.get("data_acquisition.auto_increment")
        if auto_inc is not None:
            self._auto_inc.setChecked(bool(auto_inc))

        fmt = config_tab.get("data_acquisition.default_format")
        fmt_map = {"HDF5": 0, "CSV": 1, "SPEC": 2}
        if fmt in fmt_map:
            self._fmt_combo.setCurrentIndex(fmt_map[fmt])

        self._update_fname_preview()

    def apply_config(self, key: str, value):
        """Slot wired to ConfigurationTab.config_changed — live updates."""
        import os
        if key == "data_acquisition.default_output_dir":
            self._dir_edit.setText(os.path.expanduser(str(value)))
        elif key == "data_acquisition.default_prefix":
            self._prefix_edit.setText(str(value))
        elif key == "data_acquisition.filename_mode":
            if value == "timestamp":
                self._rb_ts.setChecked(True)
            else:
                self._rb_num.setChecked(True)
        elif key == "data_acquisition.auto_increment":
            self._auto_inc.setChecked(bool(value))
        elif key == "data_acquisition.default_format":
            fmt_map = {"HDF5": 0, "CSV": 1, "SPEC": 2}
            if value in fmt_map:
                self._fmt_combo.setCurrentIndex(fmt_map[value])
        self._update_fname_preview()

    # ── UI builders ───────────────────────────────────────────────────────────
    def _build_file_group(self):
        grp = QGroupBox("File Output"); grp.setStyleSheet(GRP_STYLE)
        gl = QGridLayout(grp); gl.setContentsMargins(8,20,8,8); gl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']};"); return lb

        gl.addWidget(ql("Directory"), 0, 0)
        self._dir_edit = QLineEdit(str(Path.home()))
        self._dir_edit.setStyleSheet(INPUT_STYLE)
        gl.addWidget(self._dir_edit, 0, 1)
        browse_btn = QPushButton("…"); browse_btn.setStyleSheet(BTN_STYLE)
        browse_btn.setFixedWidth(28); browse_btn.clicked.connect(self._browse_dir)
        gl.addWidget(browse_btn, 0, 2)

        gl.addWidget(ql("File prefix"), 1, 0)
        self._prefix_edit = QLineEdit("scan")
        self._prefix_edit.setStyleSheet(INPUT_STYLE); gl.addWidget(self._prefix_edit, 1, 1, 1, 2)

        gl.addWidget(ql("Format"), 2, 0)
        self._fmt_combo = QComboBox(); self._fmt_combo.setStyleSheet(COMBO_STYLE)
        for fmt in ("HDF5 (.h5)", "CSV (.csv)", "SPEC (.dat)"): self._fmt_combo.addItem(fmt)
        gl.addWidget(self._fmt_combo, 2, 1, 1, 2)

        # ── Numbering mode radio buttons ──────────────────────────────────────
        gl.addWidget(ql("Numbering"), 3, 0)
        mode_w = QWidget(); mode_w.setStyleSheet("background:transparent;")
        mode_hl = QHBoxLayout(mode_w); mode_hl.setContentsMargins(0,0,0,0); mode_hl.setSpacing(12)
        _rb_style = f"QRadioButton {{ color:{PAL['text']}; }} " \
                    f"QRadioButton::indicator {{ width:13px; height:13px; " \
                    f"border:1px solid #2a3a5e; border-radius:7px; background:{PAL['bg']}; }} " \
                    f"QRadioButton::indicator:checked {{ background:{PAL['accent']}; " \
                    f"border-color:{PAL['accent']}; }}"
        self._rb_num  = QRadioButton("Scan number"); self._rb_num.setStyleSheet(_rb_style)
        self._rb_ts   = QRadioButton("Timestamp (YYYYMMDDHHMMSS)"); self._rb_ts.setStyleSheet(_rb_style)
        self._rb_num.setChecked(True)
        mode_hl.addWidget(self._rb_num); mode_hl.addWidget(self._rb_ts); mode_hl.addStretch()
        gl.addWidget(mode_w, 3, 1, 1, 2)

        # ── Scan number row (only visible in number mode) ─────────────────────
        self._scan_num_lbl = ql("Scan number")
        gl.addWidget(self._scan_num_lbl, 4, 0)
        self._scan_num = QSpinBox(); self._scan_num.setRange(1, 99999)
        self._scan_num.setValue(1); self._scan_num.setStyleSheet(SPIN_STYLE)
        gl.addWidget(self._scan_num, 4, 1, 1, 2)

        self._auto_inc = QCheckBox("Auto-increment scan number")
        self._auto_inc.setStyleSheet(CHECK_STYLE); self._auto_inc.setChecked(True)
        gl.addWidget(self._auto_inc, 5, 0, 1, 3)

        # ── Tiled output ──────────────────────────────────────────────────────
        self._tiled_chk = QCheckBox("Save to Tiled server")
        self._tiled_chk.setStyleSheet(CHECK_STYLE)
        # Default: enabled if tiled section says so AND package is available.
        tiled_cfg = self._app_cfg.get("tiled", {})
        self._tiled_chk.setChecked(
            TILED_AVAILABLE and tiled_cfg.get("enabled", False)
        )
        if not TILED_AVAILABLE:
            self._tiled_chk.setEnabled(False)
            self._tiled_chk.setToolTip("tiled package not installed")
        gl.addWidget(self._tiled_chk, 5, 0, 1, 2)

        # "Test" button — quick connection probe without running a scan.
        self._tiled_test_btn = QPushButton("Test")
        self._tiled_test_btn.setStyleSheet(BTN_STYLE)
        self._tiled_test_btn.setFixedWidth(48)
        self._tiled_test_btn.setEnabled(TILED_AVAILABLE)
        self._tiled_test_btn.clicked.connect(self._test_tiled)
        gl.addWidget(self._tiled_test_btn, 5, 2)

        #return grp


        # ── Preview label ─────────────────────────────────────────────────────
        gl.addWidget(ql("Preview"), 6, 0)
        self._fname_preview = QLabel("")
        self._fname_preview.setStyleSheet(
            f"color:{PAL['accent']}; font-family:monospace; font-size:8pt; background:transparent;")
        self._fname_preview.setWordWrap(True)
        gl.addWidget(self._fname_preview, 6, 1, 1, 2)

        # Wire visibility and preview updates
        self._rb_num.toggled.connect(self._on_numbering_mode_changed)
        self._rb_num.toggled.connect(self._update_fname_preview)
        self._rb_ts.toggled.connect(self._update_fname_preview)
        self._prefix_edit.textChanged.connect(self._update_fname_preview)
        self._scan_num.valueChanged.connect(self._update_fname_preview)
        self._fmt_combo.currentIndexChanged.connect(self._update_fname_preview)
        self._auto_inc.stateChanged.connect(self._update_fname_preview)

        self._on_numbering_mode_changed(True)   # set initial visibility
        self._update_fname_preview()
        return grp

     # ── Tiled connection test ─────────────────────────────────────────────────
    def _test_tiled(self):
        ok, msg = self._tiled.check_connection()
        if ok:
            self._log_msg(f"🔵 Tiled: {msg}", PAL["ok"])
        else:
            self._log_msg(f"🔴 Tiled: {msg}", PAL["nc"])

    def _on_numbering_mode_changed(self, _checked=None):
        num_mode = self._rb_num.isChecked()
        self._scan_num_lbl.setVisible(num_mode)
        self._scan_num.setVisible(num_mode)
        self._auto_inc.setVisible(num_mode)
        self._update_fname_preview()

    def _update_fname_preview(self):
        self._fname_preview.setText(Path(self._current_filename()).name)

    

    def _build_scan_group(self):
        from PySide6.QtWidgets import QStackedWidget, QFrame

        def _ql_s(text: str) -> QLabel:
            """Subtext-coloured label for scan group rows."""
            lb = QLabel(text)
            lb.setStyleSheet(f"color:{PAL['subtext']};")
            return lb

        grp = QGroupBox("Scan Parameters"); grp.setStyleSheet(GRP_STYLE)
        vl  = QVBoxLayout(grp); vl.setContentsMargins(8, 20, 8, 8); vl.setSpacing(8)

        # ── Scan type selector ────────────────────────────────────────────────
        row_type = QHBoxLayout()
        row_type.addWidget(_ql_s("Scan type"))
        self._type_combo = QComboBox(); self._type_combo.setStyleSheet(COMBO_STYLE)
        for st in self._scan_instances:
            self._type_combo.addItem(st.LABEL)
        row_type.addWidget(self._type_combo, 1)
        vl.addLayout(row_type)

        # ── Stacked parameter panel (one page per scan type) ──────────────────
        self._scan_stack = QStackedWidget()
        motor_names = list(self._motor_pvs.keys())
        for inst in self._scan_instances:
            w = inst.build_widget(motor_names)
            self._scan_stack.addWidget(w)
        vl.addWidget(self._scan_stack)

        # ── Separator ─────────────────────────────────────────────────────────
        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("color:#2a3a5e;"); vl.addWidget(sep)

        # ── Detector (shared across all scan types) ───────────────────────────
        row_det = QHBoxLayout()
        row_det.addWidget(_ql_s("Detector"))
        self._det_combo = QComboBox(); self._det_combo.setStyleSheet(COMBO_STYLE)
        for n in {**self._signal_pvs, **self._det_pvs}:
            self._det_combo.addItem(n)
        row_det.addWidget(self._det_combo, 1)
        self._det_badge = QLabel("scalar")
        self._det_badge.setStyleSheet(
            f"color:{PAL['ok']}; font-size:8pt; font-style:italic;")
        row_det.addWidget(self._det_badge)
        vl.addLayout(row_det)

        # ── Exposure ──────────────────────────────────────────────────────────
        row_exp = QHBoxLayout()
        row_exp.addWidget(_ql_s("Exposure (s)"))
        self._p_exp = QDoubleSpinBox()
        self._p_exp.setRange(0.001, 3600.0); self._p_exp.setValue(0.5)
        self._p_exp.setDecimals(3); self._p_exp.setStyleSheet(SPIN_STYLE)
        row_exp.addWidget(self._p_exp, 1)
        vl.addLayout(row_exp)

        # ── Connect signals ───────────────────────────────────────────────────
        self._type_combo.currentIndexChanged.connect(self._on_scan_type_changed)
        self._det_combo.currentTextChanged.connect(self._on_det_changed)
        self._on_det_changed(self._det_combo.currentText())   # initialise badge

        return grp

    def _build_run_group(self):
        grp = QGroupBox("Run Control"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8,20,8,8); vl.setSpacing(8)

        btn_row = QHBoxLayout(); btn_row.setSpacing(6)
        self._run_btn   = QPushButton("▶  Run");   self._run_btn.setStyleSheet(BTN_STYLE)
        self._queue_btn = QPushButton("+ Queue")
        self._queue_btn.setStyleSheet(BTN_STYLE)
        self._queue_btn.setToolTip("Add current scan parameters to the queue server")
        self._queue_btn.setEnabled(False)   # enabled once set_queue_tab() is called
        self._queue_btn.clicked.connect(self._add_to_queue)
        btn_row.addWidget(self._queue_btn)
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

    def _current_filename(self):
        d      = self._dir_edit.text().strip() or "."
        prefix = self._prefix_edit.text().strip() or "scan"
        ext    = {0:".h5", 1:".csv", 2:".dat"}.get(self._fmt_combo.currentIndex(), ".dat")
        if self._rb_ts.isChecked():
            suffix = datetime.now().strftime("%Y%m%d%H%M%S")
        else:
            suffix = f"{self._scan_num.value():04d}"
        return f"{d}/{prefix}_{suffix}{ext}"

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

    def _on_scan_type_changed(self, idx: int):
        self._active_scan_idx = idx
        self._scan_stack.setCurrentIndex(idx)

    def _on_det_changed(self, name: str = ""):
        name = name or self._det_combo.currentText()
        kind = detector_kind(name, self._signal_pvs, self._det_pvs)
        self._det_kind = kind
        self._det_badge.setText(_AREA_LABELS[kind])
        self._det_badge.setStyleSheet(
            f"color:{_AREA_COLORS[kind]}; font-size:8pt; font-style:italic;")
        if kind == DET_AREA:
            # Area detectors require HDF5 for image storage
            self._fmt_combo.setCurrentIndex(0)
            self._fmt_combo.setEnabled(False)
            self._log_msg(
                f"ℹ  Area detector selected — format locked to HDF5.",
                PAL["warn"])
        else:
            self._fmt_combo.setEnabled(True)

    def set_queue_tab(self, qt) -> None:
        """Wire in the QueueTab so the DAQ tab can submit plans."""
        self._queue_tab = qt
        self._queue_btn.setEnabled(True)
        self._log_msg("Queue server tab connected.", PAL["subtext"])

    def _add_to_queue(self) -> None:
        """Translate current scan parameters into a bluesky plan and submit."""
        if self._queue_tab is None:
            self._log_msg("⚠ Queue tab not connected.", PAL["nc"])
            return

        scan = self._scan_instances[self._active_scan_idx]
        det  = self._det_combo.currentText()

        # ── Resolve device names ───────────────────────────────────────────────
        det_dev = self._device_map.get(det)
        if det_dev is None:
            self._log_msg(
                f"⚠ No device name found for detector '{det}'. "
                f"Check devices.py / startup script.", PAL["nc"])
            return

        # For 1D and Time scans the "motor" is the inner (or only) motor.
        # For 2D it's the inner motor (plot axis).
        x_lbl, _ = scan.plot_axes()
        # x_lbl is the motor friendly name for 1D/2D; "Time (s)" for TimeScan.
        motor_dev = self._device_map.get(x_lbl, "")

        # ── Build plan tuple ───────────────────────────────────────────────────
        try:
            plan_name, args, kwargs = scan.to_plan(motor_dev, det_dev)
        except Exception as exc:
            self._log_msg(f"⚠ Could not build plan: {exc}", PAL["nc"])
            return

        # ── Resolve the Scan2D outer motor sentinel ───────────────────────────
        # Scan2D encodes the outer motor as "__outer__:<friendly_name>" in args
        # so this method can look it up without coupling scan_types to devices.
        resolved_args = []
        for a in args:
            if isinstance(a, str) and a.startswith("__outer__:"):
                outer_friendly = a[len("__outer__:"):]
                outer_dev = self._device_map.get(outer_friendly)
                if outer_dev is None:
                    self._log_msg(
                        f"⚠ No device name for outer motor '{outer_friendly}'.",
                        PAL["nc"])
                    return
                resolved_args.append(outer_dev)
            else:
                resolved_args.append(a)

        # ── Build metadata ────────────────────────────────────────────────────
        meta = dict(
            scan_type  = scan.LABEL,
            detector   = det,
            det_kind   = self._det_kind,
            scan_label = scan.scan_label(),
            output_dir = self._dir_edit.text().strip(),
            prefix     = self._prefix_edit.text().strip() or "scan",
            scan_num   = self._scan_num.value(),
            exposure   = self._p_exp.value(),
        )

        # ── Submit ────────────────────────────────────────────────────────────
        ok = self._queue_tab.add_plan(plan_name, resolved_args, kwargs, meta)
        if ok:
            self._log_msg(
                f"✔ Added to queue: {scan.scan_label()}", PAL["ok"])
        # On failure, add_plan() already logs the error in the Queue tab.


    # ── Scan engine ───────────────────────────────────────────────────────────
    def _start_scan(self):
        if self._state == _ScanState.RUNNING:
            return

        scan   = self._scan_instances[self._active_scan_idx]
        det    = self._det_combo.currentText()
        kind   = self._det_kind
        fname  = self._current_filename()

        # Build position sequence
        positions = scan.build_positions()
        if not positions:
            self._log_msg("⚠ No scan positions — check parameters.", PAL["nc"])
            return
        self._scan_positions = positions
        self._scan_idx  = 0
        self._scan_t0   = time.monotonic()
        self._acquiring = False
        self._last_outer = -1

        x_lbl, y_lbl = scan.plot_axes()

        # ── Column list: [det, other signals α, other motors α] ───────────────
        other_signals = sorted(k for k in self._signal_pvs if k != det)
        other_motors  = sorted(k for k in self._motor_pvs
                               if k not in {det, scan.outer_motor()})
        self._scan_columns = (
            [det]
            + [k for k in other_signals if k != det]
            + [k for k in other_motors  if k != det]
        )

        # ── Open file ─────────────────────────────────────────────────────────
        first_pos = positions[0]
        # Use x-axis label as the "motor" field in metadata
        meta = dict(
            scan_type = scan.LABEL,
            motor     = x_lbl,
            detector  = det,
            det_kind  = kind,
            start     = first_pos["_x"],
            stop      = positions[-1]["_x"],
            steps     = len(positions),
            exposure  = self._p_exp.value(),
            scan_num  = self._scan_num.value(),
            prefix    = self._prefix_edit.text().strip() or "scan",
        )
        self._writer = ScanFileWriter()
        if not self._writer.open(fname, meta, self._scan_columns):
            self._writer = None
            self._log_msg(f"⚠ Cannot open output file: {fname}", PAL["nc"])
            from PySide6.QtWidgets import QMessageBox
            dlg = QMessageBox(self)
            dlg.setWindowTitle("File Error")
            dlg.setIcon(QMessageBox.Warning)
            dlg.setText("Could not open scan output file.")
            dlg.setInformativeText(
                f"<b>{Path(fname).name}</b><br><br>"
                f"Directory: <code>{Path(fname).parent}</code><br><br>"
                + ("h5py is not installed — run <code>pip install h5py</code> "
                   "or switch to CSV / SPEC format."
                   if fname.endswith(".h5") and not H5_AVAILABLE
                   else "Check that the directory exists and is writable.")
            )
            dlg.setStandardButtons(QMessageBox.Abort | QMessageBox.Ignore)
            dlg.setDefaultButton(QMessageBox.Abort)
            dlg.setStyleSheet(f"background:{PAL['surface']}; color:{PAL['text']};")
            if dlg.exec() == QMessageBox.Abort:
                self._set_state(_ScanState.IDLE)
                self._reset_run_btns()
                return

        # ── UI updates ────────────────────────────────────────────────────────
        n = len(positions)
        self._scan_plot.reset(x_lbl, det, f"{det}  vs  {x_lbl}")
        self._progress.setMaximum(n); self._progress.setValue(0)
        self._st_file.setText(Path(fname).name)
        self._st_file.setStyleSheet(
            f"color:{PAL['text']}; font-family:monospace; font-size:8pt;")
        n_extra = len(self._scan_columns) - 1
        self._log_msg(f"▶ {scan.scan_label()}  →  {fname}", PAL["ok"])
        self._log_msg(f"   Det: {det} [{kind}]  |  +{n_extra} extra channels"
                      + (f"  |  area-det timeout: {SCAN_ACQ_TIMEOUT_S:.0f} s"
                         if kind == DET_AREA else ""))
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

    def _scan_step(self):
        if self._state != _ScanState.RUNNING:
            self._scan_timer.stop(); return

        scan = self._scan_instances[self._active_scan_idx]
        det  = self._det_combo.currentText()

        # ── Poll area-detector completion ─────────────────────────────────────
        if self._acquiring:
            base   = self._det_base.get(det, "")
            rbv_pv = base + ":cam1:Acquire_RBV" if base else ""
            done   = True
            if EPICS_AVAILABLE and rbv_pv:
                raw  = PVMonitor().get(rbv_pv)
                done = (raw is not None and float(raw) == 0)
            if not done and time.monotonic() < self._acq_deadline:
                return          # still acquiring — come back next tick
            # Acquisition complete (or timed out) → read image, record point
            y = self._last_area_scalar
            if not done:
                self._log_msg(
                    f"⚠ Area detector timeout at point {self._scan_idx+1}",
                    PAL["warn"])
            self._acquiring = False
            self._record_point(scan, y, det)
            return

        # ── Advance to next position ──────────────────────────────────────────
        if self._scan_idx >= len(self._scan_positions):
            self._finish_scan(); return

        pos = self._scan_positions[self._scan_idx]

        # Move all real motors in this step (skip "_"-prefixed keys)
        for mname, setpoint in pos.items():
            if mname.startswith("_"): continue
            pv = self._motor_pvs.get(mname, "")
            if pv: PVMonitor().put(pv, setpoint)

        # For 2D: reset plot at each new outer step
        if scan.n_outer() > 1:
            outer_i = scan.outer_index(self._scan_idx)
            if outer_i != self._last_outer:
                x_lbl, _ = scan.plot_axes()
                om = scan.outer_motor()
                ov = pos.get(om, outer_i) if om else outer_i
                om_str = f"{om}={ov:.4g}" if om else f"step {outer_i+1}"
                self._scan_plot.reset(
                    x_lbl, det,
                    f"{det}  vs  {x_lbl}   [{om_str}"
                    f"  {outer_i+1}/{scan.n_outer()}]")
                self._last_outer = outer_i

        # ── Acquire detector value ─────────────────────────────────────────────
        if self._det_kind == DET_AREA:
            # Trigger acquisition; poll in subsequent ticks
            base   = self._det_base.get(det, "")
            acq_pv = base + ":cam1:Acquire" if base else ""
            if EPICS_AVAILABLE and acq_pv:
                PVMonitor().put(acq_pv, 1)
            self._acquiring      = True
            self._acq_deadline   = time.monotonic() + SCAN_ACQ_TIMEOUT_S
            self._last_area_scalar = float("nan")   # placeholder for plot
            return   # don't advance _scan_idx yet; wait for completion

        # Scalar detector: read immediately
        sig_pv = ({**self._signal_pvs, **self._det_pvs}).get(det, "")
        if EPICS_AVAILABLE and sig_pv:
            raw = PVMonitor().get(sig_pv)
            y   = float(raw) if raw is not None else float("nan")
        else:
            y = sim_scalar(self._scan_positions, self._scan_idx)

        self._record_point(scan, y, det)

    def _record_point(self, scan, y: float, det: str):
        """Advance scan index, update plot, write file row."""
        pos = self._scan_positions[self._scan_idx]
        x   = pos["_x"]

        self._scan_plot.add_point(x, y)

        # Read all extra channels
        extras: dict = {det: y}
        all_readable = {**self._signal_pvs, **self._det_pvs, **self._motor_pvs}
        for col in self._scan_columns:
            if col == det: continue
            cpv = all_readable.get(col, "")
            if EPICS_AVAILABLE and cpv:
                raw = PVMonitor().get(cpv)
                extras[col] = float(raw) if raw is not None else float("nan")
            else:
                extras[col] = float("nan")

        if self._writer is not None:
            self._writer.write_point(x, y, extras)

        self._scan_idx += 1
        self._progress.setValue(self._scan_idx)
        elapsed = time.monotonic() - self._scan_t0
        self._st_elapsed.setText(f"{elapsed:.1f} s")
        self._st_point.setText(
            f"{self._scan_idx} / {len(self._scan_positions)}")

        if self._scan_idx >= len(self._scan_positions):
            self._finish_scan()


    def _finish_scan(self):
        self._scan_timer.stop()
        fname = self._current_filename()
        self._scan_plot.finish(f"Scan complete — {Path(fname).name}")
        self._log_msg(f"✔ Scan complete.  File: {fname}", PAL["ok"])

        # ── Tiled write ───────────────────────────────────────────────────────
        if self._tiled_chk.isChecked():
            self._write_tiled()

        if self._auto_inc.isChecked():
            self._scan_num.setValue(self._scan_num.value() + 1)
        self._set_state(_ScanState.IDLE)
        self._reset_run_btns()

    def _write_tiled(self):
        """Assemble a DataFrame from the completed scan and push it to Tiled."""
        if not PANDAS_AVAILABLE:
            self._log_msg("🔴 Tiled: pandas not installed — cannot build table", PAL["nc"])
            return

        scan  = self._scan_instances[self._active_scan_idx]
        x_lbl, _ = scan.plot_axes()
        motor = x_lbl
        det   = self._det_combo.currentText()

        # _scan_positions and the y-values collected in _scan_plot are the
        # canonical source of truth for the completed scan.
        xs = [p["_x"] for p in self._scan_positions[: self._scan_idx]]
        ys = list(self._scan_plot.ys)[: self._scan_idx]   # see note below

        if not xs:
            self._log_msg("🔴 Tiled: no scan data to write", PAL["nc"])
            return

        df = pd.DataFrame({motor: xs, det: ys})

        general = self._app_cfg.get("general", {})
        metadata = {
            "scan_num":   self._scan_num.value(),
            "motor":      motor,
            "detector":   det,
            "start":      xs[0],
            "stop":       xs[-1],
            "n_points":   len(xs),
            "timestamp":  datetime.now().isoformat(),
            "facility":   general.get("facility",   "ALS"),
            "beamline":   general.get("beamline",   "BL601"),
            "endstation": general.get("endstation", "HiRRIXS"),
        }

        ok, msg = self._tiled.write_scan(df, metadata)
        if ok:
            self._log_msg(f"🔵 Tiled: written → {msg}", PAL["ok"])
        else:
            self._log_msg(f"🔴 Tiled: {msg}", PAL["nc"])

    def _abort_scan(self):
        self._scan_timer.stop()
        self._acquiring = False
        self._set_state(_ScanState.ABORTING)
        if self._writer is not None:
            self._writer.close(aborted=True); self._writer = None
        self._log_msg("■ Scan aborted.", PAL["nc"])
        self._scan_plot.finish("Scan aborted")
        self._reset_run_btns()

    def _reset_run_btns(self):
        self._run_btn.setEnabled(True)
        self._pause_btn.setEnabled(False); self._pause_btn.setText("⏸  Pause")
        self._abort_btn.setEnabled(False)
