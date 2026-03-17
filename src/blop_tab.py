"""
blop_tab.py — Tab 4: BLOP (Bayesian Last-mile Optimization Protocol)

Drives the real bluesky/blop package (github.com/bluesky/blop).

Architecture
------------
* The GUI never calls blop directly on the Qt main thread — that would block.
* A QThread runs a worker that calls RE(agent.learn(...)) in a loop.
* After each RE() call the worker queries agent.best and agent.table and
  emits Qt signals back to the main thread for display.
* The Bluesky RunEngine is created once inside the worker thread, which is the
  correct pattern (RE is not thread-safe to share).

Prerequisites (must be importable)
-----------------------------------
    pip install blop bluesky ophyd databroker

Config mapping
--------------
Motors from amber.json / hirrixs.json are offered as DOF candidates.
Each DOF is wrapped in an ophyd EpicsMotor using the PV string from the config.
Signals (analog inputs) are offered as Objective candidates; the digestion
function reads the named column from the Bluesky run table, which is populated
by listing the signal as a detector (wrapped in an ophyd EpicsSignalRO).
"""

import threading
from datetime import datetime

import numpy as np

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QSplitter,
    QGroupBox, QGridLayout, QLineEdit, QPushButton, QComboBox,
    QScrollArea, QTextEdit, QDoubleSpinBox, QSpinBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox,
    QSizePolicy, QTabWidget, QFrame,
)
from PySide6.QtCore import Qt, QTimer, Signal, QObject, QThread
from PySide6.QtGui import QFont, QColor

from common import (
    PAL, COMBO_STYLE, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE,
    MPL_AVAILABLE,
)

if MPL_AVAILABLE:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    from matplotlib.gridspec import GridSpec

# ── Shared styles ─────────────────────────────────────────────────────────────
SPIN_STYLE = f"""
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

# ── Acquisition function catalogue (from blop docs) ───────────────────────────
ACQF_OPTIONS = [
    ("quasi-random (qr)",        "qr"),
    ("Expected Improvement (ei)","ei"),
    ("MC Exp. Improvement (qei)","qei"),
    ("Upper Conf. Bound (ucb)",  "ucb"),
    ("MC Upper Conf. Bound (qucb)","qucb"),
    ("Expected Mean (em)",       "em"),
    ("Probability of Impr. (pi)","pi"),
]

# ── Worker — runs blop on a background thread ─────────────────────────────────
class BLOPWorker(QObject):
    """
    Constructs a blop Agent from the supplied config and runs
    RE(agent.learn(...)) in a loop on a background QThread.

    All bluesky/blop objects live entirely in this thread.
    Results are shipped back to the GUI via Qt signals.
    """

    # Signal payloads
    log_msg      = Signal(str, str)          # text, css-colour
    learn_done   = Signal(object)            # agent.table (pandas DataFrame)
    best_updated = Signal(object)            # agent.best  (pandas Series)
    finished     = Signal()

    def __init__(self,
                 dof_configs:   list,   # [{"name":str,"pv":str,"lo":float,"hi":float}, ...]
                 obj_configs:   list,   # [{"name":str,"pv":str,"target":str}, ...]
                 acqf_init:     str,
                 n_init:        int,
                 acqf_bo:       str,
                 n_per_iter:    int,
                 n_iterations:  int,
                 parent=None):
        super().__init__(parent)
        self._dof_cfgs   = dof_configs
        self._obj_cfgs   = obj_configs
        self._acqf_init  = acqf_init
        self._n_init     = n_init
        self._acqf_bo    = acqf_bo
        self._n_per_iter = n_per_iter
        self._n_iter     = n_iterations
        self._stop_flag  = False

    def stop(self):
        self._stop_flag = True

    # ------------------------------------------------------------------
    def run(self):
        try:
            self._run_blop()
        except Exception as exc:
            self.log_msg.emit(f"✘ Fatal error: {exc}", PAL["nc"])
            import traceback
            self.log_msg.emit(traceback.format_exc(), PAL["nc"])
        finally:
            self.finished.emit()

    def _run_blop(self):
        # -- imports inside thread so Qt GUI is never blocked by torch init --
        try:
            from blop import Agent, DOF, Objective
        except ImportError as e:
            self.log_msg.emit(
                f"✘ Could not import blop: {e}\n"
                "Install with:  pip install blop", PAL["nc"])
            return

        try:
            from bluesky import RunEngine
            from databroker import temp_config
            import databroker
        except ImportError as e:
            self.log_msg.emit(
                f"✘ Could not import bluesky/databroker: {e}", PAL["nc"])
            return

        # -- Build ophyd devices from PV strings --
        try:
            from ophyd import EpicsMotor, EpicsSignalRO
        except ImportError as e:
            self.log_msg.emit(f"✘ Could not import ophyd: {e}", PAL["nc"])
            return

        self.log_msg.emit("Building ophyd devices …", PAL["subtext"])

        # DOFs — EpicsMotor per PV
        dofs = []
        for cfg in self._dof_cfgs:
            if not cfg["pv"]:
                self.log_msg.emit(
                    f"⚠  DOF '{cfg['name']}' has no PV — skipping.", PAL["warn"])
                continue
            try:
                motor = EpicsMotor(cfg["pv"], name=cfg["name"])
                dofs.append(DOF(
                    device=motor,
                    search_domain=(cfg["lo"], cfg["hi"]),
                ))
                self.log_msg.emit(
                    f"  DOF  {cfg['name']:20s}  pv={cfg['pv']}", "")
            except Exception as e:
                self.log_msg.emit(
                    f"⚠  Could not create DOF '{cfg['name']}': {e}", PAL["warn"])

        if not dofs:
            self.log_msg.emit("✘ No valid DOFs — aborting.", PAL["nc"]); return

        # Objectives — EpicsSignalRO per PV, listed as detectors
        # The digestion function reads the column named after the signal.
        objectives   = []
        det_devices  = []
        obj_names    = []
        for cfg in self._obj_cfgs:
            if not cfg["pv"]:
                self.log_msg.emit(
                    f"⚠  Objective '{cfg['name']}' has no PV — skipping.", PAL["warn"])
                continue
            try:
                sig = EpicsSignalRO(cfg["pv"], name=cfg["name"])
                det_devices.append(sig)
                objectives.append(Objective(
                    name=cfg["name"],
                    target=cfg["target"],   # "max" | "min" | float
                ))
                obj_names.append(cfg["name"])
                self.log_msg.emit(
                    f"  OBJ  {cfg['name']:20s}  pv={cfg['pv']}  target={cfg['target']}", "")
            except Exception as e:
                self.log_msg.emit(
                    f"⚠  Could not create Objective '{cfg['name']}': {e}", PAL["warn"])

        if not objectives:
            self.log_msg.emit("✘ No valid Objectives — aborting.", PAL["nc"]); return

        # -- Digestion: read objective columns straight from the run table --
        _obj_names = list(obj_names)   # closure capture

        def digestion(df):
            # df already has columns named after the EpicsSignalRO devices.
            # No transformation needed; blop will find the columns by name.
            return df

        # -- Build RunEngine + databroker (temp in-memory catalog) --
        self.log_msg.emit("Initialising Bluesky RunEngine …", PAL["subtext"])
        db = databroker.from_config(temp_config())
        RE = RunEngine({})
        RE.subscribe(db.insert)

        # -- Build agent --
        self.log_msg.emit("Building blop Agent …", PAL["subtext"])
        agent = Agent(
            dofs=dofs,
            objectives=objectives,
            digestion=digestion,
            detectors=det_devices,
            db=db,
        )

        # -- Phase 1: random/quasi-random initialisation --
        if self._stop_flag: return
        self.log_msg.emit(
            f"▶ Phase 1 — {self._acqf_init}  n={self._n_init}", PAL["ok"])
        try:
            RE(agent.learn(self._acqf_init, n=self._n_init))
        except Exception as e:
            self.log_msg.emit(f"✘ Init learn failed: {e}", PAL["nc"])
            return
        self._emit_results(agent)

        # -- Phase 2: Bayesian optimisation iterations --
        for it in range(self._n_iter):
            if self._stop_flag:
                self.log_msg.emit("■ Stopped by user.", PAL["nc"]); break
            self.log_msg.emit(
                f"▶ BO iter {it+1}/{self._n_iter}"
                f" — {self._acqf_bo}  n={self._n_per_iter}", PAL["accent"])
            try:
                RE(agent.learn(self._acqf_bo, n=self._n_per_iter))
            except Exception as e:
                self.log_msg.emit(f"✘ learn() failed at iter {it+1}: {e}", PAL["nc"])
                break
            self._emit_results(agent)

        # -- Report best --
        try:
            best = agent.best
            self.log_msg.emit(
                "✔ Optimisation complete.  Best point:", PAL["ok"])
            self.log_msg.emit(str(best), PAL["ok"])
            self.best_updated.emit(best)
        except Exception as e:
            self.log_msg.emit(f"⚠  Could not retrieve agent.best: {e}", PAL["warn"])

    def _emit_results(self, agent):
        """Emit the full observation table and the current best."""
        try:
            self.learn_done.emit(agent.table)
        except Exception:
            pass
        try:
            self.best_updated.emit(agent.best)
        except Exception:
            pass


# ── Convergence plot ──────────────────────────────────────────────────────────
class ConvergencePlot(QWidget):
    """Best objective value vs total number of observations."""

    def __init__(self, obj_name="objective", parent=None):
        super().__init__(parent)
        self._obj_name = obj_name
        vl = QVBoxLayout(self); vl.setContentsMargins(0, 0, 0, 0)
        if MPL_AVAILABLE:
            self._fig  = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax   = self._fig.add_subplot(111)
            self._line, = self._ax.plot([], [], color=PAL["ok"], lw=1.5, marker=".")
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            vl.addWidget(self._canvas)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph)
        self._ns:    list = []
        self._bests: list = []

    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.grid(True, color="#2a3a5e", lw=0.4, ls="--")
        ax.set_xlabel("# observations",   color=PAL["subtext"], fontsize=8)
        ax.set_ylabel("Best so far",       color=PAL["subtext"], fontsize=8)
        ax.set_title("Convergence",        color=PAL["text"],    fontsize=9)

    def update_from_table(self, df, obj_name: str):
        """Receive agent.table (DataFrame), recompute running best."""
        if not MPL_AVAILABLE or df is None: return
        if obj_name not in df.columns: return
        vals = df[obj_name].dropna().values
        if len(vals) == 0: return
        # running maximum (blop maximises internally)
        running_best = np.maximum.accumulate(vals)
        self._ns    = list(range(1, len(running_best) + 1))
        self._bests = running_best.tolist()
        self._line.set_data(self._ns, self._bests)
        self._ax.set_title(f"Convergence — {obj_name}", color=PAL["text"], fontsize=9)
        self._ax.relim(); self._ax.autoscale_view(); self._canvas.draw_idle()

    def reset(self):
        self._ns.clear(); self._bests.clear()
        if MPL_AVAILABLE:
            self._line.set_data([], []); self._ax.relim(); self._canvas.draw_idle()


# ── Observations table ────────────────────────────────────────────────────────
class ObsTable(QTableWidget):
    """Displays agent.table rows — DOFs + Objectives."""

    def __init__(self, parent=None):
        super().__init__(0, 0, parent)
        self.setStyleSheet(TABLE_STYLE)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectRows)

    def load_dataframe(self, df):
        if df is None or df.empty: return
        # choose columns: dof and objective columns (skip 'time', 'acqf', uid, etc.)
        skip = {"time", "acqf", "uid", "seq_num"}
        cols = [c for c in df.columns if c not in skip and not c.startswith("_")]
        self.setColumnCount(len(cols))
        self.setHorizontalHeaderLabels(cols)
        self.setRowCount(len(df))
        for r, (_, row) in enumerate(df[cols].iterrows()):
            for c, col in enumerate(cols):
                val = row[col]
                try:    txt = f"{float(val):.6g}"
                except: txt = str(val)
                item = QTableWidgetItem(txt)
                item.setForeground(QColor(PAL["text"]))
                self.setItem(r, c, item)
        self.scrollToBottom()


# ── BLOP Tab ──────────────────────────────────────────────────────────────────
class BLOPTab(QWidget):
    def __init__(self, amber_cfg: dict, hirrixs_cfg: dict, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"background:{PAL['bg']};")
        self._worker: BLOPWorker | None = None
        self._thread: QThread | None = None
        self._running = False

        # Collect PV maps from configs
        self._motor_pvs  = self._collect_pvs(amber_cfg, "motor")
        self._motor_pvs.update(self._collect_pvs(hirrixs_cfg, "motor"))
        self._signal_pvs: dict = {}
        self._signal_pvs.update(amber_cfg.get("signal", {}))
        self._signal_pvs.update(hirrixs_cfg.get("signal", {}))

        # DOF row list: each entry is {"name", "pv", "lo_spin", "hi_spin", "rm_btn"}
        self._dof_rows: list = []
        # Objective row list: each entry is {"name", "pv", "target_combo", "rm_btn"}
        self._obj_rows: list = []

        outer = QVBoxLayout(self); outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        hdr = QLabel("  🤖  BLOP — Bayesian Last-mile Optimisation  "
                     "[bluesky/blop]")
        hdr.setFont(QFont("Sans Serif", 9, QFont.Bold))
        hdr.setStyleSheet(
            f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        outer.addWidget(hdr)

        # ── Main splitter: config (left) | results (right) ─────────────────
        main_split = QSplitter(Qt.Horizontal)
        main_split.setStyleSheet(SPLITTER_STYLE)

        # ── Left: scrollable config panel ─────────────────────────────────
        left = QWidget(); left.setStyleSheet(f"background:{PAL['bg']};")
        lv = QVBoxLayout(left)
        lv.setContentsMargins(8, 8, 8, 8); lv.setSpacing(10)
        lv.addWidget(self._build_dof_group())
        lv.addWidget(self._build_obj_group())
        lv.addWidget(self._build_algo_group())
        lv.addWidget(self._build_run_group())
        lv.addStretch()
        left.setMinimumWidth(340); left.setMaximumWidth(500)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setStyleSheet(f"background:{PAL['bg']}; border:none;")
        scroll.setWidget(left)
        main_split.addWidget(scroll)

        # ── Right: results panel ───────────────────────────────────────────
        right = QWidget(); right.setStyleSheet(f"background:{PAL['bg']};")
        rv = QVBoxLayout(right)
        rv.setContentsMargins(0, 0, 0, 0); rv.setSpacing(0)

        # Plot tabs
        plot_tabs = QTabWidget()
        plot_tabs.setStyleSheet("""
            QTabWidget::pane   { border:none; background:#1a1a2e; }
            QTabBar::tab       { background:#16213e; color:#9e9e9e;
                                 padding:6px 16px; border:none;
                                 border-bottom:2px solid transparent; }
            QTabBar::tab:selected { color:#4fc3f7;
                                    border-bottom:2px solid #4fc3f7; }
        """)
        self._conv_plot = ConvergencePlot()
        plot_tabs.addTab(self._conv_plot, "Convergence")
        rv.addWidget(plot_tabs, 2)

        # Best-point display
        best_grp = QGroupBox("Current Best"); best_grp.setStyleSheet(GRP_STYLE)
        bg_v = QVBoxLayout(best_grp); bg_v.setContentsMargins(8, 18, 8, 8)
        self._best_lbl = QLabel("—")
        self._best_lbl.setFont(QFont("Monospace", 8))
        self._best_lbl.setStyleSheet(f"color:{PAL['ok']}; background:transparent;")
        self._best_lbl.setWordWrap(True)
        bg_v.addWidget(self._best_lbl)
        rv.addWidget(best_grp)

        # Observations table
        obs_grp = QGroupBox("Observations"); obs_grp.setStyleSheet(GRP_STYLE)
        og_v = QVBoxLayout(obs_grp); og_v.setContentsMargins(6, 18, 6, 6)
        self._obs_table = ObsTable()
        self._obs_table.setMinimumHeight(160)
        og_v.addWidget(self._obs_table)
        rv.addWidget(obs_grp, 1)

        # Log
        log_grp = QGroupBox("Log"); log_grp.setStyleSheet(GRP_STYLE)
        lg_v = QVBoxLayout(log_grp); lg_v.setContentsMargins(6, 18, 6, 6)
        self._log = QTextEdit(); self._log.setReadOnly(True)
        self._log.setStyleSheet(LOG_STYLE); self._log.setMaximumHeight(160)
        lg_v.addWidget(self._log)
        clr = QPushButton("Clear"); clr.setStyleSheet(BTN_STYLE)
        clr.setFixedWidth(60); clr.clicked.connect(self._log.clear)
        lg_v.addWidget(clr, alignment=Qt.AlignRight)
        rv.addWidget(log_grp)

        main_split.addWidget(right)
        main_split.setSizes([400, 1200])
        outer.addWidget(main_split, 1)

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

    # ── DOF group ─────────────────────────────────────────────────────────────
    def _build_dof_group(self):
        grp = QGroupBox("Degrees of Freedom (DOFs)")
        grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8, 20, 8, 8); vl.setSpacing(6)

        self._dof_container = QWidget()
        self._dof_container.setStyleSheet("background:transparent;")
        self._dof_vl = QVBoxLayout(self._dof_container)
        self._dof_vl.setContentsMargins(0, 0, 0, 0); self._dof_vl.setSpacing(4)
        vl.addWidget(self._dof_container)

        add_row = QHBoxLayout(); add_row.setSpacing(6)
        self._dof_motor_combo = QComboBox()
        self._dof_motor_combo.setStyleSheet(COMBO_STYLE)
        for n in self._motor_pvs: self._dof_motor_combo.addItem(n)
        add_row.addWidget(self._dof_motor_combo, 1)
        add_dof_btn = QPushButton("＋ Add DOF"); add_dof_btn.setStyleSheet(BTN_STYLE)
        add_dof_btn.clicked.connect(self._add_dof_row)
        add_row.addWidget(add_dof_btn)
        vl.addLayout(add_row)

        note = QLabel("DOFs are ophyd EpicsMotor devices driven by blop.")
        note.setStyleSheet(f"color:{PAL['subtext']}; font-size:7pt;")
        note.setWordWrap(True); vl.addWidget(note)
        return grp

    def _add_dof_row(self, name=None, pv=None, lo=-5.0, hi=5.0):
        name = name or self._dof_motor_combo.currentText()
        pv   = pv   or self._motor_pvs.get(name, "")

        frame = QFrame(); frame.setStyleSheet(
            f"background:{PAL['surface']}; border:1px solid #2a3a5e; border-radius:4px;")
        fl = QHBoxLayout(frame); fl.setContentsMargins(6, 4, 6, 4); fl.setSpacing(6)

        nl = QLabel(name); nl.setFont(QFont("Sans Serif", 8, QFont.Bold))
        nl.setStyleSheet(f"color:{PAL['text']}; background:transparent; border:none;")
        nl.setMinimumWidth(110); fl.addWidget(nl)

        def ql(t):
            lb = QLabel(t)
            lb.setStyleSheet(f"color:{PAL['subtext']}; background:transparent; border:none;")
            return lb

        fl.addWidget(ql("min"))
        lo_sp = QDoubleSpinBox(); lo_sp.setRange(-1e6, 1e6)
        lo_sp.setValue(lo); lo_sp.setDecimals(4); lo_sp.setStyleSheet(SPIN_STYLE)
        lo_sp.setFixedWidth(90); fl.addWidget(lo_sp)

        fl.addWidget(ql("max"))
        hi_sp = QDoubleSpinBox(); hi_sp.setRange(-1e6, 1e6)
        hi_sp.setValue(hi); hi_sp.setDecimals(4); hi_sp.setStyleSheet(SPIN_STYLE)
        hi_sp.setFixedWidth(90); fl.addWidget(hi_sp)

        rm = QPushButton("✕"); rm.setFixedSize(20, 20)
        rm.setStyleSheet(f"QPushButton{{background:transparent;color:{PAL['nc']};"
                         f"border:none;font-size:11px;padding:0;}}"
                         f"QPushButton:hover{{color:#ff6666;}}")
        entry = {"name": name, "pv": pv, "lo_spin": lo_sp,
                 "hi_spin": hi_sp, "frame": frame}
        rm.clicked.connect(lambda: self._remove_row(entry, self._dof_rows,
                                                    self._dof_vl))
        fl.addWidget(rm)
        self._dof_vl.addWidget(frame)
        self._dof_rows.append(entry)

    def _remove_row(self, entry, row_list, layout):
        if entry in row_list:
            row_list.remove(entry)
        w = entry.get("frame")
        if w:
            layout.removeWidget(w); w.deleteLater()

    # ── Objective group ───────────────────────────────────────────────────────
    def _build_obj_group(self):
        grp = QGroupBox("Objectives"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8, 20, 8, 8); vl.setSpacing(6)

        self._obj_container = QWidget()
        self._obj_container.setStyleSheet("background:transparent;")
        self._obj_vl = QVBoxLayout(self._obj_container)
        self._obj_vl.setContentsMargins(0, 0, 0, 0); self._obj_vl.setSpacing(4)
        vl.addWidget(self._obj_container)

        add_row = QHBoxLayout(); add_row.setSpacing(6)
        self._obj_sig_combo = QComboBox()
        self._obj_sig_combo.setStyleSheet(COMBO_STYLE)
        for n in self._signal_pvs: self._obj_sig_combo.addItem(n)
        add_row.addWidget(self._obj_sig_combo, 1)
        add_obj_btn = QPushButton("＋ Add Obj."); add_obj_btn.setStyleSheet(BTN_STYLE)
        add_obj_btn.clicked.connect(self._add_obj_row)
        add_row.addWidget(add_obj_btn)
        vl.addLayout(add_row)

        note = QLabel("Objectives are EpicsSignalRO devices read as detectors.")
        note.setStyleSheet(f"color:{PAL['subtext']}; font-size:7pt;")
        note.setWordWrap(True); vl.addWidget(note)
        return grp

    def _add_obj_row(self, name=None, pv=None, target="max"):
        name = name or self._obj_sig_combo.currentText()
        pv   = pv   or self._signal_pvs.get(name, "")

        frame = QFrame(); frame.setStyleSheet(
            f"background:{PAL['surface']}; border:1px solid #2a3a5e; border-radius:4px;")
        fl = QHBoxLayout(frame); fl.setContentsMargins(6, 4, 6, 4); fl.setSpacing(6)

        nl = QLabel(name); nl.setFont(QFont("Sans Serif", 8, QFont.Bold))
        nl.setStyleSheet(f"color:{PAL['text']}; background:transparent; border:none;")
        nl.setMinimumWidth(130); fl.addWidget(nl)

        def ql(t):
            lb = QLabel(t)
            lb.setStyleSheet(f"color:{PAL['subtext']}; background:transparent; border:none;")
            return lb

        fl.addWidget(ql("Target"))
        tgt = QComboBox(); tgt.setStyleSheet(COMBO_STYLE)
        for opt in ("max", "min"): tgt.addItem(opt)
        tgt.setCurrentText(target); tgt.setFixedWidth(70); fl.addWidget(tgt)

        rm = QPushButton("✕"); rm.setFixedSize(20, 20)
        rm.setStyleSheet(f"QPushButton{{background:transparent;color:{PAL['nc']};"
                         f"border:none;font-size:11px;padding:0;}}"
                         f"QPushButton:hover{{color:#ff6666;}}")
        entry = {"name": name, "pv": pv, "target_combo": tgt, "frame": frame}
        rm.clicked.connect(lambda: self._remove_row(entry, self._obj_rows,
                                                    self._obj_vl))
        fl.addWidget(rm)
        self._obj_vl.addWidget(frame)
        self._obj_rows.append(entry)

    # ── Algorithm group ───────────────────────────────────────────────────────
    def _build_algo_group(self):
        grp = QGroupBox("Algorithm"); grp.setStyleSheet(GRP_STYLE)
        gl = QGridLayout(grp); gl.setContentsMargins(8, 20, 8, 8); gl.setSpacing(8)

        def ql(t):
            lb = QLabel(t); lb.setStyleSheet(f"color:{PAL['subtext']};"); return lb

        # Init phase
        gl.addWidget(ql("Init acqf"), 0, 0)
        self._acqf_init_combo = QComboBox(); self._acqf_init_combo.setStyleSheet(COMBO_STYLE)
        for label, _ in ACQF_OPTIONS: self._acqf_init_combo.addItem(label)
        self._acqf_init_combo.setCurrentIndex(0)   # quasi-random
        gl.addWidget(self._acqf_init_combo, 0, 1)

        gl.addWidget(ql("Init points (n)"), 1, 0)
        self._n_init = QSpinBox(); self._n_init.setRange(1, 500)
        self._n_init.setValue(8); self._n_init.setStyleSheet(SPIN_STYLE)
        gl.addWidget(self._n_init, 1, 1)

        # BO phase
        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("background:#2a3a5e; border:none;")
        gl.addWidget(sep, 2, 0, 1, 2)

        gl.addWidget(ql("BO acqf"), 3, 0)
        self._acqf_bo_combo = QComboBox(); self._acqf_bo_combo.setStyleSheet(COMBO_STYLE)
        for label, _ in ACQF_OPTIONS: self._acqf_bo_combo.addItem(label)
        self._acqf_bo_combo.setCurrentText("MC Exp. Improvement (qei)")
        gl.addWidget(self._acqf_bo_combo, 3, 1)

        gl.addWidget(ql("Points per iter"), 4, 0)
        self._n_per_iter = QSpinBox(); self._n_per_iter.setRange(1, 100)
        self._n_per_iter.setValue(1); self._n_per_iter.setStyleSheet(SPIN_STYLE)
        gl.addWidget(self._n_per_iter, 4, 1)

        gl.addWidget(ql("BO iterations"), 5, 0)
        self._n_iter = QSpinBox(); self._n_iter.setRange(1, 1000)
        self._n_iter.setValue(20); self._n_iter.setStyleSheet(SPIN_STYLE)
        gl.addWidget(self._n_iter, 5, 1)

        return grp

    # ── Run control group ─────────────────────────────────────────────────────
    def _build_run_group(self):
        grp = QGroupBox("Run Control"); grp.setStyleSheet(GRP_STYLE)
        vl = QVBoxLayout(grp); vl.setContentsMargins(8, 20, 8, 8); vl.setSpacing(8)
        row = QHBoxLayout(); row.setSpacing(6)
        self._run_btn  = QPushButton("▶  Optimise"); self._run_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn = QPushButton("■  Stop");     self._stop_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn.setEnabled(False)
        self._run_btn.clicked.connect(self._start_blop)
        self._stop_btn.clicked.connect(self._stop_blop)
        row.addWidget(self._run_btn); row.addWidget(self._stop_btn)
        vl.addLayout(row)
        return grp

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _log_msg(self, text: str, color: str = ""):
        ts = datetime.now().strftime("%H:%M:%S")
        col = color or PAL["text"]
        self._log.append(
            f'<span style="color:{PAL["subtext"]}">[{ts}]</span> '
            f'<span style="color:{col}">{text}</span>')

    def _acqf_key(self, combo: QComboBox) -> str:
        idx = combo.currentIndex()
        return ACQF_OPTIONS[idx][1]

    # ── Start/stop ────────────────────────────────────────────────────────────
    def _start_blop(self):
        if self._running: return

        if not self._dof_rows:
            self._log_msg("⚠  Add at least one DOF before running.", PAL["warn"]); return
        if not self._obj_rows:
            self._log_msg("⚠  Add at least one Objective before running.", PAL["warn"]); return

        # Validate bounds
        dof_cfgs = []
        for r in self._dof_rows:
            lo, hi = r["lo_spin"].value(), r["hi_spin"].value()
            if lo >= hi:
                self._log_msg(
                    f"⚠  DOF '{r['name']}': min must be < max.", PAL["warn"]); return
            dof_cfgs.append({"name": r["name"], "pv": r["pv"], "lo": lo, "hi": hi})

        obj_cfgs = []
        for r in self._obj_rows:
            obj_cfgs.append({
                "name":   r["name"],
                "pv":     r["pv"],
                "target": r["target_combo"].currentText(),
            })

        # Reset displays
        self._obs_table.setRowCount(0)
        self._conv_plot.reset()
        self._best_lbl.setText("—")

        # Pick primary objective name for convergence plot
        primary_obj = obj_cfgs[0]["name"] if obj_cfgs else ""
        self._conv_plot._obj_name = primary_obj

        self._worker = BLOPWorker(
            dof_configs   = dof_cfgs,
            obj_configs   = obj_cfgs,
            acqf_init     = self._acqf_key(self._acqf_init_combo),
            n_init        = self._n_init.value(),
            acqf_bo       = self._acqf_key(self._acqf_bo_combo),
            n_per_iter    = self._n_per_iter.value(),
            n_iterations  = self._n_iter.value(),
        )
        self._thread = QThread()
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.log_msg.connect(self._log_msg)
        self._worker.learn_done.connect(self._on_learn_done)
        self._worker.best_updated.connect(self._on_best_updated)
        self._worker.finished.connect(self._on_finished)
        self._running = True
        self._run_btn.setEnabled(False); self._stop_btn.setEnabled(True)
        self._log_msg("Starting BLOP optimisation …", PAL["ok"])
        self._thread.start()

    def _stop_blop(self):
        if self._worker:
            self._worker.stop()

    def _on_learn_done(self, df):
        """Called after each agent.learn() completes."""
        self._obs_table.load_dataframe(df)
        self._conv_plot.update_from_table(df, self._conv_plot._obj_name)

    def _on_best_updated(self, best):
        """Called with agent.best (pandas Series)."""
        try:
            lines = []
            for k, v in best.items():
                try:    lines.append(f"  {k}: {float(v):.6g}")
                except: lines.append(f"  {k}: {v}")
            self._best_lbl.setText("\n".join(lines))
        except Exception as e:
            self._best_lbl.setText(str(best))

    def _on_finished(self):
        self._running = False
        if self._thread:
            self._thread.quit(); self._thread.wait(); self._thread = None
        self._run_btn.setEnabled(True); self._stop_btn.setEnabled(False)
        self._log_msg("BLOP thread exited.", PAL["subtext"])
