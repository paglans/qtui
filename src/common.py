"""
common.py — shared constants, styles, helpers and the PVMonitor / PVLabel
that are used by both tabs.
"""
import json, re, random, threading, time
from functools import partial
from pathlib import Path

from PySide6.QtWidgets import (
    QLabel, QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QComboBox, QPushButton, QGroupBox,
)
from PySide6.QtCore import Qt, QTimer, Signal, QObject
from PySide6.QtGui import QColor, QFont

# ── Optional: pyepics ─────────────────────────────────────────────────────────
try:
    import epics
    epics.ca.initialize_libca()   # force CA context onto the main thread now
    EPICS_AVAILABLE = True
except ImportError:
    EPICS_AVAILABLE = False
    print("[INFO] pyepics not found – running in simulation mode")
except Exception as e:
    EPICS_AVAILABLE = False
    print(f"[WARN] pyepics found but CA init failed: {e} – running in simulation mode")

# ── Optional: qtepics ─────────────────────────────────────────────────────────
try:
    import qtepics          # noqa: F401
    QTEPICS_AVAILABLE = True
except ImportError:
    QTEPICS_AVAILABLE = False

# ── Optional: matplotlib ──────────────────────────────────────────────────────
try:
    import matplotlib
    matplotlib.use("QtAgg")
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False
    print("[INFO] matplotlib not found – plots unavailable")

# ── Optional: PySide6-WebEngine ───────────────────────────────────────────────
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings
    WEBENGINE_AVAILABLE = True
except ImportError:
    WEBENGINE_AVAILABLE = False
    print("[INFO] PySide6-WebEngine not found – ALS beam status frame unavailable")

PYVISTA_AVAILABLE = False   # disabled: GLX conflict with QtWebEngine on X11

# ── Optional: p4p (EPICS PV Access) ──────────────────────────────────────────
try:
    from p4p.client.thread import Context as PVAContext
    PVA_AVAILABLE = True
except ImportError:
    PVA_AVAILABLE = False
    print("[INFO] p4p not found – PVA image streams unavailable")

# ── Palette ───────────────────────────────────────────────────────────────────
PAL = {
    "bg":       "#1a1a2e", "surface":  "#16213e",
    "accent":   "#4fc3f7", "ok":       "#69f0ae",
    "nc":       "#ef5350", "warn":     "#ffca28",
    "text":     "#e0e0e0", "subtext":  "#9e9e9e",
    "beam":     "#4fc3f7", "undulat":  "#81d4fa",
    "mirror":   "#80cbc4", "diag":     "#ef9a9a",
    "aperture": "#a5d6a7", "shutter":  "#ffab91",
    "mono":     "#ce93d8", "hover":    "#1e3a5e",
}
def qc(k): return QColor(PAL[k])

COMBO_STYLE = f"""
    QComboBox {{ background:{PAL['surface']}; color:{PAL['text']};
                 border:1px solid #2a3a5e; border-radius:4px;
                 padding:4px 8px; min-width:140px; }}
    QComboBox::drop-down {{ border:none; }}
    QComboBox QAbstractItemView {{ background:{PAL['surface']}; color:{PAL['text']};
                                   selection-background-color:#2a3a5e; }}
"""
BTN_STYLE = f"""
    QPushButton {{ background:{PAL['surface']}; color:{PAL['accent']};
                   border:1px solid {PAL['accent']}; border-radius:4px;
                   padding:4px 12px; }}
    QPushButton:hover   {{ background:#2a3a5e; }}
    QPushButton:pressed {{ background:#1a2a4e; }}
    QPushButton:disabled {{ color:#555; border-color:#333; }}
"""
GRP_STYLE = f"""
    QGroupBox {{ color:{PAL['accent']}; border:1px solid #2a3a5e; border-radius:6px;
                 margin-top:14px; font-weight:bold; font-size:9pt;
                 background:{PAL['surface']}; }}
    QGroupBox::title {{ subcontrol-origin:margin; left:10px; padding:0 4px; }}
    QLabel {{ background:transparent; border:none; }}
"""
INPUT_STYLE = f"""
    QLineEdit {{ background:{PAL['bg']}; color:{PAL['text']};
                 border:1px solid #2a3a5e; border-radius:4px;
                 padding:4px 6px; font-family:monospace; }}
    QLineEdit:focus {{ border-color:{PAL['accent']}; }}
"""
SPLITTER_STYLE = f"""
    QSplitter::handle {{ background:#2a3a5e; }}
    QSplitter::handle:horizontal {{ width:4px; }}
    QSplitter::handle:vertical   {{ height:4px; }}
    QSplitter::handle:hover {{ background:{PAL['accent']}; }}
"""
TRACE_COLORS = [
    "#4fc3f7","#69f0ae","#ffca28","#ef9a9a",
    "#ce93d8","#80cbc4","#ffab91","#81d4fa",
    "#a5d6a7","#f48fb1","#b39ddb","#fff176",
]

# ── JSON loader ───────────────────────────────────────────────────────────────
def load_json(path):
    try:
        text = path.read_text()
        text = re.sub(r",\s*([}\]])", r"\1", text)
        return json.loads(text)
    except FileNotFoundError:
        print(f"[WARN] Config not found: {path}"); return {}
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON: {path}: {e}"); return {}

# ── PV Monitor ────────────────────────────────────────────────────────────────
class _PVBridge(QObject):
    value_changed = Signal(str, object)

class PVMonitor:
    _inst = None
    def __new__(cls):
        if cls._inst is None:
            o = object.__new__(cls)
            o._bridge = _PVBridge()
            o._pvs: dict = {}
            o._sim: dict = {}
            o._poll_timer = None   # created lazily
            if not EPICS_AVAILABLE:
                o._timer = QTimer()
                o._timer.timeout.connect(o._tick)
                o._timer.start(1500)
            cls._inst = o
        return cls._inst

    def _ensure_poll_timer(self):
        if EPICS_AVAILABLE and self._poll_timer is None:
            self._poll_timer = QTimer()
            self._poll_timer.timeout.connect(self._poll)
            self._poll_timer.start(50)

    def _poll(self):
        try:
            epics.ca.poll()
            # retry any PVs that failed to connect
            for name, pv in list(self._pvs.items()):
                if pv is not None and not pv.connected:
                    epics.ca.poll(evt=0.01)
        except Exception:
            pass

    @property
    def value_changed(self): return self._bridge.value_changed

    def subscribe(self, name):
        self._ensure_poll_timer()
        if not name or not isinstance(name, str) or name in self._pvs: return
        if EPICS_AVAILABLE:
            try:
                self._pvs[name] = epics.PV(
                    name, callback=partial(self._cb, name),
                    auto_monitor=True,
                    connection_callback=partial(self._ccb, name),
                    connection_timeout=0.001)
            except Exception as e:
                print(f"[WARN] PV '{name}': {e}")
                self._pvs[name] = None
                self._bridge.value_changed.emit(name, None)
        else:
            self._sim[name] = round(random.uniform(0.5, 20.0), 4)
            self._pvs[name] = None
    def _cb(self, name, value=None, **_):
        self._bridge.value_changed.emit(name, value)
    def _ccb(self, name, conn=False, **_):
        if not conn: self._bridge.value_changed.emit(name, None)
    def _tick(self):
        for n, v in list(self._sim.items()):
            self._sim[n] = round(v + random.uniform(-0.05, 0.05), 4)
            self._bridge.value_changed.emit(n, self._sim[n])
    def get(self, name):
        if EPICS_AVAILABLE:
            p = self._pvs.get(name); return p.value if p else None
        return self._sim.get(name)
    def put(self, name, value):
        if EPICS_AVAILABLE:
            p = self._pvs.get(name)
            if p: p.put(value)
        else:
            self._sim[name] = float(value)
            self._bridge.value_changed.emit(name, float(value))

# ── PV Label ──────────────────────────────────────────────────────────────────
class PVLabel(QLabel):
    def __init__(self, pv_name, fmt="{:.5g}", units="", parent=None):
        super().__init__("…" if pv_name else "N/A", parent)
        self._pv, self._fmt, self._units = pv_name, fmt, units
        self._retries = 0
        self.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.setMinimumWidth(72)
        self.setStyleSheet("color:#9e9e9e; font-family:monospace;")
        if not pv_name: return
        mon = PVMonitor()
        mon.subscribe(pv_name)
        mon.value_changed.connect(self._on_value)
        self._nc_timer = QTimer(self)
        self._nc_timer.setSingleShot(True)
        self._nc_timer.timeout.connect(self._on_timeout)
        self._nc_timer.start(3000)

    def _on_timeout(self):
        if self.text() not in ("…", "N/A"):
            return                          # already got a value, nothing to do
        if self._retries < 5:
            # re-subscribe and try again
            self._retries += 1
            mon = PVMonitor()
            mon._pvs.pop(self._pv, None)    # remove stale entry so subscribe() runs again
            mon.subscribe(self._pv)
            self._nc_timer.start(3000)      # wait another 3 s
        else:
            self._show(None)               # give up after 5 retries (~15 s total)

    def _on_value(self, name, value):
        if name == self._pv:
            if hasattr(self, "_nc_timer"): self._nc_timer.stop()
            self._retries = 0
            self._show(value)

    def _show(self, value):
        if value is None:
            self.setText("N/C"); self.setStyleSheet("color:#ef5350; font-family:monospace;")
        else:
            try:
                s = self._fmt.format(float(value))
                if self._units: s += f" {self._units}"
                self.setText(s); self.setStyleSheet("color:#69f0ae; font-family:monospace;")
            except Exception:
                self.setText(str(value)[:14])
                self.setStyleSheet("color:#ffca28; font-family:monospace;")
                
# ── Table helper ──────────────────────────────────────────────────────────────
from PySide6.QtWidgets import QGridLayout, QGroupBox
from PySide6.QtGui import QFont

def fill_table(grp: QGroupBox, rows):
    grid = QGridLayout(grp); grid.setSpacing(5); grid.setContentsMargins(8,18,8,8)
    for col, hdr in enumerate(["Name","PV","Value"]):
        lbl = QLabel(hdr); lbl.setFont(QFont("Sans Serif",8,QFont.Bold))
        lbl.setStyleSheet(f"color:{PAL['accent']}; background:transparent; border:none;")
        grid.addWidget(lbl, 0, col)
    for r, (name, pv) in enumerate(rows, 1):
        nl = QLabel(name); nl.setFont(QFont("Sans Serif",8))
        nl.setStyleSheet(f"color:{PAL['text']}; background:transparent; border:none;")
        grid.addWidget(nl, r, 0)
        pl = QLabel(pv); pl.setFont(QFont("Monospace",7))
        pl.setStyleSheet(f"color:{PAL['subtext']}; background:transparent; border:none;")
        pl.setMaximumWidth(190); grid.addWidget(pl, r, 1)
        vl = PVLabel(pv); vl.setFont(QFont("Monospace",8)); grid.addWidget(vl, r, 2)
    grid.setColumnStretch(0,1); grid.setColumnStretch(1,2); grid.setColumnStretch(2,1)


# ── PVA image bridge ──────────────────────────────────────────────────────────
class _PVABridge(QObject):
    """Runs a p4p monitor in a daemon thread; emits new_frame(ndarray) on the Qt thread."""
    new_frame = Signal(object)
    MIN_INTERVAL_MS = 200   # cap at ~5 Hz to avoid saturating the Qt event queue

    def __init__(self, parent=None):
        super().__init__(parent)
        self._ctx  : "PVAContext | None" = None
        self._sub  = None
        self._last_emit = 0.0

    def subscribe(self, pv_name: str):
        self._cancel()
        if not PVA_AVAILABLE or not pv_name:
            return
        self._pending = pv_name
        threading.Thread(target=self._connect, args=(pv_name,), daemon=True).start()

    def _connect(self, pv_name: str):
        try:
            if self._ctx is None:
                self._ctx = PVAContext("pva", nt=False)
            if getattr(self, "_pending", None) != pv_name:
                return
            self._sub = self._ctx.monitor(pv_name, self._cb, notify_disconnect=True)
        except Exception as e:
            print(f"[PVABridge] subscribe error: {e}")

    def _cb(self, value):
        if isinstance(value, Exception):
            return
        now = time.monotonic()
        if (now - self._last_emit) * 1000 < self.MIN_INTERVAL_MS:
            return
        try:
            import numpy as np
            raw  = np.asarray(value.value, dtype=float)
            dims = value.dimension
            if len(dims) < 2: return
            ny = int(dims[0].size); nx = int(dims[1].size)
            if nx * ny == 0: return
            img = raw[: ny * nx].reshape(ny, nx)
            self._last_emit = now
            self.new_frame.emit(img)
        except Exception as e:
            print(f"[PVABridge] frame error: {e}")

    def _cancel(self):
        if self._sub is not None:
            try: self._sub.close()
            except: pass
            self._sub = None

    def close(self):
        self._cancel()
        if self._ctx is not None:
            try: self._ctx.close()
            except: pass
            self._ctx = None


# ── Detector Image Viewer ─────────────────────────────────────────────────────
class DetectorImageViewer(QWidget):
    """
    Live area-detector image display backed by EPICS PV Access (p4p).

    Parameters
    ----------
    detector_pvs : dict
        Mapping of friendly name → PV prefix.  The viewer appends
        ``:RawImg`` for the image stream and ``:cam1:Acquire[_RBV]``
        for the acquire toggle.
    parent : QWidget, optional
    """
    _CMAPS   = ["viridis", "inferno", "gray", "plasma", "hot"]
    _IMG_SUF = ":RawImg"
    _ACQ_SUF = ":cam1:Acquire"
    _ACQ_RBV = ":cam1:Acquire_RBV"

    def __init__(self, detector_pvs: dict, parent=None):
        super().__init__(parent)
        self._det_pvs  = detector_pvs
        self._img_data : "np.ndarray | None" = None
        self._auto     = True
        self._cmap     = "viridis"
        self._im       = None
        self._cbar     = None

        self.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(self); vl.setContentsMargins(4,4,4,4); vl.setSpacing(4)

        # ── Toolbar ───────────────────────────────────────────────────────────
        tb = QHBoxLayout(); tb.setSpacing(8)
        lbl = QLabel("Detector")
        lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        tb.addWidget(lbl)
        self._det_combo = QComboBox(); self._det_combo.setStyleSheet(COMBO_STYLE)
        self._det_combo.setFixedWidth(140)
        for name in self._det_pvs: self._det_combo.addItem(name)
        self._det_combo.currentTextChanged.connect(self._on_det_changed)
        tb.addWidget(self._det_combo)

        tb.addSpacing(8)
        lbl2 = QLabel("Colormap")
        lbl2.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        tb.addWidget(lbl2)
        self._cmap_combo = QComboBox(); self._cmap_combo.setStyleSheet(COMBO_STYLE)
        self._cmap_combo.setFixedWidth(90)
        for cm in self._CMAPS: self._cmap_combo.addItem(cm)
        self._cmap_combo.currentTextChanged.connect(self._on_cmap_changed)
        tb.addWidget(self._cmap_combo)

        self._auto_btn = QPushButton("Auto ✓")
        self._auto_btn.setStyleSheet(BTN_STYLE)
        self._auto_btn.setFixedWidth(68)
        self._auto_btn.setCheckable(True); self._auto_btn.setChecked(True)
        self._auto_btn.toggled.connect(self._on_auto_toggled)
        tb.addWidget(self._auto_btn)

        self._acq_btn = QPushButton("▶ Acquire")
        self._acq_btn.setStyleSheet(BTN_STYLE)
        self._acq_btn.setFixedWidth(90)
        self._acq_btn.setCheckable(True)
        self._acq_btn.toggled.connect(self._on_acq_toggled)
        tb.addWidget(self._acq_btn)

        self._pva_lbl = QLabel("PVA: —")
        self._pva_lbl.setFont(QFont("Monospace", 7))
        self._pva_lbl.setStyleSheet(f"color:{PAL['subtext']};")
        tb.addWidget(self._pva_lbl)
        tb.addStretch()
        vl.addLayout(tb)

        # ── Plot canvas ───────────────────────────────────────────────────────
        if MPL_AVAILABLE:
            from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
            from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar
            from matplotlib.figure import Figure
            self._fig = Figure(facecolor=PAL["bg"])
            self._fig.subplots_adjust(left=0.1, right=0.85, top=0.92, bottom=0.1)
            self._ax  = self._fig.add_subplot(111)
            self._ch_v = self._ax.axvline(x=0, color="white", lw=0.7, ls="--", visible=False)
            self._ch_h = self._ax.axhline(y=0, color="white", lw=0.7, ls="--", visible=False)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            self._canvas.mpl_connect("motion_notify_event", self._on_mouse_move)
            self._canvas.mpl_connect("axes_leave_event",    self._on_axes_leave)
            self._nav = NavToolbar(self._canvas, self)
            self._nav.setStyleSheet(
                f"background:{PAL['surface']}; color:{PAL['text']};"
                f"border:none; font-size:8pt;")
            vl.addWidget(self._nav)
            vl.addWidget(self._canvas, 1)
        else:
            ph = QLabel("matplotlib not installed")
            ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph, 1)

        self._status = QLabel("No image")
        self._status.setFont(QFont("Monospace", 7))
        self._status.setStyleSheet(f"color:{PAL['subtext']}; padding:2px 4px;")
        vl.addWidget(self._status)

        # ── PVA bridge ────────────────────────────────────────────────────────
        self._bridge = _PVABridge(self)
        self._bridge.new_frame.connect(self._on_frame)
        self._mon = PVMonitor()
        self._mon.value_changed.connect(self._sync_acq_rbv)

        if PVA_AVAILABLE:
            self._pva_lbl.setText("PVA: ok")
            self._pva_lbl.setStyleSheet(f"color:{PAL['ok']}; font-size:7pt;")
            if self._det_pvs:
                first = next(iter(self._det_pvs))
                QTimer.singleShot(500, lambda: self._on_det_changed(first))
        else:
            self._pva_lbl.setText("PVA: missing")
            self._pva_lbl.setStyleSheet(f"color:{PAL['nc']}; font-size:7pt;")
            if MPL_AVAILABLE:
                self._sim_timer = QTimer(self)
                self._sim_timer.timeout.connect(self._push_sim_frame)
                self._sim_timer.start(2000)
                self._push_sim_frame()

    def closeEvent(self, ev):
        self._bridge.close(); super().closeEvent(ev)

    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=6)
        ax.set_title("No image loaded", color=PAL["subtext"], fontsize=8)

    def _on_det_changed(self, name: str):
        prefix = self._det_pvs.get(name, "")
        pv = prefix + self._IMG_SUF if prefix else ""
        self._img_data = None; self._im = None
        if self._cbar is not None:
            try: self._cbar.remove()
            except: pass
            self._cbar = None
        if MPL_AVAILABLE:
            self._ax.cla(); self._style_ax(); self._canvas.draw_idle()
        self._acq_btn.blockSignals(True)
        self._acq_btn.setChecked(False)
        self._acq_btn.setText("▶ Acquire")
        self._acq_btn.setStyleSheet(BTN_STYLE)
        self._acq_btn.blockSignals(False)
        if prefix:
            self._mon.subscribe(prefix + self._ACQ_RBV)
            self._bridge.subscribe(pv)
            self._status.setText(f"Monitoring  {pv} …")
        else:
            self._status.setText("No PV configured for this detector")

    def _on_cmap_changed(self, cmap: str):
        self._cmap = cmap
        if self._im is not None and MPL_AVAILABLE:
            self._im.set_cmap(cmap); self._canvas.draw_idle()

    def _on_auto_toggled(self, checked: bool):
        self._auto = checked
        self._auto_btn.setText("Auto ✓" if checked else "Auto ✗")
        if checked and self._img_data is not None:
            self._render(self._img_data)

    def _on_acq_toggled(self, checked: bool):
        prefix = self._det_pvs.get(self._det_combo.currentText(), "")
        if not prefix: return
        pv = prefix + self._ACQ_SUF
        val = 1 if checked else 0
        self._acq_btn.setText("■ Stop" if checked else "▶ Acquire")
        self._acq_btn.setStyleSheet(
            BTN_STYLE.replace("background:#1a2a4a", "background:#7a1a1a") if checked
            else BTN_STYLE)
        try:
            import epics; epics.caput(pv, val)
        except ImportError:
            print(f"[SIM] caput {pv} = {val}")

    def _sync_acq_rbv(self, name: str, value):
        prefix = self._det_pvs.get(self._det_combo.currentText(), "")
        if not prefix or name != prefix + self._ACQ_RBV: return
        try: acquiring = bool(int(value))
        except (TypeError, ValueError): return
        self._acq_btn.blockSignals(True)
        self._acq_btn.setChecked(acquiring)
        self._acq_btn.setText("■ Stop" if acquiring else "▶ Acquire")
        self._acq_btn.setStyleSheet(
            BTN_STYLE.replace("background:#1a2a4a", "background:#7a1a1a") if acquiring
            else BTN_STYLE)
        self._acq_btn.blockSignals(False)

    def _on_frame(self, img: "np.ndarray"):
        det = self._det_combo.currentText()
        h, w = img.shape
        self._status.setText(f"{det}  {w}×{h}  —  move mouse over image for pixel value")
        self._render(img)

    def _on_mouse_move(self, ev):
        if ev.inaxes is not self._ax or self._img_data is None: return
        col, row = int(ev.xdata + 0.5), int(ev.ydata + 0.5)
        h, w = self._img_data.shape
        if 0 <= col < w and 0 <= row < h:
            val = self._img_data[row, col]
            self._ch_v.set_xdata([col]); self._ch_v.set_visible(True)
            self._ch_h.set_ydata([row]); self._ch_h.set_visible(True)
            self._canvas.draw_idle()
            self._status.setText(f"col={col}  row={row}  value={val:.4g}")

    def _on_axes_leave(self, _ev):
        self._ch_v.set_visible(False); self._ch_h.set_visible(False)
        if MPL_AVAILABLE: self._canvas.draw_idle()

    def _render(self, img: "np.ndarray"):
        if not MPL_AVAILABLE: return
        self._img_data = img
        if self._im is None:
            self._im = self._ax.imshow(img, origin="lower", aspect="equal",
                                       cmap=self._cmap, interpolation="nearest")
            self._cbar = self._fig.colorbar(self._im, ax=self._ax,
                                            fraction=0.046, pad=0.04)
            self._cbar.ax.tick_params(colors=PAL["subtext"], labelsize=6)
            self._cbar.outline.set_edgecolor("#2a3a5e")
            self._ax.add_line(self._ch_v); self._ax.add_line(self._ch_h)
        else:
            self._im.set_data(img); self._im.set_cmap(self._cmap)
        if self._auto: self._im.autoscale()
        h, w = img.shape
        self._ax.set_title(f"{self._det_combo.currentText()}  —  {w}×{h}",
                           color=PAL["text"], fontsize=8)
        self._canvas.draw_idle()

    def _push_sim_frame(self):
        if not MPL_AVAILABLE: return
        import numpy as np
        N = 256
        y, x = np.ogrid[:N, :N]
        cx = N/2 + np.random.uniform(-3, 3); cy = N/2 + np.random.uniform(-3, 3)
        r = np.hypot(x - cx, y - cy)
        img = np.zeros((N, N), dtype=float)
        for r0, bw, A in [(40,4,800),(70,6,600),(100,5,400),(130,7,250)]:
            img += A * np.exp(-0.5*((r-r0)/bw)**2)
        img += np.random.normal(scale=15, size=img.shape)
        img  = np.clip(img, 0, None)
        self._status.setText(f"[SIM]  {self._det_combo.currentText() or 'simDetector'}  {N}×{N}")
        self._render(img)


# ── Camera Panel ──────────────────────────────────────────────────────────────
class CameraPanel(QWidget):
    """
    Live camera feed panel for use in any tab.

    Wraps :class:`DetectorImageViewer` and adds a labelled camera-selector
    combo above it.  The combo is pre-populated from *camera_pvs*; selecting
    a different entry switches the underlying PVA subscription.

    Parameters
    ----------
    camera_pvs : dict
        Mapping of friendly camera name → PV prefix (same convention as
        ``hirrixs.json`` ``camera`` section).
    parent : QWidget, optional
    """

    def __init__(self, camera_pvs: dict, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(self); vl.setContentsMargins(0, 0, 0, 0); vl.setSpacing(4)

        # ── Camera selector row ───────────────────────────────────────────────
        sel_row = QHBoxLayout(); sel_row.setSpacing(6)
        sel_lbl = QLabel("Camera")
        sel_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        sel_row.addWidget(sel_lbl)
        self._cam_combo = QComboBox(); self._cam_combo.setStyleSheet(COMBO_STYLE)
        for name in camera_pvs:
            self._cam_combo.addItem(name)
        sel_row.addWidget(self._cam_combo, 1)
        vl.addLayout(sel_row)

        # ── Image viewer ──────────────────────────────────────────────────────
        self._viewer = DetectorImageViewer(detector_pvs=camera_pvs, parent=self)
        # Hide the viewer's internal detector combo — the panel's combo drives it
        self._viewer._det_combo.setVisible(False)
        # Also hide the redundant "Detector" label inside the viewer toolbar
        # (first child of the toolbar HBoxLayout that is a QLabel)
        vl.addWidget(self._viewer, 1)

        self._cam_combo.currentTextChanged.connect(self._viewer._on_det_changed)

    def closeEvent(self, ev):
        self._viewer._bridge.close(); super().closeEvent(ev)
