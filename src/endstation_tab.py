"""
endstation_tab.py — Tab 2: HiRRIXS Endstation 3D viewer, live chart,
                    detector image viewer, motor/signal tables.
"""
from collections import deque
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QScrollArea,
    QSizePolicy, QComboBox, QPushButton, QSplitter, QGroupBox, QLineEdit,
    QFileDialog, QGraphicsOpacityEffect,
)
from PySide6.QtCore import Qt, QTimer, Signal, QObject, QRect, QPoint, QSize, QRectF
from PySide6.QtGui import QFont, QPixmap, QPainter, QColor, QPen, QCursor, QDoubleValidator

from common import (
    PAL, COMBO_STYLE, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE,
    MPL_AVAILABLE, PVMonitor, PVLabel, fill_table,
)
from beamline_tab import ScanWindow

if MPL_AVAILABLE:
    import numpy as np
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar
    from matplotlib.figure import Figure

try:
    from p4p.client.thread import Context as PVAContext
    PVA_AVAILABLE = True
except ImportError:
    PVA_AVAILABLE = False

HISTORY = 200

# ── Live Chart ────────────────────────────────────────────────────────────────
class LiveChart(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._x_pv = self._y_pv = ""
        self._x_buf: deque = deque(maxlen=HISTORY)
        self._y_buf: deque = deque(maxlen=HISTORY)
        self._x_val = self._y_val = None
        vl = QVBoxLayout(self); vl.setContentsMargins(0,0,0,0)
        if MPL_AVAILABLE:
            self._fig  = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax   = self._fig.add_subplot(111)
            self._line, = self._ax.plot([], [], color=PAL["accent"],
                                        linewidth=1.2, marker=".", markersize=3)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            vl.addWidget(self._canvas)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph)
        PVMonitor().value_changed.connect(self._on_pv)

    def set_x_pv(self, pv): self._x_pv=pv; self._x_val=None; self._clear()
    def set_y_pv(self, pv): self._y_pv=pv; self._y_val=None; self._clear()
    def set_labels(self, xl, yl):
        if not MPL_AVAILABLE: return
        self._ax.set_xlabel(xl, color=PAL["subtext"], fontsize=8)
        self._ax.set_ylabel(yl, color=PAL["subtext"], fontsize=8)
        self._ax.set_title(f"{yl}  vs  {xl}", color=PAL["text"], fontsize=9)
        self._canvas.draw_idle()
    def clear_data(self): self._clear()

    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.set_xlabel("x motor", color=PAL["subtext"], fontsize=8)
        ax.set_ylabel("signal",  color=PAL["subtext"], fontsize=8)
        ax.set_title("Select axes to begin", color=PAL["text"], fontsize=9)
        ax.grid(True, color="#2a3a5e", linewidth=0.5, linestyle="--")

    def _clear(self):
        self._x_buf.clear(); self._y_buf.clear()
        if MPL_AVAILABLE:
            self._line.set_data([], []); self._ax.relim(); self._canvas.draw_idle()

    def _on_pv(self, name, value):
        if value is None: return
        try: fv = float(value)
        except: return
        if   name == self._x_pv: self._x_val = fv
        elif name == self._y_pv: self._y_val = fv
        else: return
        if self._x_val is not None and self._y_val is not None:
            self._x_buf.append(self._x_val); self._y_buf.append(self._y_val)
            self._y_val = None
            if MPL_AVAILABLE:
                self._line.set_data(list(self._x_buf), list(self._y_buf))
                self._ax.relim(); self._ax.autoscale_view(); self._canvas.draw_idle()


# ── PVA image bridge — emits new frames onto the Qt main thread ───────────────
class _PVABridge(QObject):
    """Receives p4p monitor callbacks (arbitrary thread) and re-emits on Qt thread.

    Frames arriving faster than *min_interval_ms* are silently dropped so the
    Qt main thread is never saturated by a high-rate detector.
    """
    new_frame = Signal(object)   # payload: numpy ndarray (2-D)
    MIN_INTERVAL_MS = 200        # max ~5 Hz to the GUI regardless of IOC rate

    def __init__(self, parent=None):
        super().__init__(parent)
        self._ctx  : "PVAContext | None" = None
        self._sub  = None                        # active p4p subscription
        self._last_emit = 0.0                    # monotonic time of last emit

    def subscribe(self, pv_name: str):
        """Cancel any existing subscription and start monitoring *pv_name*."""
        self._cancel()
        if not PVA_AVAILABLE or not pv_name:
            return
        self._pending = pv_name
        # Defer Context creation off the main thread to avoid blocking the UI
        import threading
        threading.Thread(target=self._connect, args=(pv_name,), daemon=True).start()

    def _connect(self, pv_name: str):
        try:
            if self._ctx is None:
                self._ctx = PVAContext("pva", nt=False)
            # check we haven't been superseded by a detector switch
            if getattr(self, "_pending", None) != pv_name:
                return
            self._sub = self._ctx.monitor(
                pv_name,
                self._cb,
                notify_disconnect=True,
            )
        except Exception as e:
            print(f"[PVABridge] subscribe error: {e}")

    def _cb(self, value):
        """Called by p4p in its own thread."""
        if isinstance(value, Exception):
            return
        import time
        now = time.monotonic()
        if (now - self._last_emit) * 1000 < self.MIN_INTERVAL_MS:
            return                               # drop frame — too soon
        try:
            import numpy as np
            raw  = np.asarray(value.value, dtype=float)
            dims = value.dimension
            if len(dims) < 2:
                return
            ny = int(dims[0].size)
            nx = int(dims[1].size)
            if nx * ny == 0:
                return
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
    2-D area-detector image display via EPICS PV Access (p4p).

    Data source: single NTNDArray PV  <prefix>:RawImg
        .value       — flattened pixel array
        .dimension   — [{size: ny, …}, {size: nx, …}]

    Falls back to synthetic diffraction-ring simulation when p4p is
    unavailable or no detector prefix is configured.

    Features
    --------
    * Detector selector (populated from hirrixs detector config)
    * Colormap selector: viridis / inferno / gray / plasma / hot
    * Auto-scale toggle
    * matplotlib imshow canvas + Navigation toolbar (zoom / pan)
    * Crosshair that tracks mouse motion; status bar shows (col, row, value)
    """
    _CMAPS   = ["viridis", "inferno", "gray", "plasma", "hot"]
    _IMG_SUF = ":RawImg"
    _ACQ_SUF = ":cam1:Acquire"
    _ACQ_RBV = ":cam1:Acquire_RBV"

    def __init__(self, detector_pvs: dict, parent=None):
        """
        Parameters
        ----------
        detector_pvs : dict  {name: base_pv_prefix}
            e.g. {"simDetector": "6013SIM1", "Andor": "6013ANDOR1"}
        """
        super().__init__(parent)
        self._det_pvs  = detector_pvs
        self._img_data : "np.ndarray | None" = None
        self._auto     = True
        self._cmap     = "viridis"
        self._im       = None
        self._cbar     = None

        self.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(self); vl.setContentsMargins(4,4,4,4); vl.setSpacing(4)

        # ── toolbar row ──────────────────────────────────────────────────────
        tb = QHBoxLayout(); tb.setSpacing(8)

        lbl = QLabel("Detector")
        lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        tb.addWidget(lbl)
        self._det_combo = QComboBox(); self._det_combo.setStyleSheet(COMBO_STYLE)
        self._det_combo.setFixedWidth(140)
        for name in self._det_pvs:
            self._det_combo.addItem(name)
        self._det_combo.currentTextChanged.connect(self._on_det_changed)
        tb.addWidget(self._det_combo)

        tb.addSpacing(8)
        lbl2 = QLabel("Colormap")
        lbl2.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        tb.addWidget(lbl2)
        self._cmap_combo = QComboBox(); self._cmap_combo.setStyleSheet(COMBO_STYLE)
        self._cmap_combo.setFixedWidth(90)
        for cm in self._CMAPS:
            self._cmap_combo.addItem(cm)
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

        # PVA status indicator
        self._pva_lbl = QLabel("PVA: —")
        self._pva_lbl.setFont(QFont("Monospace", 7))
        self._pva_lbl.setStyleSheet(f"color:{PAL['subtext']};")
        tb.addWidget(self._pva_lbl)

        tb.addStretch()
        vl.addLayout(tb)

        # ── matplotlib canvas ────────────────────────────────────────────────
        if MPL_AVAILABLE:
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
                f"border:none; font-size:8pt;"
            )
            vl.addWidget(self._nav)
            vl.addWidget(self._canvas, 1)
        else:
            ph = QLabel("matplotlib not installed")
            ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            vl.addWidget(ph, 1)

        # ── status bar ───────────────────────────────────────────────────────
        self._status = QLabel("No image")
        self._status.setFont(QFont("Monospace", 7))
        self._status.setStyleSheet(f"color:{PAL['subtext']}; padding:2px 4px;")
        vl.addWidget(self._status)

        # PVA bridge
        self._bridge = _PVABridge(self)
        self._bridge.new_frame.connect(self._on_frame)

        # CA monitor for Acquire_RBV
        self._mon = PVMonitor()
        self._mon.value_changed.connect(self._sync_acq_rbv)

        if PVA_AVAILABLE:
            self._pva_lbl.setText("PVA: ok")
            self._pva_lbl.setStyleSheet(f"color:{PAL['ok']}; font-size:7pt;")
            if self._det_pvs:
                first = next(iter(self._det_pvs))
                # Defer first subscription until after the main window is shown
                QTimer.singleShot(500, lambda: self._on_det_changed(first))
        else:
            self._pva_lbl.setText("PVA: missing")
            self._pva_lbl.setStyleSheet(f"color:{PAL['nc']}; font-size:7pt;")
            # fall back to simulation
            self._sim_timer = QTimer(self)
            self._sim_timer.timeout.connect(self._push_sim_frame)
            self._sim_timer.start(2000)
            self._push_sim_frame()

    def closeEvent(self, ev):
        self._bridge.close()
        super().closeEvent(ev)

    # ── helpers ───────────────────────────────────────────────────────────────
    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=6)
        ax.set_title("No image loaded", color=PAL["subtext"], fontsize=8)

    # ── slots ─────────────────────────────────────────────────────────────────
    def _on_det_changed(self, name: str):
        prefix = self._det_pvs.get(name, "")
        pv = prefix + self._IMG_SUF if prefix else ""
        self._img_data = None
        self._im = None
        if self._cbar is not None:
            try: self._cbar.remove()
            except: pass
            self._cbar = None
        self._ax.cla() if MPL_AVAILABLE else None
        if MPL_AVAILABLE:
            self._style_ax()
            self._canvas.draw_idle()
        # reset acquire button
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
        if not prefix:
            return
        pv = prefix + self._ACQ_SUF
        val = 1 if checked else 0
        self._acq_btn.setText("■ Stop" if checked else "▶ Acquire")
        self._acq_btn.setStyleSheet(
            BTN_STYLE.replace("background:#1a2a4a", "background:#7a1a1a") if checked
            else BTN_STYLE
        )
        try:
            import epics
            epics.caput(pv, val)
        except ImportError:
            print(f"[SIM] caput {pv} = {val}")

    def _sync_acq_rbv(self, name: str, value):
        """Keep the Acquire button in sync with the hardware RBV."""
        prefix = self._det_pvs.get(self._det_combo.currentText(), "")
        if not prefix or name != prefix + self._ACQ_RBV:
            return
        try:
            acquiring = bool(int(value))
        except (TypeError, ValueError):
            return
        # block signal to avoid retriggering caput
        self._acq_btn.blockSignals(True)
        self._acq_btn.setChecked(acquiring)
        self._acq_btn.setText("■ Stop" if acquiring else "▶ Acquire")
        self._acq_btn.setStyleSheet(
            BTN_STYLE.replace("background:#1a2a4a", "background:#7a1a1a") if acquiring
            else BTN_STYLE
        )
        self._acq_btn.blockSignals(False)

    def _on_frame(self, img: "np.ndarray"):
        """Slot — always called on Qt main thread via Signal."""
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

    # ── rendering ─────────────────────────────────────────────────────────────
    def _render(self, img: "np.ndarray"):
        if not MPL_AVAILABLE: return
        self._img_data = img
        if self._im is None:
            self._im = self._ax.imshow(
                img, origin="lower", aspect="equal",
                cmap=self._cmap, interpolation="nearest",
            )
            self._cbar = self._fig.colorbar(
                self._im, ax=self._ax, fraction=0.046, pad=0.04
            )
            self._cbar.ax.tick_params(colors=PAL["subtext"], labelsize=6)
            self._cbar.outline.set_edgecolor("#2a3a5e")
            self._ax.add_line(self._ch_v)
            self._ax.add_line(self._ch_h)
        else:
            self._im.set_data(img)
            self._im.set_cmap(self._cmap)
        if self._auto:
            self._im.autoscale()
        h, w = img.shape
        self._ax.set_title(
            f"{self._det_combo.currentText()}  —  {w}×{h}",
            color=PAL["text"], fontsize=8,
        )
        self._canvas.draw_idle()

    # ── simulation (p4p unavailable) ──────────────────────────────────────────
    def _push_sim_frame(self):
        if not MPL_AVAILABLE: return
        N = 256
        y, x = np.ogrid[:N, :N]
        cx = N/2 + np.random.uniform(-3, 3)
        cy = N/2 + np.random.uniform(-3, 3)
        r   = np.hypot(x - cx, y - cy)
        img = np.zeros((N, N), dtype=float)
        for r0, bw, A in [(40,4,800),(70,6,600),(100,5,400),(130,7,250)]:
            img += A * np.exp(-0.5*((r-r0)/bw)**2)
        img += np.random.normal(scale=15, size=img.shape)
        img  = np.clip(img, 0, None)
        self._status.setText(f"[SIM]  {self._det_combo.currentText() or 'simDetector'}  {N}×{N}")
        self._render(img)


# ── PV overlay widget ─────────────────────────────────────────────────────────
class _PVOverlayWidget(QWidget):
    """
    Small floating widget drawn over _ImageCanvas showing a PV readback
    and an editable setpoint field.  Opacity is controlled externally.
    """
    _STYLE_BASE = """
        QWidget {{ background: rgba(20,30,50,210); border-radius: 4px; }}
        QLabel  {{ color: {fg}; font-size: 7pt; background: transparent; padding: 0 2px; }}
        QLineEdit {{
            background: rgba(255,255,255,30); color: {fg};
            border: 1px solid rgba(255,220,80,160); border-radius: 3px;
            font-size: 7pt; padding: 1px 3px;
        }}
    """

    def __init__(self, label: str, rbv_pv: str, sp_pv: str, parent=None):
        super().__init__(parent)
        self._sp_pv  = sp_pv
        self._label  = label
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setStyleSheet(self._STYLE_BASE.format(fg="#e0e8ff"))

        hl = QHBoxLayout(self)
        hl.setContentsMargins(6, 3, 6, 3); hl.setSpacing(6)

        lbl = QLabel(label); lbl.setFont(QFont("Sans Serif", 7, QFont.Bold))
        lbl.setStyleSheet("color:#ffd850; background:transparent;")
        hl.addWidget(lbl)

        self._rbv = QLabel("—")
        self._rbv.setMinimumWidth(30)
        self._rbv.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        hl.addWidget(self._rbv)

        self._sp = QLineEdit()
        self._sp.setPlaceholderText("setpoint")
        self._sp.setFixedWidth(64)
        self._sp.setValidator(QDoubleValidator(-1e9, 1e9, 4))
        self._sp.returnPressed.connect(self._send)
        hl.addWidget(self._sp)

        self.adjustSize()

        # opacity effect — faded by default
        self._eff = QGraphicsOpacityEffect(self)
        self._eff.setOpacity(0.35)
        self.setGraphicsEffect(self._eff)

        # subscribe to RBV
        PVMonitor().value_changed.connect(self._on_pv)
        PVMonitor().subscribe(rbv_pv)
        self._rbv_pv = rbv_pv

    def set_hovered(self, hovered: bool):
        self._eff.setOpacity(1.0 if hovered else 0.35)

    def _on_pv(self, name: str, value):
        if name != self._rbv_pv: return
        try:    self._rbv.setText(f"{float(value):.4f}")
        except: self._rbv.setText(str(value))

    def _send(self):
        txt = self._sp.text().strip()
        if not txt: return
        try:
            val = float(txt)
            try:
                import epics
                epics.caput(self._sp_pv, val)
            except ImportError:
                print(f"[SIM] caput {self._sp_pv} = {val}")
        except ValueError:
            pass
        self._sp.clear()


# ── Hit region definition ─────────────────────────────────────────────────────
# Rectangles in original image-space pixels (x, y, w, h) at 2222×1316.
# Tweak these to align with the rendered PNG.
_REGIONS = [
    # name           x     y     w    h    motors (from hirrixs config)
    ("Sample",       339,  688,   90,  70, ["MainManipX", "MainManipY",
                                            "MainManipZ", "MainManiptheta"]),
    ("Microscope",   529,  490,  190, 160, ["MicroscopeX", "MicroscopeY", "MicroscopeZ"]),
    ("Mirror",      1230,  640,  470, 275, ["MirrorAngle"]),
    ("Optics",      1260,  915,  550, 200, ["SpectOpticsHeight", "SpectOpticsPitch",
                                            "SpectOpticsRoll"]),
    ("Grating",     1560,  490,  555, 200, ["GratingAngle"]),
    ("Detector",    1890,  810,  330, 260, ["DetectorX", "DetectorY"]),
]
_HOVER_COLOR  = QColor(255, 220,  80, 90)
_BORDER_COLOR = QColor(255, 220,  80, 200)
_LABEL_COLOR  = QColor(255, 255, 255, 220)

_OVERLAYS = [
    # (region_name, img_x, img_y, rbv_pv, sp_pv, label)
    # Mirror
    ("Mirror",       850,   500, "BL6013:MirrorAngle",       "BL6013:MirrorAngle",      "Mirror Angle"),
    # Grating
    ("Grating",     1560,   350, "BL6013:GratingAnglealpha", "BL6013:GratingAnglealpha", "Grating Angle"),
    # Optics
    ("Optics",       900,  1120, "BL6013:SpectOpticsHeight", "BL6013:SpectOpticsHeight", "Optics Height"),
    ("Optics",       900,  1195, "BL6013:SpectOpticsPitch",  "BL6013:SpectOpticsPitch",  "Optics Pitch"),
    ("Optics",       900,  1270, "BL6013:SpectOpticsRoll",   "BL6013:SpectOpticsRoll",   "Optics Roll"),
    # Detector
    ("Detector",    1600,  1100, "BL6013:DetectorX",         "BL6013:DetectorX",         "Detector X"),
    ("Detector",    1600,  1175, "BL6013:DetectorZ",         "BL6013:DetectorZ",         "Detector Z"),
    # Microscope
    ("Microscope",   450,   250, "BL6013:MicroscopeX",       "BL6013:MicroscopeX",       "Microscope X"),
    ("Microscope",   450,   325, "BL6013:MicroscopeY",       "BL6013:MicroscopeY",       "Microscope Y"),
    ("Microscope",   450,   400, "BL6013:MicroscopeZ",       "BL6013:MicroscopeZ",       "Microscope Z"),
    # Sample / Manipulator
    ("Sample",       220,   775, "BL6013:MainManipX",        "BL6013:MainManipX",        "Manip X"),
    ("Sample",       220,   850, "BL6013:MainManipY",        "BL6013:MainManipY",        "Manip Y"),
    ("Sample",       220,   925, "BL6013:MainManipZ",        "BL6013:MainManipZ",        "Manip Z"),
    ("Sample",       220,  1000, "BL6013:MainManiptheta",    "BL6013:MainManiptheta",    "Manip θ"),
]


# ── Clickable image viewer ────────────────────────────────────────────────────
class EndstationViewer(QWidget):
    """
    Displays a static PNG of the endstation with named clickable hit regions.
    Regions are defined in original image-space coordinates and scaled to
    whatever size the widget is currently drawn at.
    Clicking a region opens ScanWindow for the associated motors.
    """
    # image natural size (matches camera width/height in shapescript)
    _IMG_W = 2222
    _IMG_H = 1316
    _DEFAULT_IMG = Path(__file__).parent.parent / "config" / "RIXS_endstation.png"

    def __init__(self, all_pvs: dict, all_signals: dict, parent=None):
        super().__init__(parent)
        self._all_pvs     = all_pvs
        self._all_signals = all_signals
        self._pixmap      : QPixmap | None = None
        self._hovered     : str | None = None   # name of hovered region
        self.setMouseTracking(True)
        self.setCursor(QCursor(Qt.ArrowCursor))
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setStyleSheet(f"background:{PAL['bg']};")

        vl = QVBoxLayout(self); vl.setContentsMargins(4,4,4,4); vl.setSpacing(4)

        # toolbar
        tb = QHBoxLayout(); tb.setSpacing(6)
        title = QLabel("Endstation Schematic")
        title.setFont(QFont("Sans Serif", 8, QFont.Bold))
        title.setStyleSheet(f"color:{PAL['accent']};")
        tb.addWidget(title)
        self._path_edit = QLineEdit()
        self._path_edit.setPlaceholderText("Path to image …")
        self._path_edit.setStyleSheet(INPUT_STYLE)
        tb.addWidget(self._path_edit, 1)
        browse_btn = QPushButton("Browse"); browse_btn.setStyleSheet(BTN_STYLE)
        browse_btn.setFixedWidth(60); browse_btn.clicked.connect(self._browse)
        tb.addWidget(browse_btn)
        vl.addLayout(tb)

        # canvas — we paint directly on this widget below the toolbar
        self._canvas = _ImageCanvas(self)
        self._canvas.region_clicked.connect(self._on_region_clicked)
        vl.addWidget(self._canvas, 1)

        # load default image if present
        if self._DEFAULT_IMG.exists():
            self.load_file(str(self._DEFAULT_IMG))
            self._path_edit.setText(str(self._DEFAULT_IMG))

    def _browse(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open endstation image", "",
            "Images (*.png *.jpg *.jpeg *.bmp *.tif *.tiff)")
        if path:
            self._path_edit.setText(path); self.load_file(path)

    def load_file(self, path: str):
        px = QPixmap(path)
        if not px.isNull():
            self._canvas.set_pixmap(px)

    def _on_region_clicked(self, name: str, motors: list):
        # collect PVs for this region's motors
        pvs = {m: self._all_pvs[m] for m in motors if m in self._all_pvs}
        if not pvs:
            pvs = {"(no motors)": ""}
        dlg = ScanWindow(name, pvs, self._all_signals, self)
        dlg.exec()

    def initialize(self):
        pass


class _ImageCanvas(QWidget):
    """Inner widget that owns painting, mouse logic, and PV overlays."""
    region_clicked = Signal(str, list)   # (region name, motor list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._pixmap  : QPixmap | None = None
        self._hovered : str | None     = None
        self.setMouseTracking(True)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # build overlay widgets — multiple per region, keyed by region name
        self._overlays: dict[str, list[_PVOverlayWidget]] = {}
        for region_name, ix, iy, rbv_pv, sp_pv, label in _OVERLAYS:
            w = _PVOverlayWidget(label, rbv_pv, sp_pv, parent=self)
            w.setProperty("img_x", ix)
            w.setProperty("img_y", iy)
            w.show()
            self._overlays.setdefault(region_name, []).append(w)

    def set_pixmap(self, px: QPixmap):
        self._pixmap = px; self._reposition_overlays(); self.update()

    # ── coordinate helpers ────────────────────────────────────────────────────
    def _draw_rect(self) -> QRect:
        if self._pixmap is None:
            return self.rect()
        pw, ph = self._pixmap.width(), self._pixmap.height()
        ww, wh = self.width(), self.height()
        scale = min(ww / pw, wh / ph)
        dw, dh = int(pw * scale), int(ph * scale)
        return QRect((ww - dw) // 2, (wh - dh) // 2, dw, dh)

    def _scale_region(self, rx, ry, rw, rh) -> QRect:
        dr = self._draw_rect()
        if self._pixmap is None:
            return QRect()
        sx = dr.width()  / self._pixmap.width()
        sy = dr.height() / self._pixmap.height()
        return QRect(
            dr.x() + int(rx * sx),
            dr.y() + int(ry * sy),
            int(rw * sx),
            int(rh * sy),
        )

    def _img_to_widget(self, ix: int, iy: int) -> QPoint:
        """Convert image-space point to widget-space."""
        dr = self._draw_rect()
        if self._pixmap is None:
            return QPoint(ix, iy)
        sx = dr.width()  / self._pixmap.width()
        sy = dr.height() / self._pixmap.height()
        return QPoint(dr.x() + int(ix * sx), dr.y() + int(iy * sy))

    def _region_at(self, pos: QPoint) -> tuple | None:
        for name, rx, ry, rw, rh, motors in _REGIONS:
            if self._scale_region(rx, ry, rw, rh).contains(pos):
                return (name, motors)
        return None

    def _reposition_overlays(self):
        for widgets in self._overlays.values():
            for w in widgets:
                ix = w.property("img_x"); iy = w.property("img_y")
                pt = self._img_to_widget(ix, iy)
                w.move(pt)

    # ── events ────────────────────────────────────────────────────────────────
    def resizeEvent(self, ev):
        super().resizeEvent(ev)
        self._reposition_overlays()

    def mouseMoveEvent(self, ev):
        hit = self._region_at(ev.position().toPoint())
        name = hit[0] if hit else None
        if name != self._hovered:
            self._hovered = name
            self.setCursor(QCursor(Qt.PointingHandCursor if name else Qt.ArrowCursor))
            # update overlay opacities
            for rname, widgets in self._overlays.items():
                for w in widgets:
                    w.set_hovered(rname == name)
            self.update()
        if name:
            pass  # tooltip removed — region name shown in hover label on canvas

    def mousePressEvent(self, ev):
        if ev.button() == Qt.LeftButton:
            hit = self._region_at(ev.position().toPoint())
            if hit:
                self.region_clicked.emit(hit[0], hit[1])

    def leaveEvent(self, ev):
        if self._hovered:
            self._hovered = None
            self.setCursor(QCursor(Qt.ArrowCursor))
            for widgets in self._overlays.values():
                for w in widgets:
                    w.set_hovered(False)
            self.update()

    # ── painting ──────────────────────────────────────────────────────────────
    def paintEvent(self, ev):
        p = QPainter(self)
        p.setRenderHint(QPainter.SmoothPixmapTransform)

        if self._pixmap is None:
            p.setPen(QColor(PAL["subtext"]))
            p.drawText(self.rect(), Qt.AlignCenter, "No image loaded")
            return

        dr = self._draw_rect()
        p.drawPixmap(dr, self._pixmap)

        for name, rx, ry, rw, rh, _ in _REGIONS:
            sr = self._scale_region(rx, ry, rw, rh)
            if name == self._hovered:
                p.fillRect(sr, _HOVER_COLOR)
                p.setPen(QPen(_BORDER_COLOR, 2))
                p.drawRect(sr)
                p.setPen(QPen(_LABEL_COLOR))
                p.setFont(QFont("Sans Serif", 8, QFont.Bold))
                p.drawText(sr, Qt.AlignCenter, name)
            else:
                p.setPen(QPen(_BORDER_COLOR.darker(150), 1, Qt.DotLine))
                p.drawRect(sr)


# ── Endstation Tab ────────────────────────────────────────────────────────────
class EndstationTab(QWidget):
    def __init__(self, hirrixs_cfg, amber_cfg, all_pvs, all_signals, parent=None):
        super().__init__(parent)
        self.setStyleSheet(f"background:{PAL['bg']};")
        m  = hirrixs_cfg.get("motor",{})
        s  = hirrixs_cfg.get("signal",{})
        d  = hirrixs_cfg.get("detector",{})
        c  = hirrixs_cfg.get("camera",{})
        am = amber_cfg.get("motor",{})
        self._motor_pvs  = {}
        self._signal_pvs = {}
        self._motor_pvs["DIAG133"] = am.get("DIAG133","")
        self._motor_pvs["DIAG134"] = am.get("DIAG134","")
        for n,p in m.items(): self._motor_pvs[n]=p
        for n,p in s.items(): self._signal_pvs[n]=p

        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0)
        hdr = QLabel("  ⚗️  HiRRIXS Endstation")
        hdr.setFont(QFont("Sans Serif",9,QFont.Bold))
        hdr.setStyleSheet(f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        outer.addWidget(hdr)

        hsplit = QSplitter(Qt.Horizontal); hsplit.setStyleSheet(SPLITTER_STYLE)
        self._viewer = EndstationViewer(all_pvs, all_signals)
        self._viewer.setMinimumWidth(300)
        hsplit.addWidget(self._viewer)

        # ── right-side vertical splitter: chart | image viewer | tables ──────
        vsplit = QSplitter(Qt.Vertical); vsplit.setStyleSheet(SPLITTER_STYLE)

        # ── panel 1: Live Chart ───────────────────────────────────────────────
        chart_panel = QWidget(); chart_panel.setStyleSheet(f"background:{PAL['bg']};")
        top_row = QHBoxLayout(chart_panel); top_row.setSpacing(12); top_row.setContentsMargins(8,8,8,8)
        self._chart = LiveChart()
        self._chart.setMinimumSize(300,200)
        self._chart.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        top_row.addWidget(self._chart, 3)

        ctrl = QGroupBox("Chart axes"); ctrl.setStyleSheet(GRP_STYLE)
        ctrl.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        cvl = QVBoxLayout(ctrl); cvl.setContentsMargins(10,20,10,10); cvl.setSpacing(10)

        def hdr2(t):
            l = QLabel(t); l.setStyleSheet(f"color:{PAL['accent']};")
            l.setFont(QFont("Sans Serif",8,QFont.Bold)); return l

        cvl.addWidget(hdr2("Y axis — Signal"))
        self._y_combo = QComboBox(); self._y_combo.setStyleSheet(COMBO_STYLE)
        for n in self._signal_pvs: self._y_combo.addItem(n)
        cvl.addWidget(self._y_combo)
        cvl.addWidget(hdr2("X axis — Motor"))
        self._x_combo = QComboBox(); self._x_combo.setStyleSheet(COMBO_STYLE)
        for n in self._motor_pvs: self._x_combo.addItem(n)
        cvl.addWidget(self._x_combo)
        clr_btn = QPushButton("Clear plot"); clr_btn.setStyleSheet(BTN_STYLE)
        clr_btn.clicked.connect(self._chart.clear_data)
        cvl.addWidget(clr_btn); cvl.addStretch()
        top_row.addWidget(ctrl, 1)
        vsplit.addWidget(chart_panel)

        # ── panel 2: Detector Image Viewer ────────────────────────────────────
        img_grp = QGroupBox("Detector Image"); img_grp.setStyleSheet(GRP_STYLE)
        img_grp.setMinimumHeight(280)
        ig_vl = QVBoxLayout(img_grp); ig_vl.setContentsMargins(4,16,4,4)
        # Merge detectors and cameras for the image viewer (same PVA pattern)
        img_pvs = {**d, **{f"{k} (cam)": v for k, v in c.items()}}
        self._img_viewer = DetectorImageViewer(detector_pvs=img_pvs)
        ig_vl.addWidget(self._img_viewer)
        vsplit.addWidget(img_grp)

        # ── panel 3: Tables ───────────────────────────────────────────────────
        tables_scroll = QScrollArea(); tables_scroll.setWidgetResizable(True)
        tables_scroll.setStyleSheet(f"background:{PAL['bg']}; border:none;")
        tables_widget = QWidget(); tables_widget.setStyleSheet(f"background:{PAL['bg']};")
        tbl_vl = QVBoxLayout(tables_widget); tbl_vl.setContentsMargins(8,8,8,8); tbl_vl.setSpacing(12)
        tbl_row = QHBoxLayout(); tbl_row.setSpacing(12)

        motors_grp = QGroupBox("Motors"); motors_grp.setStyleSheet(GRP_STYLE)
        fill_table(motors_grp, list(self._motor_pvs.items()))
        tbl_row.addWidget(motors_grp, 3)

        right = QWidget(); right.setStyleSheet("background:transparent;")
        rvl = QVBoxLayout(right); rvl.setContentsMargins(0,0,0,0); rvl.setSpacing(12)
        sigs_grp = QGroupBox("Signals"); sigs_grp.setStyleSheet(GRP_STYLE)
        fill_table(sigs_grp, list(self._signal_pvs.items()))
        rvl.addWidget(sigs_grp)
        det_grp = QGroupBox("Detectors"); det_grp.setStyleSheet(GRP_STYLE)
        fill_table(det_grp, [(n, p+":Acquire_RBV") for n,p in d.items()])
        rvl.addWidget(det_grp)
        cam_grp = QGroupBox("Cameras"); cam_grp.setStyleSheet(GRP_STYLE)
        fill_table(cam_grp, [(n, p+":Acquire_RBV") for n,p in c.items()])
        rvl.addWidget(cam_grp); rvl.addStretch()
        tbl_row.addWidget(right, 2)
        tbl_vl.addLayout(tbl_row); tbl_vl.addStretch()
        tables_scroll.setWidget(tables_widget)
        vsplit.addWidget(tables_scroll)

        vsplit.setSizes([300, 320, 380])

        hsplit.addWidget(vsplit)
        hsplit.setSizes([500,900])
        outer.addWidget(hsplit, 1)

        mon = PVMonitor()
        for pv in {**self._motor_pvs, **self._signal_pvs}.values(): mon.subscribe(pv)
        self._x_combo.currentTextChanged.connect(self._on_axis)
        self._y_combo.currentTextChanged.connect(self._on_axis)
        self._on_axis()

    def _on_axis(self):
        xn=self._x_combo.currentText(); yn=self._y_combo.currentText()
        self._chart.set_x_pv(self._motor_pvs.get(xn,""))
        self._chart.set_y_pv(self._signal_pvs.get(yn,""))
        self._chart.set_labels(xn, yn)
