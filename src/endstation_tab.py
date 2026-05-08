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
    MPL_AVAILABLE, PVA_AVAILABLE, PVMonitor, PVLabel, fill_table,
    _PVABridge, DetectorImageViewer,
)
from beamline_tab import ScanWindow

if MPL_AVAILABLE:
    import numpy as np
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar
    from matplotlib.figure import Figure

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
            self._fig = Figure(facecolor=PAL["bg"])
            self._fig.subplots_adjust(left=0.1, right=0.85, top=0.92, bottom=0.1)
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


# ── PV overlay widget ─────────────────────────────────────────────────────────
class _PVOverlayWidget(QWidget):
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
        self._sp_pv = sp_pv
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setStyleSheet(self._STYLE_BASE.format(fg="#e0e8ff"))

        hl = QHBoxLayout(self)
        hl.setContentsMargins(6, 3, 6, 3); hl.setSpacing(6)

        lbl = QLabel(label); lbl.setFont(QFont("Sans Serif", 7, QFont.Bold))
        lbl.setStyleSheet("color:#ffd850; background:transparent;")
        hl.addWidget(lbl)

        self._rbv = QLabel("—")
        self._rbv.setMinimumWidth(60)
        self._rbv.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        hl.addWidget(self._rbv)

        self._sp = QLineEdit()
        self._sp.setPlaceholderText("setpoint")
        self._sp.setFixedWidth(64)
        self._sp.setValidator(QDoubleValidator(-1e9, 1e9, 4))
        self._sp.returnPressed.connect(self._send)
        hl.addWidget(self._sp)

        self.adjustSize()
        self._eff = QGraphicsOpacityEffect(self)
        self._eff.setOpacity(0.35)
        self.setGraphicsEffect(self._eff)

        PVMonitor().value_changed.connect(self._on_pv)
        PVMonitor().subscribe(rbv_pv)
        self._rbv_pv = rbv_pv

    def set_hovered(self, hovered: bool):
        self._eff.setOpacity(1.0 if hovered else 0.35)

    def _on_pv(self, name: str, value):
        if name != self._rbv_pv: return
        try:    self._rbv.setText(f"{float(value):.6g}")
        except: self._rbv.setText(str(value))

    def _send(self):
        txt = self._sp.text().strip()
        if not txt: return
        try:
            val = float(txt)
            try:
                import epics; epics.caput(self._sp_pv, val)
            except ImportError:
                print(f"[SIM] caput {self._sp_pv} = {val}")
        except ValueError:
            pass
        self._sp.clear()


# ── Hit region definition ─────────────────────────────────────────────────────
# Rectangles in original image-space pixels (x, y, w, h) at 2222×1316.
_REGIONS = [
    # name           x     y     w    h    motors (from hirrixs config)
    ("Sample",       300,  688,  200,  70, ["MainManipX", "MainManipY",
                                            "MainManipZ", "MainManiptheta"]),
    ("Microscope",   495,  490,  275, 160, ["MicroscopeX", "MicroscopeY", "MicroscopeZ"]),
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
    ("Mirror",       820,   500, "BL6013:MirrorAngle",       "BL6013:MirrorAngle",       "Mirror Angle"),
    # Grating
    ("Grating",     1320,   350, "BL6013:GratingAnglealpha", "BL6013:GratingAnglealpha", "Grating Angle"),
    # Optics
    ("Optics",       850,  1250, "BL6013:SpectOpticsHeight", "BL6013:SpectOpticsHeight", "Optics Height"),
    ("Optics",       850,  1325, "BL6013:SpectOpticsPitch",  "BL6013:SpectOpticsPitch",  "Optics Pitch"),
    ("Optics",       850,  1400, "BL6013:SpectOpticsRoll",   "BL6013:SpectOpticsRoll",   "Optics Roll"),
    # Detector
    ("Detector",    1400,  1100, "BL6013:DetectorX",         "BL6013:DetectorX",         "Detector X"),
    ("Detector",    1400,  1175, "BL6013:DetectorZ",         "BL6013:DetectorZ",         "Detector Z"),
    # Microscope
    ("Microscope",   250,   230, "BL6013:MicroscopeX",       "BL6013:MicroscopeX",       "Microscope X"),
    ("Microscope",   250,   305, "BL6013:MicroscopeY",       "BL6013:MicroscopeY",       "Microscope Y"),
    ("Microscope",   250,   380, "BL6013:MicroscopeZ",       "BL6013:MicroscopeZ",       "Microscope Z"),
    # Sample / Manipulator
    ("Sample",       200,   825, "BL6013:MainManipX",        "BL6013:MainManipX",        "Manip X"),
    ("Sample",       200,   900, "BL6013:MainManipY",        "BL6013:MainManipY",        "Manip Y"),
    ("Sample",       200,   975, "BL6013:MainManipZ",        "BL6013:MainManipZ",        "Manip Z"),
    ("Sample",       200,  1050, "BL6013:MainManiptheta",    "BL6013:MainManiptheta",    "Manip θ"),
]


# ── Clickable image viewer ────────────────────────────────────────────────────
class EndstationViewer(QWidget):
    _IMG_W = 2222
    _IMG_H = 1316
    _DEFAULT_IMG = Path(__file__).parent.parent / "config" / "RIXS_endstation.png"

    def __init__(self, all_pvs: dict, all_signals: dict, parent=None):
        super().__init__(parent)
        self._all_pvs     = all_pvs
        self._all_signals = all_signals
        self._pixmap      : QPixmap | None = None
        self._hovered     : str | None = None
        self.setMouseTracking(True)
        self.setCursor(QCursor(Qt.ArrowCursor))
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setStyleSheet(f"background:{PAL['bg']};")

        vl = QVBoxLayout(self); vl.setContentsMargins(4,4,4,4); vl.setSpacing(4)

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

        self._canvas = _ImageCanvas(self)
        self._canvas.region_clicked.connect(self._on_region_clicked)
        vl.addWidget(self._canvas, 1)

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
        pvs = {m: self._all_pvs[m] for m in motors if m in self._all_pvs}
        if not pvs: pvs = {"(no motors)": ""}
        dlg = ScanWindow(name, pvs, self._all_signals, self)
        dlg.exec()

    def initialize(self):
        pass


class _ImageCanvas(QWidget):
    region_clicked = Signal(str, list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._pixmap  : QPixmap | None = None
        self._hovered : str | None     = None
        self.setMouseTracking(True)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self._overlays: dict[str, list[_PVOverlayWidget]] = {}
        for region_name, ix, iy, rbv_pv, sp_pv, label in _OVERLAYS:
            w = _PVOverlayWidget(label, rbv_pv, sp_pv, parent=self)
            w.setProperty("img_x", ix)
            w.setProperty("img_y", iy)
            w.show()
            self._overlays.setdefault(region_name, []).append(w)

    def set_pixmap(self, px: QPixmap):
        self._pixmap = px; self._reposition_overlays(); self.update()

    def _draw_rect(self) -> QRect:
        if self._pixmap is None: return self.rect()
        pw, ph = self._pixmap.width(), self._pixmap.height()
        ww, wh = self.width(), self.height()
        scale = min(ww / pw, wh / ph)
        dw, dh = int(pw * scale), int(ph * scale)
        return QRect((ww - dw) // 2, (wh - dh) // 2, dw, dh)

    def _scale_region(self, rx, ry, rw, rh) -> QRect:
        dr = self._draw_rect()
        if self._pixmap is None: return QRect()
        sx = dr.width()  / self._pixmap.width()
        sy = dr.height() / self._pixmap.height()
        return QRect(dr.x() + int(rx * sx), dr.y() + int(ry * sy),
                     int(rw * sx), int(rh * sy))

    def _img_to_widget(self, ix: int, iy: int) -> QPoint:
        dr = self._draw_rect()
        if self._pixmap is None: return QPoint(ix, iy)
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

    def resizeEvent(self, ev):
        super().resizeEvent(ev); self._reposition_overlays()

    def mouseMoveEvent(self, ev):
        hit = self._region_at(ev.position().toPoint())
        name = hit[0] if hit else None
        if name != self._hovered:
            self._hovered = name
            self.setCursor(QCursor(Qt.PointingHandCursor if name else Qt.ArrowCursor))
            for rname, widgets in self._overlays.items():
                for w in widgets:
                    w.set_hovered(rname == name)
            self.update()

    def mousePressEvent(self, ev):
        if ev.button() == Qt.LeftButton:
            hit = self._region_at(ev.position().toPoint())
            if hit: self.region_clicked.emit(hit[0], hit[1])

    def leaveEvent(self, ev):
        if self._hovered:
            self._hovered = None
            self.setCursor(QCursor(Qt.ArrowCursor))
            for widgets in self._overlays.values():
                for w in widgets: w.set_hovered(False)
            self.update()

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
                p.setPen(QPen(_BORDER_COLOR, 2)); p.drawRect(sr)
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

        vsplit = QSplitter(Qt.Vertical); vsplit.setStyleSheet(SPLITTER_STYLE)

        # panel 1: Live Chart
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

        # panel 2: Detector Image Viewer
        img_grp = QGroupBox("Detector Image"); img_grp.setStyleSheet(GRP_STYLE)
        img_grp.setMinimumHeight(280)
        ig_vl = QVBoxLayout(img_grp); ig_vl.setContentsMargins(4,16,4,4)
        img_pvs = {**d, **{f"{k} (cam)": v for k, v in c.items()}}
        self._img_viewer = DetectorImageViewer(detector_pvs=img_pvs)
        ig_vl.addWidget(self._img_viewer)
        vsplit.addWidget(img_grp)

        # panel 3: Tables
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

    def apply_config(self, key: str, value):
        """Receive a config_changed signal from ConfigurationTab."""
        pass   # extend as config-driven behaviour is added

    def _on_axis(self):
        xn=self._x_combo.currentText(); yn=self._y_combo.currentText()
        self._chart.set_x_pv(self._motor_pvs.get(xn,""))
        self._chart.set_y_pv(self._signal_pvs.get(yn,""))
        self._chart.set_labels(xn, yn)
