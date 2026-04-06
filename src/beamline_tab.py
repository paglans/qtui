"""
beamline_tab.py — Tab 1: AMBER Beamline schematic, ALS web status, strip charts.
"""
import math, random
from collections import deque
from datetime import datetime, timedelta
from functools import partial

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QScrollArea, QFrame,
    QSizePolicy, QComboBox, QPushButton, QDialog, QLineEdit,
    QProgressBar, QMessageBox, QSplitter, QGridLayout, QGroupBox,
    QStackedWidget, QRadioButton, QDoubleSpinBox, QProgressBar,
)
from PySide6.QtCore import Qt, QTimer, Signal, QRect, QUrl, QMetaObject, Q_ARG, Slot
from PySide6.QtGui import (
    QPainter, QPen, QBrush, QColor, QFont,
    QLinearGradient, QCursor,
)

from common import (
    PAL, qc, COMBO_STYLE, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE,
    TRACE_COLORS, MPL_AVAILABLE, WEBENGINE_AVAILABLE, EPICS_AVAILABLE,
    PVMonitor, PVLabel,
)

if MPL_AVAILABLE:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates

if WEBENGINE_AVAILABLE:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings

def _lbl(text):
    l = QLabel(text)
    l.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
    return l

# ── Symbol widget ─────────────────────────────────────────────────────────────
SYM_W, SYM_H = 100, 80

class SymbolWidget(QWidget):
    def __init__(self, kind, parent=None):
        super().__init__(parent)
        self.kind = kind
        self.setFixedSize(SYM_W, SYM_H)
        self.setAttribute(Qt.WA_TransparentForMouseEvents)

    def paintEvent(self, _):
        p = QPainter(self); p.setRenderHint(QPainter.Antialiasing)
        w, h = self.width(), self.height(); by = h // 2
        p.setPen(QPen(qc("beam"), 2)); p.drawLine(0, by, w, by)
        fn = getattr(self, f"_draw_{self.kind}", None)
        if fn: fn(p, w, h, by)

    def _draw_undulator(self, p, w, h, by):
        n, pw, ph, gap = 5, 9, 15, 5
        x0 = (w - n*pw - (n-1)*gap) // 2
        for i in range(n):
            x = x0 + i*(pw+gap)
            c1 = qc("undulat") if i%2==0 else qc("warn")
            c2 = qc("warn")    if i%2==0 else qc("undulat")
            for clr, y0 in [(c1, by-ph-3), (c2, by+3)]:
                p.setBrush(QBrush(clr)); p.setPen(QPen(clr.darker(140),1))
                p.drawRect(x, y0, pw, ph)

    def _draw_mirror_h(self, p, w, h, by):
        p.save(); p.translate(w//2, by); p.rotate(-25)
        p.setBrush(QBrush(qc("mirror"))); p.setPen(QPen(qc("mirror").darker(150),1))
        p.drawRect(-5,-22,10,44); p.restore()

    def _draw_mirror_v(self, p, w, h, by):
        p.save(); p.translate(w//2, by); p.rotate(-15)
        p.setBrush(QBrush(qc("mirror"))); p.setPen(QPen(qc("mirror").darker(150),1))
        p.drawRect(-4,-24,8,48); p.restore()

    def _draw_diag(self, p, w, h, by):
        cx = w//2
        p.setBrush(QBrush(qc("diag"))); p.setPen(QPen(qc("diag").darker(140),1))
        p.drawRect(cx-7, 3, 14, by+10)
        p.setBrush(QBrush(qc("subtext"))); p.setPen(Qt.NoPen)
        p.drawRect(cx-3, 1, 6, 5)

    def _draw_mono(self, p, w, h, by):
        box = QRect(4, by-26, w-8, 52)
        p.setBrush(QBrush(qc("mono"))); p.setPen(QPen(qc("mono").darker(150),1))
        p.drawRoundedRect(box, 4, 4)
        mid = box.left() + box.width()//2
        p.drawLine(mid, box.top(), mid, box.bottom())
        p.setPen(QPen(QColor("#1a1a2e"),1))
        f = QFont("Sans Serif", 7, QFont.Bold); p.setFont(f)
        p.drawText(QRect(box.left(), by-26, box.width()//2, 52), Qt.AlignCenter, "M\n102")
        p.drawText(QRect(mid, by-26, box.width()//2, 52), Qt.AlignCenter, "G\n10x")

    def _draw_slit(self, p, w, h, by):
        jh, gap = 14, 8
        p.setBrush(QBrush(qc("aperture"))); p.setPen(QPen(qc("aperture").darker(150),1))
        p.drawRect(8, by-gap//2-jh, w-16, jh)
        p.drawRect(8, by+gap//2,    w-16, jh)

    def _draw_shutter(self, p, w, h, by):
        cx, r = w//2, 18
        p.setBrush(QBrush(qc("shutter"))); p.setPen(QPen(qc("shutter").darker(150),1))
        p.drawEllipse(cx-r, by-r, 2*r, 2*r)
        p.setBrush(QBrush(qc("bg"))); p.setPen(Qt.NoPen)
        p.drawRect(cx-r, by-3, 2*r, 6)
        p.setPen(QPen(qc("beam"),2)); p.drawLine(cx-r, by, cx+r, by)

    def _draw_aperture_h(self, p, w, h, by):
        jw, jh, gap = 10, h-12, 12; cx = w//2
        p.setBrush(QBrush(qc("aperture"))); p.setPen(QPen(qc("aperture").darker(150),1))
        p.drawRect(cx-gap//2-jw, 6, jw, jh)
        p.drawRect(cx+gap//2,    6, jw, jh)

    def _draw_aperture_v(self, p, w, h, by):
        jh, gap = 10, 12
        p.setBrush(QBrush(qc("aperture"))); p.setPen(QPen(qc("aperture").darker(150),1))
        p.drawRect(8, by-gap//2-jh, w-16, jh)
        p.drawRect(8, by+gap//2,    w-16, jh)

    def _draw_endstation(self, p, w, h, by):
        grad = QLinearGradient(0, by-28, 0, by+28)
        grad.setColorAt(0, QColor("#2a2a5e")); grad.setColorAt(1, QColor("#1a1a3e"))
        p.setBrush(QBrush(grad)); p.setPen(QPen(QColor("#5050a0"),1.5))
        p.drawRoundedRect(3, by-28, w-6, 56, 6, 6)
        p.setPen(QPen(qc("text"),1))
        p.setFont(QFont("Sans Serif", 7, QFont.Bold))
        p.drawText(QRect(3, by-28, w-6, 56), Qt.AlignCenter, "HiRRIXS")

# ── Strip Chart ───────────────────────────────────────────────────────────────
STRIP_HISTORY = 300

class StripChart(QWidget):
    def __init__(self, pv_map: dict, title: str = "", parent=None):
        super().__init__(parent)
        self._pv_map = pv_map
        self._traces: dict = {}
        self._dirty  = False

        root = QVBoxLayout(self)
        root.setContentsMargins(4,4,4,4); root.setSpacing(4)

        tb = QHBoxLayout(); tb.setSpacing(6)
        if title:
            tl = QLabel(title); tl.setFont(QFont("Sans Serif", 8, QFont.Bold))
            tl.setStyleSheet(f"color:{PAL['accent']};"); tb.addWidget(tl)
        self._add_combo = QComboBox(); self._add_combo.setStyleSheet(COMBO_STYLE)
        for lbl in pv_map: self._add_combo.addItem(lbl)
        tb.addWidget(self._add_combo)
        add_btn = QPushButton("＋ Add"); add_btn.setStyleSheet(BTN_STYLE)
        add_btn.setFixedWidth(64); add_btn.clicked.connect(self._add_trace)
        tb.addWidget(add_btn)
        clr_btn = QPushButton("Clear all"); clr_btn.setStyleSheet(BTN_STYLE)
        clr_btn.setFixedWidth(72); clr_btn.clicked.connect(self._clear_all)
        tb.addWidget(clr_btn); tb.addStretch()
        root.addLayout(tb)

        self._legend_widget = QWidget()
        self._legend_widget.setStyleSheet("background:transparent;")
        self._legend_layout = QVBoxLayout(self._legend_widget)
        self._legend_layout.setContentsMargins(0,0,0,0); self._legend_layout.setSpacing(2)
        root.addWidget(self._legend_widget)

        if MPL_AVAILABLE:
            self._fig    = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax     = self._fig.add_subplot(111)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            root.addWidget(self._canvas, 1)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            root.addWidget(ph, 1)

        PVMonitor().value_changed.connect(self._on_pv)

        # Coalescing redraw timer — redraws at configured rate, not on every PV update
        self._redraw_timer = QTimer(self)
        self._redraw_timer.setInterval(1000)          # default 1 s, matches config default
        self._redraw_timer.timeout.connect(self._redraw)
        self._redraw_timer.start()
        self._dirty = False                           # flag set by _on_pv

    # ── live-config hooks ──────────────────────────────────────────────────────
    def set_update_interval(self, ms: int):
        self._redraw_timer.setInterval(max(100, int(ms)))

    def set_history_length(self, seconds: int):
        """Resize all trace deques; existing data that still fits is preserved."""
        new_maxlen = max(10, int(seconds))
        for info in self._traces.values():
            info['times']  = deque(info['times'],  maxlen=new_maxlen)
            info['values'] = deque(info['values'], maxlen=new_maxlen)

    # ── internal ───────────────────────────────────────────────────────────────
    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.grid(True, color="#2a3a5e", linewidth=0.5, linestyle="--")
        ax.set_xlabel("Time",  color=PAL["subtext"], fontsize=8)
        ax.set_ylabel("Value", color=PAL["subtext"], fontsize=8)

    def _add_trace(self):
        label = self._add_combo.currentText()
        if label in self._traces: return
        pv    = self._pv_map.get(label, "")
        color = TRACE_COLORS[len(self._traces) % len(TRACE_COLORS)]
        line  = None
        if MPL_AVAILABLE:
            line, = self._ax.plot([], [], color=color, linewidth=1.2, label=label)
            self._ax.legend(facecolor=PAL["surface"], labelcolor=PAL["text"],
                            edgecolor="#2a3a5e", fontsize=7)
            self._canvas.draw_idle()
        self._traces[label] = {
            "pv": pv, "color": color, "line": line,
            "times":  deque(maxlen=STRIP_HISTORY),
            "values": deque(maxlen=STRIP_HISTORY),
        }
        PVMonitor().subscribe(pv)
        self._add_legend_row(label, color)

    def _remove_trace(self, label):
        t = self._traces.pop(label, None)
        if t and t["line"] and MPL_AVAILABLE:
            t["line"].remove()
            self._ax.legend(facecolor=PAL["surface"], labelcolor=PAL["text"],
                            edgecolor="#2a3a5e", fontsize=7)
            self._canvas.draw_idle()
        row = self._legend_widget.findChild(QWidget, f"row_{label}")
        if row:
            self._legend_layout.removeWidget(row); row.deleteLater()

    def _add_legend_row(self, label, color):
        row = QWidget(); row.setObjectName(f"row_{label}")
        row.setStyleSheet("background:transparent;")
        hl = QHBoxLayout(row); hl.setContentsMargins(0,0,0,0); hl.setSpacing(4)
        swatch = QLabel("  "); swatch.setFixedWidth(18)
        swatch.setStyleSheet(f"background:{color}; border-radius:2px;")
        hl.addWidget(swatch)
        nl = QLabel(label); nl.setFont(QFont("Sans Serif",7))
        nl.setStyleSheet(f"color:{PAL['text']};")
        hl.addWidget(nl); hl.addStretch()
        rm = QPushButton("✕"); rm.setFixedSize(18,18)
        rm.setStyleSheet(f"QPushButton{{background:transparent;color:{PAL['nc']};"
                         f"border:none;font-size:10px;padding:0;}}"
                         f"QPushButton:hover{{color:#ff6666;}}")
        rm.clicked.connect(lambda: self._remove_trace(label))
        hl.addWidget(rm)
        self._legend_layout.addWidget(row)

    def _on_pv(self, name, value):
        if value is None: return
        try: fv = float(value)
        except: return
        now = datetime.now()
        for t in self._traces.values():
            if t["pv"] == name:
                t["times"].append(now); t["values"].append(fv)
                if MPL_AVAILABLE and t["line"]:
                    t["line"].set_data(list(t["times"]), list(t["values"]))
                self._dirty = True

    def _redraw(self):
        if not self._dirty: return
        if not MPL_AVAILABLE or not self._traces: return
        all_y = [v for t in self._traces.values() for v in t["values"]]
        all_x = [v for t in self._traces.values() for v in t["times"]]
        if not all_y: return
        ymin, ymax = min(all_y), max(all_y)
        ypad = (ymax - ymin) * 0.05 if ymax != ymin else 0.5
        self._ax.set_ylim(ymin - ypad, ymax + ypad)
        if all_x:
            xmin, xmax = min(all_x), max(all_x)
            xpad = (xmax - xmin) * 0.05 if xmax != xmin else timedelta(seconds=1)
            self._ax.set_xlim(xmin - xpad, xmax + xpad)
        self._ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self._fig.autofmt_xdate(rotation=30, ha="right")
        self._canvas.draw_idle()
        self._dirty = False

    def _clear_all(self):
        for t in self._traces.values():
            t["times"].clear(); t["values"].clear()
            if MPL_AVAILABLE and t["line"]: t["line"].set_data([], [])
        if MPL_AVAILABLE: self._canvas.draw_idle()
        self._dirty = False

# ── Scan Window ───────────────────────────────────────────────────────────────
SCAN_DWELL_MS = 300

class ScanWindow(QDialog):
    """
    Modal dialog for scanning or jogging a motor while monitoring a signal.

    Two modes selectable via radio buttons:
      Scan       — sweep motor start→stop in N steps, plot signal vs position
      Stripchart — jog motor ±step, live time-series + scatter plot
    """
    def __init__(self, title: str, motor_pvs: dict, signal_pvs: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Scan / Stripchart — {title}")
        self.resize(700, 600)
        self.setStyleSheet(f"background:{PAL['bg']}; color:{PAL['text']};")

        self._motor_pvs  = motor_pvs
        self._signal_pvs = signal_pvs
        self._scanning   = False
        self._scan_thread = None

        # stripchart state
        self._sc_times:  deque = deque(maxlen=200)
        self._sc_signal: deque = deque(maxlen=200)
        self._sc_pos:    deque = deque(maxlen=200)
        self._sc_sig2:   deque = deque(maxlen=200)
        self._t0 = None
        self._last_pos = None
        self._last_sig = None

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        # ── mode selector ─────────────────────────────────────────────────────
        mode_row = QHBoxLayout()
        self._rb_scan = QRadioButton("Scan")
        self._rb_sc   = QRadioButton("Stripchart")
        self._rb_scan.setChecked(True)
        for rb in (self._rb_scan, self._rb_sc):
            rb.setStyleSheet(f"color:{PAL['text']}; font-size:9pt;")
        mode_row.addWidget(self._rb_scan)
        mode_row.addWidget(self._rb_sc)
        mode_row.addStretch()
        root.addLayout(mode_row)

        # ── shared motor / signal combos ──────────────────────────────────────
        combo_row = QHBoxLayout(); combo_row.setSpacing(12)
        combo_row.addWidget(_lbl("Motor:"))
        self._motor_combo = QComboBox(); self._motor_combo.setStyleSheet(COMBO_STYLE)
        for n in motor_pvs: self._motor_combo.addItem(n)
        combo_row.addWidget(self._motor_combo)
        combo_row.addSpacing(12)
        combo_row.addWidget(_lbl("Signal:"))
        self._sig_combo = QComboBox(); self._sig_combo.setStyleSheet(COMBO_STYLE)
        for n in signal_pvs: self._sig_combo.addItem(n)
        combo_row.addWidget(self._sig_combo)
        combo_row.addStretch()
        root.addLayout(combo_row)

        # ── stacked panels ────────────────────────────────────────────────────
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_scan_panel())
        self._stack.addWidget(self._build_sc_panel())
        root.addWidget(self._stack, 1)

        self._rb_scan.toggled.connect(lambda on: self._stack.setCurrentIndex(0) if on else None)
        self._rb_sc.toggled.connect(lambda on: self._stack.setCurrentIndex(1) if on else None)

        # ── PV monitor ────────────────────────────────────────────────────────
        self._mon = PVMonitor()
        self._mon.value_changed.connect(self._on_pv)
        for pv in list(motor_pvs.values()) + list(signal_pvs.values()):
            self._mon.subscribe(pv)

        # motor combo change → re-subscribe RBV
        self._motor_combo.currentTextChanged.connect(self._on_motor_changed)
        self._sig_combo.currentTextChanged.connect(self._on_sig_changed)
        self._on_motor_changed(self._motor_combo.currentText())
        self._on_sig_changed(self._sig_combo.currentText())

    # ── helpers ───────────────────────────────────────────────────────────────
    def _motor_pv(self):
        return self._motor_pvs.get(self._motor_combo.currentText(), "")

    def _sig_pv(self):
        return self._signal_pvs.get(self._sig_combo.currentText(), "")

    # ── scan panel ────────────────────────────────────────────────────────────
    def _build_scan_panel(self):
        w = QWidget(); w.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(w); vl.setContentsMargins(0,0,0,0); vl.setSpacing(6)

        param_row = QHBoxLayout(); param_row.setSpacing(8)
        self._start = QDoubleSpinBox(); self._start.setRange(-1e6,1e6); self._start.setValue(0)
        self._stop  = QDoubleSpinBox(); self._stop.setRange(-1e6,1e6);  self._stop.setValue(1)
        self._step  = QDoubleSpinBox(); self._step.setRange(1e-6,1e6);  self._step.setValue(0.1)
        for sb, lbl in ((self._start,"Start:"), (self._stop,"Stop:"), (self._step,"Step:")):
            sb.setStyleSheet(INPUT_STYLE); sb.setFixedWidth(100)
            param_row.addWidget(_lbl(lbl)); param_row.addWidget(sb)
        self._prog = QProgressBar(); self._prog.setValue(0)
        self._prog.setStyleSheet(
            f"QProgressBar{{background:{PAL['surface']}; border:1px solid #2a3a5e;"
            f" border-radius:3px; color:{PAL['text']};}}"
            f"QProgressBar::chunk{{background:{PAL['accent']};}}")
        param_row.addWidget(self._prog, 1)
        vl.addLayout(param_row)

        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        self._scan_btn  = QPushButton("▶ Scan");  self._scan_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn  = QPushButton("■ Stop");  self._stop_btn.setStyleSheet(BTN_STYLE)
        self._clear_btn = QPushButton("Clear");   self._clear_btn.setStyleSheet(BTN_STYLE)
        self._done_btn  = QPushButton("Done");    self._done_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn.setEnabled(False)
        for b in (self._scan_btn, self._stop_btn, self._clear_btn, self._done_btn):
            b.setFixedWidth(80); btn_row.addWidget(b)
        btn_row.addStretch()
        self._scan_status = QLabel("Ready")
        self._scan_status.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
        btn_row.addWidget(self._scan_status)
        vl.addLayout(btn_row)

        if MPL_AVAILABLE:
            self._scan_fig = Figure(facecolor=PAL["bg"], tight_layout=True)
            self._scan_ax  = self._scan_fig.add_subplot(111)
            self._scan_line, = self._scan_ax.plot([], [], color=PAL["accent"],
                                                   marker="o", markersize=4, linewidth=1)
            self._style_ax(self._scan_ax, "Motor position", "Signal")
            self._scan_canvas = FigureCanvas(self._scan_fig)
            self._scan_canvas.setStyleSheet("background:transparent;")
            vl.addWidget(self._scan_canvas, 1)

        self._scan_btn.clicked.connect(self._start_scan)
        self._stop_btn.clicked.connect(self._stop_scan)
        self._clear_btn.clicked.connect(self._clear_scan)
        self._done_btn.clicked.connect(self.accept)
        return w

    # ── stripchart panel ──────────────────────────────────────────────────────
    def _build_sc_panel(self):
        w = QWidget(); w.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(w); vl.setContentsMargins(0,0,0,0); vl.setSpacing(6)

        ctrl_row = QHBoxLayout(); ctrl_row.setSpacing(8)
        ctrl_row.addWidget(_lbl("Step size:"))
        self._jog_step = QDoubleSpinBox()
        self._jog_step.setRange(1e-6, 1e6); self._jog_step.setValue(0.1)
        self._jog_step.setDecimals(4); self._jog_step.setStyleSheet(INPUT_STYLE)
        self._jog_step.setFixedWidth(100)
        ctrl_row.addWidget(self._jog_step)

        ctrl_row.addSpacing(8)
        self._rbv_lbl = QLabel("RBV: —")
        self._rbv_lbl.setStyleSheet(
            f"color:{PAL['ok']}; font-family:monospace; font-size:9pt;")
        ctrl_row.addWidget(self._rbv_lbl)

        ctrl_row.addSpacing(8)
        self._up_btn   = QPushButton("▲ Step Up")
        self._down_btn = QPushButton("▼ Step Down")
        self._sc_clear = QPushButton("Clear")
        self._sc_done  = QPushButton("Done")
        for b in (self._up_btn, self._down_btn, self._sc_clear, self._sc_done):
            b.setStyleSheet(BTN_STYLE); b.setFixedWidth(100)
            ctrl_row.addWidget(b)
        ctrl_row.addStretch()
        vl.addLayout(ctrl_row)

        if MPL_AVAILABLE:
            splitter = QSplitter(Qt.Vertical)
            splitter.setStyleSheet(SPLITTER_STYLE)

            # time-series strip
            self._ts_fig = Figure(facecolor=PAL["bg"], tight_layout=True)
            self._ts_ax  = self._ts_fig.add_subplot(111)
            self._ts_line, = self._ts_ax.plot([], [], color="#4fc3f7",
                                               linewidth=1.2, marker=".", markersize=3)
            self._style_ax(self._ts_ax, "Time (s)", "Signal")
            self._ts_canvas = FigureCanvas(self._ts_fig)
            self._ts_canvas.setStyleSheet("background:transparent;")
            splitter.addWidget(self._ts_canvas)

            # scatter: signal vs motor position
            self._sc_fig = Figure(facecolor=PAL["bg"], tight_layout=True)
            self._sc_ax  = self._sc_fig.add_subplot(111)
            self._sc_scat, = self._sc_ax.plot([], [], color=PAL["accent"],
                                               linestyle="none", marker="o", markersize=4)
            self._style_ax(self._sc_ax, "Motor position", "Signal")
            self._sc_canvas = FigureCanvas(self._sc_fig)
            self._sc_canvas.setStyleSheet("background:transparent;")
            splitter.addWidget(self._sc_canvas)

            splitter.setSizes([250, 250])
            vl.addWidget(splitter, 1)

        self._up_btn.clicked.connect(lambda: self._jog(+1))
        self._down_btn.clicked.connect(lambda: self._jog(-1))
        self._sc_clear.clicked.connect(self._clear_sc)
        self._sc_done.clicked.connect(self.accept)
        return w

    # ── shared axis style ─────────────────────────────────────────────────────
    def _style_ax(self, ax, xlabel, ylabel):
        ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.set_xlabel(xlabel, color=PAL["subtext"], fontsize=8)
        ax.set_ylabel(ylabel, color=PAL["subtext"], fontsize=8)
        ax.grid(True, color="#2a3a5e", linewidth=0.5, linestyle="--")

    # ── PV callbacks ──────────────────────────────────────────────────────────
    def _on_motor_changed(self, name):
        self._current_motor_pv = self._motor_pvs.get(name, "")

    def _on_sig_changed(self, name):
        self._current_sig_pv = self._signal_pvs.get(name, "")

    def _on_pv(self, name, value):
        if value is None: return
        try: fv = float(value)
        except: return

        # update RBV label
        if name == self._current_motor_pv:
            self._last_pos = fv
            self._rbv_lbl.setText(f"RBV: {fv:.6g}")

        # stripchart update
        if self._rb_sc.isChecked() and MPL_AVAILABLE:
            if name == self._current_sig_pv:
                self._last_sig = fv
                if self._t0 is None: self._t0 = __import__("time").monotonic()
                t = __import__("time").monotonic() - self._t0
                self._sc_times.append(t)
                self._sc_signal.append(fv)
                self._ts_line.set_data(list(self._sc_times), list(self._sc_signal))
                self._ts_ax.relim(); self._ts_ax.autoscale_view()
                self._ts_canvas.draw_idle()

            if name in (self._current_motor_pv, self._current_sig_pv):
                if self._last_pos is not None and self._last_sig is not None:
                    self._sc_pos.append(self._last_pos)
                    self._sc_sig2.append(self._last_sig)
                    self._sc_scat.set_data(list(self._sc_pos), list(self._sc_sig2))
                    self._sc_ax.relim(); self._sc_ax.autoscale_view()
                    self._sc_canvas.draw_idle()

    # ── scan logic ────────────────────────────────────────────────────────────
    def _start_scan(self):
        if self._scanning: return
        mpv = self._motor_pv(); spv = self._sig_pv()
        if not mpv or not spv:
            self._scan_status.setText("⚠ Select motor and signal"); return
        start = self._start.value(); stop = self._stop.value()
        step  = self._step.value()
        if step <= 0 or start == stop:
            self._scan_status.setText("⚠ Invalid range"); return
        import numpy as np
        positions = np.arange(start, stop + step * 0.5, step)
        self._scanning = True
        self._scan_btn.setEnabled(False); self._stop_btn.setEnabled(True)
        self._clear_scan()
        self._scan_xs, self._scan_ys = [], []
        self._prog.setMaximum(len(positions)); self._prog.setValue(0)

        import threading
        self._scan_thread = threading.Thread(
            target=self._scan_worker, args=(mpv, spv, positions), daemon=True)
        self._scan_thread.start()

    def _scan_worker(self, mpv, spv, positions):
        import epics, time
        for i, pos in enumerate(positions):
            if not self._scanning: break
            try: epics.caput(mpv, pos, wait=True)
            except: pass
            time.sleep(0.05)
            val = self._mon.get(spv)
            if val is not None:
                QMetaObject.invokeMethod(
                    self, "_scan_point", Qt.QueuedConnection,
                    Q_ARG(float, float(pos)), Q_ARG(float, float(val)),
                    Q_ARG(int, i+1))
        QMetaObject.invokeMethod(self, "_scan_done", Qt.QueuedConnection)

    @Slot(float, float, int)
    def _scan_point(self, pos, val, n):
        self._scan_xs.append(pos); self._scan_ys.append(val)
        if MPL_AVAILABLE:
            self._scan_line.set_data(self._scan_xs, self._scan_ys)
            self._scan_ax.relim(); self._scan_ax.autoscale_view()
            self._scan_canvas.draw_idle()
        self._prog.setValue(n)
        self._scan_status.setText(f"Point {n}/{self._prog.maximum()}  pos={pos:.4g}  sig={val:.4g}")

    @Slot()
    def _scan_done(self):
        self._scanning = False
        self._scan_btn.setEnabled(True); self._stop_btn.setEnabled(False)
        self._scan_status.setText("Done")

    def _stop_scan(self):
        self._scanning = False

    def _clear_scan(self):
        self._scan_xs, self._scan_ys = [], []
        if MPL_AVAILABLE:
            self._scan_line.set_data([], [])
            self._scan_ax.relim(); self._scan_canvas.draw_idle()
        self._prog.setValue(0); self._scan_status.setText("Ready")

    # ── jog logic ─────────────────────────────────────────────────────────────
    def _jog(self, direction: int):
        mpv = self._motor_pv()
        if not mpv: return
        cur = self._last_pos
        if cur is None:
            cur = self._mon.get(mpv)
        if cur is None:
            self._rbv_lbl.setText("RBV: N/C"); return
        target = cur + direction * self._jog_step.value()
        try:
            import epics; epics.caput(mpv, target)
        except ImportError:
            print(f"[SIM] caput {mpv} = {target}")

    def _clear_sc(self):
        self._sc_times.clear(); self._sc_signal.clear()
        self._sc_pos.clear();   self._sc_sig2.clear()
        self._t0 = None; self._last_pos = None; self._last_sig = None
        if MPL_AVAILABLE:
            self._ts_line.set_data([], [])
            self._ts_ax.relim(); self._ts_canvas.draw_idle()
            self._sc_scat.set_data([], [])
            self._sc_ax.relim(); self._sc_canvas.draw_idle()
        self._rbv_lbl.setText("RBV: —")

# ── Component Card ────────────────────────────────────────────────────────────
CARD_W = 118

class ComponentCard(QFrame):
    clicked = Signal()

    def __init__(self, name, kind, pvs, parent=None):
        super().__init__(parent)
        self.setFixedWidth(CARD_W)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.MinimumExpanding)
        self.setCursor(QCursor(Qt.PointingHandCursor))
        self._base  = "QFrame{background:#16213e;border:1px solid #2a3a5e;border-radius:6px;}QLabel{background:transparent;border:none;}"
        self._hover = "QFrame{background:#1e3a5e;border:1px solid #4fc3f7;border-radius:6px;}QLabel{background:transparent;border:none;}"
        self.setStyleSheet(self._base)
        vl = QVBoxLayout(self); vl.setContentsMargins(5,6,5,6); vl.setSpacing(2)
        vl.addWidget(SymbolWidget(kind), alignment=Qt.AlignHCenter)
        nl = QLabel(name); nl.setAlignment(Qt.AlignCenter)
        nl.setFont(QFont("Sans Serif",8,QFont.Bold))
        nl.setStyleSheet("color:#e0e0e0;"); nl.setWordWrap(True); vl.addWidget(nl)
        hint = QLabel("🔍 click to scan"); hint.setAlignment(Qt.AlignCenter)
        hint.setFont(QFont("Sans Serif",6))
        hint.setStyleSheet(f"color:{PAL['subtext']};"); vl.addWidget(hint)
        sep = QFrame(); sep.setFrameShape(QFrame.HLine)
        sep.setFixedHeight(1); sep.setStyleSheet("background:#2a3a5e;border:none;")
        vl.addWidget(sep)
        for lbl, pv in pvs.items():
            if not pv: continue
            row = QWidget(); row.setStyleSheet("background:transparent;")
            rl  = QHBoxLayout(row); rl.setContentsMargins(0,0,0,0); rl.setSpacing(2)
            kl = QLabel(lbl); kl.setFont(QFont("Sans Serif",7))
            kl.setStyleSheet("color:#9e9e9e;"); rl.addWidget(kl); rl.addStretch()
            vvl = PVLabel(pv); vvl.setFont(QFont("Monospace",7))
            rl.addWidget(vvl); vl.addWidget(row)
        vl.addStretch()

    def enterEvent(self, e): self.setStyleSheet(self._hover); super().enterEvent(e)
    def leaveEvent(self, e): self.setStyleSheet(self._base);  super().leaveEvent(e)
    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton: self.clicked.emit()
        super().mousePressEvent(e)

# ── Beam connector ────────────────────────────────────────────────────────────
class BeamConnector(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent); self.setFixedSize(14, SYM_H)
    def paintEvent(self, _):
        p = QPainter(self); p.setPen(QPen(qc("beam"),2))
        p.drawLine(0, self.height()//2, self.width(), self.height()//2)

# ── Beamline Tab ──────────────────────────────────────────────────────────────
class BeamlineTab(QWidget):
    def __init__(self, amber_cfg, all_signals, all_pvs, parent=None):
        super().__init__(parent)
        self._all_signals = all_signals
        self.setStyleSheet(f"background:{PAL['bg']};")
        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0); outer.setSpacing(0)

        hdr = QLabel("  🔬  AMBER Beamline — BL601   ·   upstream → downstream   ·   click a component to scan")
        hdr.setFont(QFont("Sans Serif",9,QFont.Bold))
        hdr.setStyleSheet(f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        outer.addWidget(hdr)

        vsplit = QSplitter(Qt.Vertical); vsplit.setStyleSheet(SPLITTER_STYLE)

        scroll = QScrollArea()
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setWidgetResizable(False)
        scroll.setStyleSheet(f"background:{PAL['bg']}; border:none;")
        content = QWidget(); content.setStyleSheet(f"background:{PAL['bg']};")
        hl = QHBoxLayout(content); hl.setContentsMargins(16,24,16,8); hl.setSpacing(0)
        cards = self._build_cards(amber_cfg)
        for i, card in enumerate(cards):
            hl.addWidget(card)
            if i < len(cards)-1: hl.addWidget(BeamConnector())
        hl.addStretch()
        scroll.setWidget(content)
        vsplit.addWidget(scroll)

        hsplit = QSplitter(Qt.Horizontal); hsplit.setStyleSheet(SPLITTER_STYLE)

        if WEBENGINE_AVAILABLE:
            class _ScalingWebView(QWebEngineView):
                def __init__(self, parent=None):
                    super().__init__(parent)
                    self._content_w = 0.0
                    self.settings().setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, True)
                    self.settings().setAttribute(QWebEngineSettings.WebAttribute.AutoLoadImages, True)
                    self.settings().setAttribute(QWebEngineSettings.WebAttribute.ScrollAnimatorEnabled, False)
                    self.page().setBackgroundColor(QColor(PAL["bg"]))
                    self.loadFinished.connect(self._on_loaded)

                def _on_loaded(self, _ok):
                    self._try_measure(0)

                def _try_measure(self, attempt):
                    def _cb(w):
                        if (not w) and attempt < 10:
                            t = QTimer(self); t.setSingleShot(True)
                            t.timeout.connect(lambda: self._try_measure(attempt+1))
                            t.start(300); return
                        if w and w > 0:
                            self.setZoomFactor(1.0)
                            self._content_w = float(w)
                            self._apply_zoom()
                    self.setZoomFactor(1.0)
                    self.page().runJavaScript("""
                        (function() {
                            if (!document.body) return 0;
                            var id = '__amb_reset__';
                            if (!document.getElementById(id)) {
                                var st = document.createElement('style');
                                st.id = id;
                                st.textContent =
                                    'html, body { margin:0!important; padding:0!important;' +
                                    '  background:#1a1a2e!important;' +
                                    '  width:max-content!important; }';
                                document.head.appendChild(st);
                            }
                            var max = 0;
                            Array.from(document.body.children).forEach(function(el) {
                                var r = el.getBoundingClientRect();
                                if (r.width > max) max = r.width;
                            });
                            return max || document.body.scrollWidth;
                        })()
                    """, _cb)

                def _apply_zoom(self):
                    if self._content_w <= 0: return
                    vw = self.width()
                    if vw > 0: self.setZoomFactor(vw / self._content_w)

                def resizeEvent(self, ev):
                    super().resizeEvent(ev); self._apply_zoom()

            web = _ScalingWebView()
            web.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            web.load(QUrl("https://controls.als.lbl.gov/als-beamstatus/site/alsstatus_alsweb"))
            _reload_timer = QTimer(self)
            _reload_timer.timeout.connect(web.reload)
            _reload_timer.start(30_000)
            hsplit.addWidget(web)
        else:
            ph = QLabel("PySide6-WebEngine not installed\nALS beam status unavailable")
            ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            hsplit.addWidget(ph)

        chart_split = QSplitter(Qt.Vertical); chart_split.setStyleSheet(SPLITTER_STYLE)
        self._strip1 = StripChart(all_pvs, "Strip Chart 1")
        self._strip2 = StripChart(all_pvs, "Strip Chart 2")
        chart_split.addWidget(self._strip1)
        chart_split.addWidget(self._strip2)
        chart_split.setSizes([1,1])
        hsplit.addWidget(chart_split)
        hsplit.setSizes([600, 400])

        vsplit.addWidget(hsplit)
        vsplit.setSizes([200, 700])
        outer.addWidget(vsplit, 1)

    def apply_config(self, key: str, value):
        """
        Slot wired to ConfigurationTab.config_changed(key, value).
        Handles every 'ui.*' key that affects the beamline tab.

        Parameters
        ----------
        key   : dotted config path, e.g. "ui.strip_chart_update_ms"
        value : new Python value (already coerced to the correct type)
        """
        if key == "ui.strip_chart_update_ms":
            ms = max(100, int(value))
            for sc in self._strip_charts():
                sc.set_update_interval(ms)

        elif key == "ui.strip_chart_history_s":
            secs = max(10, int(value))
            for sc in self._strip_charts():
                sc.set_history_length(secs)

    def _strip_charts(self):
        """Return all StripChart instances owned by this tab."""
        # Adjust attribute names to match your actual implementation:
        charts = []
        for attr in ("_strip1", "_strip2"):
            sc = getattr(self, attr, None)
            if sc is not None:
                charts.append(sc)
        return charts

    def _open_scan(self, comp_name, motor_pvs):
        dlg = ScanWindow(comp_name, motor_pvs, self._all_signals, self)
        dlg.exec()

    def _build_cards(self, cfg):
        m = cfg.get("motor",{}); cards = []

        def flatten(entry):
            if isinstance(entry, dict):
                return {k: v for k,v in entry.items() if isinstance(v,str)}
            if isinstance(entry, str) and entry: return {"Pos": entry}
            return {}

        def add(name, kind, pvs, motor_pvs=None):
            scan_motors = motor_pvs if motor_pvs is not None else pvs
            card = ComponentCard(name, kind, pvs)
            card.clicked.connect(partial(self._open_scan, name, scan_motors))
            cards.append(card)

        ivid = flatten(m.get("IVID",{}))
        add("IVID\nUndulator",   "undulator",  ivid, ivid)
        m101 = m.get("M101",{})
        add("M101\n(H-Mirror)",  "mirror_h",
            {"Pitch": m101.get("M101Pitch",""), "Roll": m101.get("M101Roll","")})
        add("DIAG101",           "diag",       {"Pos": m.get("DIAG101","")})
        add("Mono\nM102+G10x",   "mono",
            {"M102": m.get("M102",""), "G10x": m.get("G10x",""),
             "Energy": m.get("BeamlineEnergy","")},
            {"M102": m.get("M102",""), "G10x": m.get("G10x","")})
        m131 = m.get("M131",{})
        add("M131\n(H-Mirror)",  "mirror_h",
            {"Pitch": m131.get("M131Pitch",""), "Roll": m131.get("M131Roll","")})
        add("SLIT131\n(Exit Slit)","slit",     {"V-Size": m.get("SLIT","")})
        shtr = m.get("SHTR131",{})
        add("SHTR131\n(Shutter)", "shutter",
            {"Pos": shtr.get("SHTR131Pos",""), "PZT": shtr.get("SHTR131PZT","")})
        add("DIAG132",           "diag",       {"Pos": m.get("DIAG132","")})
        ap131 = m.get("AP131",{})
        add("AP131\n(H-Apert.)", "aperture_h",
            {"Pos": ap131.get("AP131Pos",""), "Size": ap131.get("AP131Size","")})
        ap132 = m.get("AP132",{})
        add("AP132\n(V-Apert.)", "aperture_v",
            {"Pos": ap132.get("AP132Pos",""), "Size": ap132.get("AP132Size","")})
        add("M132\n(H-Mirror)",  "mirror_h",   {"Pitch": m.get("M132","")})
        add("M133\n(V-Mirror)",  "mirror_v",   {"Pitch": m.get("M133","")})
        add("DIAG133",           "diag",       {"Pos": m.get("DIAG133","")})
        add("DIAG134",           "diag",       {"Pos": m.get("DIAG134","")})
        add("HiRRIXS\nEndstation","endstation",{}, {})
        return cards
