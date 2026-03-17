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
)
from PySide6.QtCore import Qt, QTimer, Signal, QRect, QUrl
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

    def _redraw(self):
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

    def _on_pv(self, name, value):
        if value is None: return
        try: fv = float(value)
        except: return
        now = datetime.now(); matched = False
        for t in self._traces.values():
            if t["pv"] == name:
                t["times"].append(now); t["values"].append(fv)
                if MPL_AVAILABLE and t["line"]:
                    t["line"].set_data(list(t["times"]), list(t["values"]))
                matched = True
        if matched: self._redraw()

    def _clear_all(self):
        for t in self._traces.values():
            t["times"].clear(); t["values"].clear()
            if MPL_AVAILABLE and t["line"]: t["line"].set_data([], [])
        if MPL_AVAILABLE: self._canvas.draw_idle()

# ── Scan Window ───────────────────────────────────────────────────────────────
SCAN_DWELL_MS = 300

class ScanWindow(QDialog):
    def __init__(self, comp_name, motor_pvs, all_signals, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Scan — {comp_name}")
        self.resize(700, 540)
        self.setStyleSheet(f"background:{PAL['bg']}; color:{PAL['text']};")
        self._motor_pvs = motor_pvs; self._all_signals = all_signals
        self._scan_xs: list = []; self._scan_ys: list = []
        self._scan_idx = 0; self._scanning = False
        self._scan_timer = QTimer(self); self._scan_timer.timeout.connect(self._scan_step)

        root = QVBoxLayout(self); root.setContentsMargins(12,12,12,12); root.setSpacing(10)
        ctrl = QGroupBox(f"Scan setup — {comp_name}"); ctrl.setStyleSheet(GRP_STYLE)
        cl = QGridLayout(ctrl); cl.setContentsMargins(10,20,10,10); cl.setSpacing(8)

        def qlbl(t):
            l = QLabel(t); l.setStyleSheet(f"color:{PAL['subtext']};"); return l

        cl.addWidget(qlbl("Motor (x-axis)"), 0, 0)
        self._motor_combo = QComboBox(); self._motor_combo.setStyleSheet(COMBO_STYLE)
        for lbl in motor_pvs: self._motor_combo.addItem(lbl)
        self._motor_combo.setEnabled(len(motor_pvs) > 1)
        cl.addWidget(self._motor_combo, 0, 1, 1, 3)

        cl.addWidget(qlbl("Signal (y-axis)"), 1, 0)
        self._sig_combo = QComboBox(); self._sig_combo.setStyleSheet(COMBO_STYLE)
        for lbl in all_signals: self._sig_combo.addItem(lbl)
        cl.addWidget(self._sig_combo, 1, 1, 1, 3)

        for col, (lbl_txt, attr, default) in enumerate([
            ("Start","_inp_start","0.0"),
            ("Stop", "_inp_stop", "1.0"),
            ("Step", "_inp_step", "0.1"),
        ]):
            cl.addWidget(qlbl(lbl_txt), 2, col*2)
            inp = QLineEdit(default); inp.setStyleSheet(INPUT_STYLE); inp.setFixedWidth(90)
            setattr(self, attr, inp); cl.addWidget(inp, 2, col*2+1)
        root.addWidget(ctrl)

        if MPL_AVAILABLE:
            self._fig  = Figure(facecolor=PAL["surface"], tight_layout=True)
            self._ax   = self._fig.add_subplot(111)
            self._line, = self._ax.plot([], [], color=PAL["accent"],
                                        linewidth=1.4, marker=".", markersize=4)
            self._style_ax()
            self._canvas = FigureCanvas(self._fig)
            self._canvas.setStyleSheet("background:transparent;")
            root.addWidget(self._canvas, 1)
        else:
            ph = QLabel("matplotlib not installed"); ph.setAlignment(Qt.AlignCenter)
            ph.setStyleSheet(f"color:{PAL['subtext']}; background:{PAL['surface']};")
            root.addWidget(ph, 1)

        self._progress = QProgressBar(); self._progress.setValue(0)
        self._progress.setTextVisible(False); self._progress.setFixedHeight(6)
        self._progress.setStyleSheet(f"""
            QProgressBar {{ background:{PAL['surface']}; border:none; border-radius:3px; }}
            QProgressBar::chunk {{ background:{PAL['accent']}; border-radius:3px; }}
        """)
        root.addWidget(self._progress)

        btn_row = QHBoxLayout()
        self._scan_btn = QPushButton("▶  Scan"); self._scan_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn = QPushButton("■  Stop"); self._stop_btn.setStyleSheet(BTN_STYLE)
        self._stop_btn.setEnabled(False)
        self._clr_btn  = QPushButton("Clear");   self._clr_btn.setStyleSheet(BTN_STYLE)
        done_btn       = QPushButton("Done");     done_btn.setStyleSheet(BTN_STYLE)
        self._scan_btn.clicked.connect(self._start_scan)
        self._stop_btn.clicked.connect(self._stop_scan)
        self._clr_btn.clicked.connect(self._clear_plot)
        done_btn.clicked.connect(self.accept)
        for b in (self._scan_btn, self._stop_btn, self._clr_btn, done_btn):
            btn_row.addWidget(b)
        root.addLayout(btn_row)

    def _style_ax(self):
        ax = self._ax; ax.set_facecolor(PAL["bg"])
        for sp in ax.spines.values(): sp.set_color("#2a3a5e")
        ax.tick_params(colors=PAL["subtext"], labelsize=7)
        ax.grid(True, color="#2a3a5e", linewidth=0.5, linestyle="--")

    def _refresh_labels(self):
        if not MPL_AVAILABLE: return
        mx = self._motor_combo.currentText(); sy = self._sig_combo.currentText()
        self._ax.set_xlabel(f"{mx}  [{self._motor_pvs.get(mx,'')}]",
                            color=PAL["subtext"], fontsize=8)
        self._ax.set_ylabel(sy, color=PAL["subtext"], fontsize=8)
        self._ax.set_title(f"{sy}  vs  {mx}", color=PAL["text"], fontsize=9)
        self._canvas.draw_idle()

    def _clear_plot(self):
        self._scan_xs.clear(); self._scan_ys.clear()
        if MPL_AVAILABLE:
            self._line.set_data([], []); self._ax.relim(); self._canvas.draw_idle()
        self._progress.setValue(0)

    def _parse_inputs(self):
        try:
            start = float(self._inp_start.text())
            stop  = float(self._inp_stop.text())
            step  = float(self._inp_step.text())
            if step == 0 or (stop-start)/step < 0: raise ValueError("invalid range")
            return start, stop, step, max(1, round(abs((stop-start)/step))+1)
        except ValueError as e:
            QMessageBox.warning(self, "Input error", f"Bad scan parameters:\n{e}")
            return None

    def _start_scan(self):
        parsed = self._parse_inputs()
        if parsed is None: return
        start, stop, step, n = parsed
        self._clear_plot()
        self._positions = [start + i*abs(step)*(1 if stop>=start else -1) for i in range(n)]
        self._scan_n = n; self._scan_idx = 0; self._scanning = True
        self._progress.setMaximum(n); self._refresh_labels()
        self._scan_btn.setEnabled(False); self._stop_btn.setEnabled(True)
        self._scan_timer.start(SCAN_DWELL_MS)

    def _scan_step(self):
        if not self._scanning or self._scan_idx >= len(self._positions):
            self._finish_scan(); return
        x_pos    = self._positions[self._scan_idx]
        motor_pv = self._motor_pvs.get(self._motor_combo.currentText(),"")
        sig_pv   = self._all_signals.get(self._sig_combo.currentText(),"")
        mon = PVMonitor()
        if motor_pv: mon.put(motor_pv, x_pos)
        if EPICS_AVAILABLE and sig_pv:
            raw = mon.get(sig_pv); y = float(raw) if raw is not None else float("nan")
        else:
            mid = (self._positions[0]+self._positions[-1])/2
            rng = abs(self._positions[-1]-self._positions[0]) or 1
            y = math.exp(-0.5*((x_pos-mid)/(rng*0.2))**2) + random.gauss(0,0.05)
        self._scan_xs.append(x_pos); self._scan_ys.append(y)
        self._scan_idx += 1; self._progress.setValue(self._scan_idx)
        if MPL_AVAILABLE:
            self._line.set_data(self._scan_xs, self._scan_ys)
            self._ax.relim(); self._ax.autoscale_view(); self._canvas.draw_idle()
        if self._scan_idx >= len(self._positions): self._finish_scan()

    def _stop_scan(self):
        self._scanning = False; self._scan_timer.stop()
        self._scan_btn.setEnabled(True); self._stop_btn.setEnabled(False)

    def _finish_scan(self):
        self._scanning = False; self._scan_timer.stop()
        self._scan_btn.setEnabled(True); self._stop_btn.setEnabled(False)
        self._progress.setValue(self._scan_n)

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
        chart_split.addWidget(StripChart(all_pvs, "Strip Chart 1"))
        chart_split.addWidget(StripChart(all_pvs, "Strip Chart 2"))
        chart_split.setSizes([1,1])
        hsplit.addWidget(chart_split)
        hsplit.setSizes([600, 400])

        vsplit.addWidget(hsplit)
        vsplit.setSizes([200, 700])
        outer.addWidget(vsplit, 1)

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
