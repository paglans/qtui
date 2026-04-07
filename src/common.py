"""
common.py — shared constants, styles, helpers and the PVMonitor / PVLabel
that are used by both tabs.
"""
import json, re, random
from functools import partial
from pathlib import Path

from PySide6.QtWidgets import QLabel, QApplication
from PySide6.QtCore import Qt, QTimer, Signal, QObject
from PySide6.QtGui import QColor

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

# ── Optional: tiled ───────────────────────────────────────────────────────────
try:
    from tiled.client import from_uri as _tiled_from_uri
    TILED_AVAILABLE = True
except ImportError:
    TILED_AVAILABLE = False
    print("[INFO] tiled not found – Tiled output unavailable")

# ── Optional: PySide6-WebEngine ───────────────────────────────────────────────
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings
    WEBENGINE_AVAILABLE = True
except ImportError:
    WEBENGINE_AVAILABLE = False
    print("[INFO] PySide6-WebEngine not found – ALS beam status frame unavailable")

PYVISTA_AVAILABLE = False   # disabled: GLX conflict with QtWebEngine on X11

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
                epics.ca.poll(evt=0.002)   # yield to CA context after each PV creation
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

# ── TiledWriter ───────────────────────────────────────────────────────────────
class TiledWriter:
    """
    Thin wrapper around the Tiled Python client for writing scan data.

    Connection is established lazily on the first write attempt so that
    startup is never blocked.  The API key is read from the environment
    variable TILED_SINGLE_USER_API_KEY at connection time; it is never
    stored in configuration.json.

    Usage
    -----
    writer = TiledWriter(cfg)          # cfg = config["tiled"] dict
    ok, msg = writer.write_scan(df, metadata)

    Parameters in cfg dict
    ----------------------
    enabled   : bool   – master switch (checked before every write)
    host      : str    – server hostname, e.g. "localhost"
    port      : int    – server port,     e.g. 8000
    container : str    – top-level container name, e.g. "amber"
    """

    _ENV_KEY = "TILED_SINGLE_USER_API_KEY"

    def __init__(self, cfg: dict):
        self._cfg     = cfg
        self._client  = None   # lazily initialised

    # ── Public API ────────────────────────────────────────────────────────────

    def write_scan(self, df, metadata: dict) -> tuple[bool, str]:
        """
        Write a completed 1-D scan as a Tiled table.

        Parameters
        ----------
        df       : pandas.DataFrame  – columns are motor name and detector name
        metadata : dict              – arbitrary scan metadata (scan_num, motor,
                                       detector, timestamp, facility, …)

        Returns
        -------
        (True,  "uri/key")   on success
        (False, "error msg") on failure or when disabled/unavailable
        """
        if not TILED_AVAILABLE:
            return False, "tiled package not installed"
        if not self._cfg.get("enabled", False):
            return False, "Tiled disabled in configuration"

        ok, msg = self._ensure_connected()
        if not ok:
            return False, msg

        try:
            container = self._get_container()
            node = container.write_table(df, metadata=metadata)
            return True, str(node.uri)
        except Exception as exc:
            # Connection may have dropped — invalidate so next call retries.
            self._client = None
            return False, f"Tiled write error: {exc}"

    def check_connection(self) -> tuple[bool, str]:
        """
        Probe the server without writing.  Returns (ok, message).
        Useful for a "Test connection" button in the UI.
        """
        if not TILED_AVAILABLE:
            return False, "tiled package not installed"
        self._client = None          # force reconnect
        ok, msg = self._ensure_connected()
        if ok:
            return True, f"Connected to {self._base_uri()}"
        return False, msg

    # ── Internals ─────────────────────────────────────────────────────────────

    def _base_uri(self) -> str:
        host = self._cfg.get("host", "localhost")
        port = int(self._cfg.get("port", 8000))
        return f"http://{host}:{port}"

    def _ensure_connected(self) -> tuple[bool, str]:
        if self._client is not None:
            return True, "ok"
        import os
        api_key = os.environ.get(self._ENV_KEY, "").strip() or None
        try:
            uri = self._base_uri()
            self._client = _tiled_from_uri(uri, api_key=api_key)
            return True, "ok"
        except Exception as exc:
            return False, f"Tiled connection failed ({self._base_uri()}): {exc}"

    def _get_container(self):
        """Return (creating if necessary) the top-level container node."""
        name = self._cfg.get("container", "amber")
        try:
            return self._client[name]
        except KeyError:
            # Container doesn't exist yet — create it.
            return self._client.create_container(key=name)

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
