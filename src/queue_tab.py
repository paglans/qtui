"""
queue_tab.py — Tab 4: Bluesky Queue Server
Connects to a running bluesky-queueserver instance via the HTTP API.
Provides queue management, RE controls, and a live event log.

Requires:  pip install bluesky-queueserver-api
Falls back gracefully if the package is absent or the server is unreachable.

Public API (called from DAQTab)
────────────────────────────────
    queue_tab.add_plan(name, args, kwargs, meta)  → bool
        Submits a plan dict to the back of the queue.
        Returns True on success, False otherwise.
"""

import time
import json
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSplitter, QGroupBox, QGridLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QTextEdit, QFrame, QAbstractItemView, QSizePolicy,
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QMutex, QMutexLocker
from PySide6.QtGui import QFont, QColor

from common import (
    PAL, BTN_STYLE, GRP_STYLE, SPLITTER_STYLE,
)

# ── Optional dependency ────────────────────────────────────────────────────────
QS_AVAILABLE = False
try:
    from bluesky_queueserver_api.http import REManagerAPI
    QS_AVAILABLE = True
except ImportError:
    pass

# ── Local styles ───────────────────────────────────────────────────────────────
_TABLE_STYLE = f"""
    QTableWidget {{
        background:{PAL['bg']}; color:{PAL['text']};
        gridline-color:#2a3a5e; border:1px solid #2a3a5e;
        border-radius:4px; font-family:monospace; font-size:8pt;
    }}
    QTableWidget::item {{ padding: 2px 4px; }}
    QTableWidget::item:selected {{ background:#2a3a5e; color:{PAL['accent']}; }}
    QHeaderView::section {{
        background:{PAL['surface']}; color:{PAL['accent']};
        border:none; border-right:1px solid #2a3a5e;
        border-bottom:1px solid #2a3a5e; padding:4px; font-size:8pt;
    }}
"""
_LOG_STYLE = f"""
    QTextEdit {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        font-family:monospace; font-size:8pt;
    }}
"""
_IND_BASE = ("width:12px; height:12px; border-radius:6px; border:1px solid #555;"
             " background:{col};")

# RE/environment state → colour
_ENV_COLORS = {
    "idle":    PAL["ok"],    # environment exists and is ready
    "opening": PAL["warn"],
    "closing": PAL["warn"],
    "failed":  PAL["nc"],
    "closed":  PAL["nc"],
}
_RE_COLORS = {
    "idle":     PAL["ok"],
    "running":  PAL["accent"],
    "paused":   PAL["warn"],
    "stopping": PAL["warn"],
    "aborting": PAL["nc"],
    "unknown":  PAL["subtext"],
}
_ITEM_STATUS_COLORS = {
    "completed": PAL["ok"],
    "failed":    PAL["nc"],
    "stopped":   PAL["warn"],
    "running":   PAL["accent"],
}


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _fmt_args(item: dict) -> str:
    """One-line summary of plan args/kwargs."""
    args   = item.get("args",   [])
    kwargs = item.get("kwargs", {})
    parts  = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
    s = ", ".join(parts)
    return s[:60] + "…" if len(s) > 60 else s


# ══════════════════════════════════════════════════════════════════════════════
# Background poller thread
# ══════════════════════════════════════════════════════════════════════════════

class _QSPoller(QThread):
    """Polls the queue server REST API on a background thread.

    Emits signals on the Qt thread via the normal queued connection mechanism.
    All API calls are made inside this thread — the Qt main thread never blocks.
    """

    status_updated  = Signal(dict)   # full status dict from /api/status
    queue_updated   = Signal(list)   # list of plan dicts
    history_updated = Signal(list)   # list of completed-plan dicts
    log_message     = Signal(str, str)  # (message, colour)
    connected       = Signal(bool)   # True = server reachable

    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self._cfg      = cfg
        self._rm       = None          # REManagerAPI instance
        self._running  = True
        self._mutex    = QMutex()
        self._last_connected = None

    # ── Public control ────────────────────────────────────────────────────────

    def stop(self):
        with QMutexLocker(self._mutex):
            self._running = False

    def call(self, method: str, **kwargs):
        """Execute an API method synchronously from *any* thread.
        Returns the response dict, or None on error.
        """
        if self._rm is None:
            return None
        try:
            fn = getattr(self._rm, method)
            return fn(**kwargs)
        except Exception as exc:
            self.log_message.emit(
                f"[{_ts()}] API error ({method}): {exc}", PAL["nc"])
            return None

    # ── Thread body ───────────────────────────────────────────────────────────

    def run(self):
        cfg      = self._cfg
        host     = cfg.get("http_host", "localhost")
        port     = cfg.get("http_port", 60610)
        interval = float(cfg.get("poll_interval_s", 2.0))
        uri      = f"http://{host}:{port}"

        if QS_AVAILABLE:
            try:
                self._rm = REManagerAPI(
                    http_server_uri=uri,
                    http_auth_provider="APIKEY",
                )
                api_key = cfg.get("http_api_key", "")
                if api_key:
                    self._rm.set_authorization_key(api_key=api_key)
            except Exception as exc:
                self.log_message.emit(
                    f"[{_ts()}] Cannot create REManagerAPI: {exc}", PAL["nc"])

        while True:
            with QMutexLocker(self._mutex):
                if not self._running:
                    break

            reachable = self._poll_once()

            if reachable != self._last_connected:
                self._last_connected = reachable
                self.connected.emit(reachable)
                colour = PAL["ok"] if reachable else PAL["nc"]
                msg    = "Connected to queue server" if reachable \
                         else f"Queue server unreachable at {uri}"
                self.log_message.emit(f"[{_ts()}] {msg}", colour)

            time.sleep(interval)

    def _poll_once(self) -> bool:
        if self._rm is None:
            return False
        try:
            status  = self._rm.status()
            q       = self._rm.queue_get()
            history = self._rm.history_get()
            self.status_updated.emit(status)
            self.queue_updated.emit(q.get("items", []))
            self.history_updated.emit(history.get("items", []))
            return True
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════════════════════
# Queue tab widget
# ══════════════════════════════════════════════════════════════════════════════

class QueueTab(QWidget):
    """Bluesky queue server monitor and control tab.

    Layout
    ──────
    Status bar
    ─────────────────────────────────────
    Horizontal QSplitter
      Left  : Queue table  + item buttons
      Right : QSplitter (vertical)
                History table  (top)
                Event log      (bottom)
    ─────────────────────────────────────
    RE control bar
    """

    def __init__(self, qs_cfg: dict, parent=None):
        super().__init__(parent)
        self._cfg      = qs_cfg
        self._poller   = None
        self._connected = False
        self._env_exists = False
        # UIDs for the current queue items (parallel to table rows)
        self._queue_uids: list[str] = []

        self.setStyleSheet(f"background:{PAL['bg']};")
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Header
        hdr = QLabel("  🗂  Bluesky Queue Server")
        hdr.setFont(QFont("Sans Serif", 9, QFont.Bold))
        hdr.setStyleSheet(
            f"background:{PAL['surface']}; color:{PAL['accent']}; padding:6px;")
        root.addWidget(hdr)

        root.addWidget(self._build_status_bar())

        # Main splitter
        hsplit = QSplitter(Qt.Horizontal)
        hsplit.setStyleSheet(SPLITTER_STYLE)
        hsplit.addWidget(self._build_queue_panel())
        hsplit.addWidget(self._build_right_panel())
        hsplit.setSizes([600, 700])
        root.addWidget(hsplit, 1)

        root.addWidget(self._build_re_bar())

        # Start poller
        if qs_cfg.get("autoconnect", True):
            self._start_poller()
        else:
            self._log("Queue server autoconnect disabled — press Connect.",
                      PAL["subtext"])

    # ── UI builders ───────────────────────────────────────────────────────────

    def _build_status_bar(self) -> QWidget:
        bar = QWidget()
        bar.setStyleSheet(f"background:{PAL['surface']}; border-bottom:1px solid #2a3a5e;")
        hl  = QHBoxLayout(bar)
        hl.setContentsMargins(10, 4, 10, 4); hl.setSpacing(12)

        # Connection indicator
        self._ind_conn = QLabel()
        self._ind_conn.setFixedSize(12, 12)
        self._set_indicator(self._ind_conn, PAL["nc"])
        hl.addWidget(self._ind_conn)
        self._lbl_conn = QLabel("Disconnected")
        self._lbl_conn.setStyleSheet(f"color:{PAL['nc']}; font-size:8pt;")
        hl.addWidget(self._lbl_conn)

        hl.addWidget(_vline())

        # Environment state
        hl.addWidget(_sub("Env:"))
        self._lbl_env = QLabel("–")
        self._lbl_env.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt; font-family:monospace;")
        hl.addWidget(self._lbl_env)

        hl.addWidget(_vline())

        # RE state
        hl.addWidget(_sub("RE:"))
        self._lbl_re = QLabel("–")
        self._lbl_re.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt; font-family:monospace;")
        hl.addWidget(self._lbl_re)

        hl.addWidget(_vline())

        # Queue counts (replaces missing worker_pid)
        hl.addWidget(_sub("Queue:"))
        self._lbl_worker = QLabel("–")
        self._lbl_worker.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt; font-family:monospace;")
        hl.addWidget(self._lbl_worker)

        hl.addStretch()

        # Connect / Disconnect button
        self._conn_btn = QPushButton("Connect")
        self._conn_btn.setStyleSheet(BTN_STYLE)
        self._conn_btn.setFixedWidth(90)
        self._conn_btn.clicked.connect(self._toggle_connect)
        hl.addWidget(self._conn_btn)

        # Open / Close environment button
        self._env_btn = QPushButton("Open Env")
        self._env_btn.setStyleSheet(BTN_STYLE)
        self._env_btn.setFixedWidth(90)
        self._env_btn.setEnabled(False)
        self._env_btn.clicked.connect(self._toggle_env)
        hl.addWidget(self._env_btn)

        return bar

    def _build_queue_panel(self) -> QWidget:
        w  = QWidget(); w.setStyleSheet(f"background:{PAL['bg']};")
        vl = QVBoxLayout(w); vl.setContentsMargins(8, 8, 8, 4); vl.setSpacing(6)

        self._queue_grp = QGroupBox("Queue  (0 items)")
        self._queue_grp.setStyleSheet(GRP_STYLE)
        grp_vl = QVBoxLayout(self._queue_grp)
        grp_vl.setContentsMargins(4, 16, 4, 4); grp_vl.setSpacing(4)

        self._queue_tbl = _make_table(["#", "Plan", "Args / kwargs", "UID"])
        self._queue_tbl.setColumnWidth(0, 32)
        self._queue_tbl.setColumnWidth(1, 110)
        self._queue_tbl.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.Stretch)
        self._queue_tbl.setColumnWidth(3, 90)
        grp_vl.addWidget(self._queue_tbl)

        # Item action buttons
        btn_row = QHBoxLayout(); btn_row.setSpacing(6)
        self._up_btn     = _btn("↑  Up",       self._queue_item_up)
        self._dn_btn     = _btn("↓  Down",      self._queue_item_down)
        self._rm_btn     = _btn("✕  Remove",    self._queue_item_remove)
        self._clr_btn    = _btn("Clear Queue",  self._queue_clear)
        for b in (self._up_btn, self._dn_btn, self._rm_btn, self._clr_btn):
            b.setEnabled(False)
            btn_row.addWidget(b)
        btn_row.addStretch()
        grp_vl.addLayout(btn_row)

        vl.addWidget(self._queue_grp, 1)
        return w

    def _build_right_panel(self) -> QWidget:
        vsplit = QSplitter(Qt.Vertical)
        vsplit.setStyleSheet(SPLITTER_STYLE)

        # History
        hist_grp = QGroupBox("History")
        hist_grp.setStyleSheet(GRP_STYLE)
        hg_vl = QVBoxLayout(hist_grp)
        hg_vl.setContentsMargins(4, 16, 4, 4)
        self._hist_tbl = _make_table(["#", "Plan", "Status", "Exit status", "UID"])
        self._hist_tbl.setColumnWidth(0, 32)
        self._hist_tbl.setColumnWidth(1, 110)
        self._hist_tbl.setColumnWidth(2, 80)
        self._hist_tbl.setColumnWidth(3, 80)
        self._hist_tbl.horizontalHeader().setSectionResizeMode(
            4, QHeaderView.Stretch)
        hg_vl.addWidget(self._hist_tbl)
        vsplit.addWidget(hist_grp)

        # Log
        log_grp = QGroupBox("Event Log")
        log_grp.setStyleSheet(GRP_STYLE)
        lg_vl = QVBoxLayout(log_grp)
        lg_vl.setContentsMargins(4, 16, 4, 4); lg_vl.setSpacing(4)
        self._log_edit = QTextEdit()
        self._log_edit.setReadOnly(True)
        self._log_edit.setStyleSheet(_LOG_STYLE)
        lg_vl.addWidget(self._log_edit)
        clr_row = QHBoxLayout()
        clr_row.addStretch()
        clr_row.addWidget(_btn("Clear Log",
                               lambda: self._log_edit.clear()))
        lg_vl.addLayout(clr_row)
        vsplit.addWidget(log_grp)

        vsplit.setSizes([300, 300])
        return vsplit

    def _build_re_bar(self) -> QWidget:
        bar = QWidget()
        bar.setStyleSheet(
            f"background:{PAL['surface']}; border-top:1px solid #2a3a5e;")
        hl = QHBoxLayout(bar)
        hl.setContentsMargins(10, 6, 10, 6); hl.setSpacing(8)

        hl.addWidget(_sub("RE Controls:"))

        self._start_btn  = _btn("▶  Start Queue",  self._re_start)
        self._stop_btn   = _btn("⏹  Stop Queue",   self._re_stop)
        self._pause_btn  = _btn("⏸  Pause",        self._re_pause)
        self._resume_btn = _btn("▶  Resume",       self._re_resume)
        self._abort_btn  = _btn("■  Abort",        self._re_abort)

        for b in (self._start_btn, self._stop_btn, self._pause_btn,
                  self._resume_btn, self._abort_btn):
            b.setEnabled(False)
            hl.addWidget(b)

        hl.addStretch()
        return bar

    # ── Poller lifecycle ──────────────────────────────────────────────────────

    def _start_poller(self):
        if self._poller and self._poller.isRunning():
            return
        if not QS_AVAILABLE:
            self._log(
                "bluesky-queueserver-api not installed — "
                "run: pip install bluesky-queueserver-api",
                PAL["nc"])
            return
        self._poller = _QSPoller(self._cfg, parent=self)
        self._poller.status_updated.connect(self._on_status)
        self._poller.queue_updated.connect(self._on_queue)
        self._poller.history_updated.connect(self._on_history)
        self._poller.log_message.connect(self._log)
        self._poller.connected.connect(self._on_connected)
        self._poller.start()
        self._conn_btn.setText("Disconnect")

    def _stop_poller(self):
        if self._poller:
            self._poller.stop()
            self._poller.wait(3000)
            self._poller = None
        self._conn_btn.setText("Connect")
        self._on_connected(False)

    def _toggle_connect(self):
        if self._poller and self._poller.isRunning():
            self._stop_poller()
        else:
            self._start_poller()

    # ── Poller slots ──────────────────────────────────────────────────────────

    def _on_connected(self, ok: bool):
        self._connected = ok
        col  = PAL["ok"] if ok else PAL["nc"]
        text = "Connected" if ok else "Disconnected"
        self._set_indicator(self._ind_conn, col)
        self._lbl_conn.setText(text)
        self._lbl_conn.setStyleSheet(f"color:{col}; font-size:8pt;")
        self._env_btn.setEnabled(ok)
        if not ok:
            self._env_exists = False
            for b in (self._start_btn, self._stop_btn, self._pause_btn,
                      self._resume_btn, self._abort_btn,
                      self._up_btn, self._dn_btn, self._rm_btn, self._clr_btn):
                b.setEnabled(False)
            self._lbl_env.setText("–")
            self._lbl_re.setText("–")
            self._lbl_worker.setText("–")

    def _on_status(self, s: dict):
        env_exists = s.get("worker_environment_exists", False)
        env_state  = s.get("worker_environment_state", "unknown")
        re         = s.get("re_state", "unknown")
        mgr        = s.get("manager_state", "unknown")

        self._env_exists = env_exists   # keep in sync for _toggle_env

        env_col = PAL["ok"] if env_exists and env_state == "idle" else \
                  _ENV_COLORS.get(env_state, PAL["subtext"])
        self._lbl_env.setText(env_state + (" ✓" if env_exists else " ✗"))
        self._lbl_env.setStyleSheet(
            f"color:{env_col}; font-size:8pt; font-family:monospace;")

        re_col = _RE_COLORS.get(re, PAL["subtext"])
        self._lbl_re.setText(re)
        self._lbl_re.setStyleSheet(
            f"color:{re_col}; font-size:8pt; font-family:monospace;")

        self._lbl_worker.setText(f"{s.get('items_in_queue', 0)} queued / "
                                  f"{s.get('items_in_history', 0)} done")

        self._env_btn.setText("Close Env" if env_exists else "Open Env")
        self._env_btn.setEnabled(self._connected)

        env_open      = env_exists and env_state == "idle"
        is_idle       = (re == "idle")
        is_paused     = (re == "paused")
        is_running    = (re == "running")
        queue_running = (mgr == "executing_queue")

        self._start_btn.setEnabled(env_open and is_idle)
        self._stop_btn.setEnabled(env_open and queue_running)
        self._pause_btn.setEnabled(env_open and is_running)
        self._resume_btn.setEnabled(env_open and is_paused)
        self._abort_btn.setEnabled(env_open and (is_running or is_paused))

    def _on_queue(self, items: list):
        n = len(items)
        self._queue_grp.setTitle(f"Queue  ({n} item{'s' if n!=1 else ''})")
        self._queue_uids = [it.get("item_uid", "") for it in items]
        tbl = self._queue_tbl
        tbl.setRowCount(n)
        for row, it in enumerate(items):
            uid_short = it.get("item_uid", "")[:8]
            _set_row(tbl, row, [
                str(row + 1),
                it.get("name", "?"),
                _fmt_args(it),
                uid_short,
            ])
        # Item buttons enabled only when something is selected
        has_sel = bool(self._queue_uids)
        for b in (self._up_btn, self._dn_btn, self._rm_btn, self._clr_btn):
            b.setEnabled(has_sel and self._connected)

    def _on_history(self, items: list):
        # Most recent first
        items = list(reversed(items))
        tbl   = self._hist_tbl
        tbl.setRowCount(len(items))
        for row, it in enumerate(items):
            result = it.get("result", {})
            status = result.get("run_uids", [""])
            exit_s = result.get("exit_status", "–")
            col    = _ITEM_STATUS_COLORS.get(exit_s.lower(), PAL["text"])
            uid_s  = it.get("item_uid", "")[:8]
            _set_row(tbl, row, [
                str(row + 1),
                it.get("name", "?"),
                exit_s,
                str(len(status)) + " run(s)",
                uid_s,
            ])
            # Colour the status cell
            cell = tbl.item(row, 2)
            if cell:
                cell.setForeground(QColor(col))

    # ── RE / queue actions ────────────────────────────────────────────────────

    def _api(self, method: str, **kwargs) -> dict | None:
        if not self._poller:
            return None
        result = self._poller.call(method, **kwargs)
        return result

    def _re_start(self):
        r = self._api("queue_start")
        self._log(f"[{_ts()}] queue_start → {_result_str(r)}", PAL["ok"])

    def _re_stop(self):
        r = self._api("queue_stop")
        self._log(f"[{_ts()}] queue_stop → {_result_str(r)}", PAL["warn"])

    def _re_pause(self):
        r = self._api("re_pause")
        self._log(f"[{_ts()}] re_pause → {_result_str(r)}", PAL["warn"])

    def _re_resume(self):
        r = self._api("re_resume")
        self._log(f"[{_ts()}] re_resume → {_result_str(r)}", PAL["ok"])

    def _re_abort(self):
        r = self._api("re_abort")
        self._log(f"[{_ts()}] re_abort → {_result_str(r)}", PAL["nc"])

    def _toggle_env(self):
        if self._env_exists:
            r = self._api("environment_close")
            self._log(f"[{_ts()}] environment_close → {_result_str(r)}", PAL["warn"])
        else:
            r = self._api("environment_open")
            self._log(f"[{_ts()}] environment_open → {_result_str(r)}", PAL["ok"])

    def _queue_item_up(self):
        row = self._queue_tbl.currentRow()
        if row <= 0 or row >= len(self._queue_uids):
            return
        uid = self._queue_uids[row]
        r   = self._api("item_move", uid=uid, pos_dest=row - 1)
        self._log(f"[{_ts()}] move ↑ {uid[:8]} → {_result_str(r)}", PAL["text"])

    def _queue_item_down(self):
        row = self._queue_tbl.currentRow()
        if row < 0 or row >= len(self._queue_uids) - 1:
            return
        uid = self._queue_uids[row]
        r   = self._api("item_move", uid=uid, pos_dest=row + 1)
        self._log(f"[{_ts()}] move ↓ {uid[:8]} → {_result_str(r)}", PAL["text"])

    def _queue_item_remove(self):
        row = self._queue_tbl.currentRow()
        if row < 0 or row >= len(self._queue_uids):
            return
        uid = self._queue_uids[row]
        r   = self._api("item_remove", uid=uid)
        self._log(f"[{_ts()}] remove {uid[:8]} → {_result_str(r)}", PAL["warn"])

    def _queue_clear(self):
        r = self._api("queue_clear")
        self._log(f"[{_ts()}] queue_clear → {_result_str(r)}", PAL["warn"])

    # ── Public API (called from DAQTab) ───────────────────────────────────────

    def add_plan(self, name: str, args: list, kwargs: dict,
                 meta: dict | None = None) -> bool:
        """Submit a plan to the back of the queue.

        Parameters
        ----------
        name   : bluesky plan name, e.g. "scan" or "grid_scan"
        args   : positional arguments (devices, ranges, …)
        kwargs : keyword arguments
        meta   : optional metadata stored with the item

        Returns True on success.
        """
        if not self._connected:
            self._log(
                f"[{_ts()}] Cannot add plan — server not connected.",
                PAL["nc"])
            return False

        item: dict = {
            "item_type": "plan",
            "name":      name,
            "args":      args,
            "kwargs":    kwargs,
        }
        if meta:
            item["meta"] = meta

        r = self._api("item_add", item=item)
        ok = r is not None and r.get("success", False)
        col = PAL["ok"] if ok else PAL["nc"]
        self._log(
            f"[{_ts()}] add_plan '{name}' → {_result_str(r)}", col)
        return ok

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _log(self, message: str, colour: str = ""):
        colour = colour or PAL["text"]
        self._log_edit.append(
            f'<span style="color:{colour};">{message}</span>')
        sb = self._log_edit.verticalScrollBar()
        sb.setValue(sb.maximum())

    @staticmethod
    def _set_indicator(label: QLabel, colour: str):
        label.setStyleSheet(
            f"background:{colour}; border-radius:6px; border:1px solid #555;")

    def closeEvent(self, ev):
        if self._poller:
            self._poller.stop()
            self._poller.wait(3000)
        super().closeEvent(ev)


# ── Module-level helpers ───────────────────────────────────────────────────────

def _sub(text: str) -> QLabel:
    lb = QLabel(text)
    lb.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt;")
    return lb


def _vline() -> QFrame:
    f = QFrame(); f.setFrameShape(QFrame.VLine)
    f.setStyleSheet("color:#2a3a5e;"); f.setFixedWidth(1)
    return f


def _btn(label: str, slot=None) -> QPushButton:
    b = QPushButton(label); b.setStyleSheet(BTN_STYLE)
    if slot:
        b.clicked.connect(slot)
    return b


def _make_table(headers: list[str]) -> QTableWidget:
    tbl = QTableWidget(0, len(headers))
    tbl.setHorizontalHeaderLabels(headers)
    tbl.setStyleSheet(_TABLE_STYLE)
    tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
    tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
    tbl.setSelectionMode(QAbstractItemView.SingleSelection)
    tbl.verticalHeader().setVisible(False)
    tbl.horizontalHeader().setStretchLastSection(True)
    tbl.setAlternatingRowColors(False)
    return tbl


def _set_row(tbl: QTableWidget, row: int, values: list[str]):
    for col, v in enumerate(values):
        item = QTableWidgetItem(str(v))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        tbl.setItem(row, col, item)


def _result_str(r) -> str:
    if r is None:
        return "no response"
    if isinstance(r, dict):
        if not r.get("success", True):
            return f"FAILED: {r.get('msg', r)}"
        return "ok"
    return str(r)
