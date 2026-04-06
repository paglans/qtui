#!/usr/bin/env python3
"""
AMBER Beamline / HiRRIXS Endstation Control GUI — entry point
"""
# ── Environment flags — must be set before ANY imports touch OpenGL/Qt ────────
import os
os.environ.setdefault("VTK_DEFAULT_RENDER_WINDOW", "vtkEGLRenderWindow")
os.environ.setdefault("LIBGL_ALWAYS_SOFTWARE", "1")
os.environ.setdefault("GALLIUM_DRIVER",         "llvmpipe")
os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = (
    "--disable-gpu --disable-gpu-compositing "
    "--disable-software-rasterizer "
    "--no-sandbox --disable-dev-shm-usage"
)

import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QMainWindow, QTabWidget
from PySide6.QtGui import QPalette, QColor
from PySide6.QtCore import Qt

from common import PAL, load_json
from beamline_tab import BeamlineTab
from endstation_tab import EndstationTab
from daq_tab import DAQTab
from blop_tab import BLOPTab
from configuration_tab import ConfigurationTab


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AMBER / HiRRIXS Control GUI")
        self.resize(1600, 1100)
        self._apply_palette()

        base = Path(__file__).parent.parent / "config"
        amber_cfg   = load_json(base / "amber.json").get("amber", {})
        hirrixs_cfg = load_json(base / "hirrixs.json").get("hirrixs", {})

        all_signals: dict = {}
        for n, p in amber_cfg.get("signal", {}).items():   all_signals[n] = p
        for n, p in hirrixs_cfg.get("signal", {}).items(): all_signals[n] = p

        def _flat(entry, prefix=""):
            if isinstance(entry, str):  return {prefix: entry} if prefix else {}
            if isinstance(entry, dict): return {(f"{prefix}:{k}" if prefix else k): v
                                                for k, v in entry.items()
                                                if isinstance(v, str)}
            return {}

        all_pvs: dict = {}
        for k, v in amber_cfg.get("motor",  {}).items(): all_pvs.update(_flat(v, k))
        for k, v in amber_cfg.get("signal", {}).items(): all_pvs[k] = v
        for k, v in hirrixs_cfg.get("motor",  {}).items(): all_pvs.update(_flat(v, k))
        for k, v in hirrixs_cfg.get("signal", {}).items(): all_pvs[k] = v

        tabs = QTabWidget()
        tabs.setStyleSheet("""
            QTabWidget::pane   { border:none; background:#1a1a2e; }
            QTabBar::tab       { background:#16213e; color:#9e9e9e;
                                 padding:8px 22px; border:none;
                                 border-bottom:2px solid transparent; }
            QTabBar::tab:selected { color:#4fc3f7; border-bottom:2px solid #4fc3f7;
                                    background:#1a1a2e; }
            QTabBar::tab:hover { background:#1e2a4e; color:#e0e0e0; }
        """)

        # ── Instantiate all tabs (assign to locals so signals can be wired) ───
        beamline_tab   = BeamlineTab(amber_cfg, all_signals, all_pvs)
        endstation_tab = EndstationTab(hirrixs_cfg, amber_cfg, all_pvs, all_signals)
        config_tab     = ConfigurationTab()
        daq_tab        = DAQTab(amber_cfg, hirrixs_cfg, config_tab)
        blop_tab       = BLOPTab(amber_cfg, hirrixs_cfg)

        tabs.addTab(beamline_tab,   "🔬  AMBER Beamline")
        tabs.addTab(endstation_tab, "⚗️  HiRRIXS Endstation")
        tabs.addTab(daq_tab,        "💾  Data Acquisition")
        tabs.addTab(blop_tab,       "🤖  BLOP")
        tabs.addTab(config_tab,     "⚙️  Configuration")

        # ── Wire live config updates ──────────────────────────────────────────
        config_tab.config_changed.connect(beamline_tab.apply_config)
        config_tab.config_changed.connect(endstation_tab.apply_config)
        config_tab.config_changed.connect(daq_tab.apply_config)
        config_tab.config_changed.connect(self._apply_epics_config)
        # config_tab.config_changed.connect(blop_tab.apply_config)

        # ── Apply stored config values at startup ─────────────────────────────
        for key in [
            "ui.strip_chart_history_s",
            "ui.strip_chart_update_ms",
            "ui.image_rate_limit_hz",
            "ui.overlay_opacity_rest",
            "ui.overlay_opacity_hover",
        ]:
            value = config_tab.get(key)
            if value is not None:
                beamline_tab.apply_config(key, value)
                endstation_tab.apply_config(key, value)

        # ── Apply EPICS env vars from stored config at startup ────────────────
        for key in [
            "epics.ca_addr_list", "epics.ca_auto_addr",
            "epics.pva_addr_list", "epics.pva_auto_addr",
            "epics.ca_max_array_bytes",
        ]:
            value = config_tab.get(key)
            if value is not None:
                self._apply_epics_config(key, value)

        self.setCentralWidget(tabs)

        def _on_tab_changed(idx):
            if idx == 1:
                endstation_tab._viewer.initialize()
        tabs.currentChanged.connect(_on_tab_changed)

    # ── EPICS environment variable handler ────────────────────────────────────
    def _apply_epics_config(self, key: str, value):
        """
        Push epics.* config values into os.environ so that both pyepics (CA)
        and p4p (PVA) pick them up.  Must be called before CA/PVA contexts
        are created; at runtime a warning is printed because libca is already
        initialised — a full restart is required for CA changes to take effect.
        """
        _MAP = {
            "epics.ca_addr_list":       "EPICS_CA_ADDR_LIST",
            "epics.ca_auto_addr":       "EPICS_CA_AUTO_ADDR_LIST",
            "epics.pva_addr_list":      "EPICS_PVA_ADDR_LIST",
            "epics.pva_auto_addr":      "EPICS_PVA_AUTO_ADDR_LIST",
            "epics.ca_max_array_bytes": "EPICS_CA_MAX_ARRAY_BYTES",
        }
        env_key = _MAP.get(key)
        if env_key is None:
            return

        # Booleans → "YES" / "NO" as EPICS expects
        if isinstance(value, bool):
            env_val = "YES" if value else "NO"
        else:
            env_val = str(value)

        os.environ[env_key] = env_val
        print(f"[config] {env_key}={env_val}")

    def _apply_palette(self):
        pal = QPalette()
        pal.setColor(QPalette.Window,     QColor(PAL["bg"]))
        pal.setColor(QPalette.WindowText, QColor(PAL["text"]))
        pal.setColor(QPalette.Base,       QColor(PAL["surface"]))
        pal.setColor(QPalette.Text,       QColor(PAL["text"]))
        pal.setColor(QPalette.Button,     QColor(PAL["surface"]))
        pal.setColor(QPalette.ButtonText, QColor(PAL["text"]))
        QApplication.instance().setPalette(pal)


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("AMBER/HiRRIXS Control")
    app.setStyle("Fusion")

    try:
        import epics
        epics.ca.initialize_libca()
    except Exception:
        pass

    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
