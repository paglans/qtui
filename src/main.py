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

        endstation_tab = EndstationTab(hirrixs_cfg, amber_cfg, all_pvs, all_signals)
        daq_tab        = DAQTab(amber_cfg, hirrixs_cfg)
        blop_tab       = BLOPTab(amber_cfg, hirrixs_cfg)
        tabs.addTab(BeamlineTab(amber_cfg, all_signals, all_pvs), "🔬  AMBER Beamline")
        tabs.addTab(endstation_tab,                               "⚗️  HiRRIXS Endstation")
        tabs.addTab(daq_tab,                                      "💾  Data Acquisition")
        tabs.addTab(blop_tab,                                     "🤖  BLOP")
        self.setCentralWidget(tabs)

        def _on_tab_changed(idx):
            if idx == 1:
                endstation_tab._viewer.initialize()
        tabs.currentChanged.connect(_on_tab_changed)

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
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
