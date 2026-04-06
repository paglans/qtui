"""
configuration_tab.py — Tab 5: Configuration
Displays and edits ../config/configuration.json, guided by
../config/configuration_schema.json for richer widgets and validation.

Public signal
─────────────
ConfigurationTab.config_changed(str, object)
    Emitted after a successful save for every key whose schema has a "live"
    field.  First arg is the dotted path (e.g. "ui.strip_chart_update_ms"),
    second arg is the new Python value (int / float / bool / str).
"""
import json
import re
import shutil
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QScrollArea, QGroupBox, QFrame, QSizePolicy, QMessageBox,
    QSplitter, QTextEdit, QCheckBox, QComboBox, QDoubleSpinBox,
    QSpinBox, QLineEdit, QApplication,
)
from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtGui import QFont

from common import PAL, BTN_STYLE, GRP_STYLE, INPUT_STYLE, SPLITTER_STYLE

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE         = Path(__file__).parent.parent / "config"
_CONFIG_PATH  = _BASE / "configuration.json"
_SCHEMA_PATH  = _BASE / "configuration_schema.json"

# ── Local styles ──────────────────────────────────────────────────────────────
_HEADER_STYLE = (
    f"color:{PAL['accent']}; font-weight:bold; font-size:10pt; "
    "background:transparent; border:none;"
)
_KEY_STYLE    = (
    f"color:{PAL['text']}; font-family:monospace; font-size:9pt; "
    "background:transparent; border:none;"
)
_SUBKEY_STYLE = (
    f"color:{PAL['subtext']}; font-family:monospace; font-size:8pt; "
    "background:transparent; border:none;"
)
_CHECK_STYLE  = f"""
    QCheckBox {{ color:{PAL['text']}; spacing:6px; }}
    QCheckBox::indicator {{
        width:14px; height:14px; border:1px solid #2a3a5e;
        border-radius:3px; background:{PAL['bg']};
    }}
    QCheckBox::indicator:checked {{
        background:{PAL['accent']}; border-color:{PAL['accent']};
    }}
"""
_SPIN_STYLE   = f"""
    QSpinBox, QDoubleSpinBox {{
        background:{PAL['bg']}; color:{PAL['text']};
        border:1px solid #2a3a5e; border-radius:4px;
        padding:3px 6px; font-family:monospace;
    }}
    QSpinBox:focus, QDoubleSpinBox:focus {{ border-color:{PAL['accent']}; }}
    QSpinBox::up-button, QDoubleSpinBox::up-button,
    QSpinBox::down-button, QDoubleSpinBox::down-button {{
        background:{PAL['surface']}; border:none; width:16px;
    }}
"""
_COMBO_STYLE  = f"""
    QComboBox {{ background:{PAL['surface']}; color:{PAL['text']};
                 border:1px solid #2a3a5e; border-radius:4px;
                 padding:4px 8px; min-width:120px; }}
    QComboBox::drop-down {{ border:none; }}
    QComboBox QAbstractItemView {{ background:{PAL['surface']}; color:{PAL['text']};
                                   selection-background-color:#2a3a5e; }}
"""
_ST_OK   = f"color:{PAL['ok']};  font-size:8pt; font-family:monospace;"
_ST_ERR  = f"color:{PAL['nc']};  font-size:8pt; font-family:monospace;"
_ST_WARN = f"color:{PAL['warn']}; font-size:8pt; font-family:monospace;"

_SKIP = {"_comment", "_descriptor_keys"}   # meta-keys in the schema file


# ── JSON helpers ──────────────────────────────────────────────────────────────
def _load_json(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"//[^\n]*",     "",  text)
    text = re.sub(r"/\*.*?\*/",    "",  text, flags=re.DOTALL)
    text = re.sub(r",\s*([\]}])", r"\1", text)
    return json.loads(text)


def _get_nested(d: dict, dotted: str):
    """Retrieve a value from a nested dict by dotted path."""
    keys = dotted.split(".")
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _set_nested(d: dict, dotted: str, value):
    """Set a value in a nested dict by dotted path (mutates in place)."""
    keys = dotted.split(".")
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value


# ── Schema descriptor → widget factory ───────────────────────────────────────
class _ValueEditor(QWidget):
    """
    One key/value editor row, appearance driven by a schema descriptor dict.
    Falls back to a plain QLineEdit when no schema is present.
    """
    changed = Signal()

    def __init__(self, key: str, value, descriptor: dict, depth: int = 0,
                 parent=None):
        super().__init__(parent)
        self._key        = key
        self._original   = value
        self._descriptor = descriptor or {}
        self._depth      = depth
        self._dtype      = self._descriptor.get("type", _infer_type(value))

        hl = QHBoxLayout(self)
        hl.setContentsMargins(depth * 20, 2, 4, 2)
        hl.setSpacing(8)

        # ── Label ─────────────────────────────────────────────────────────────
        label_text = self._descriptor.get("label", key)
        unit       = self._descriptor.get("unit", "")
        if unit:
            label_text = f"{label_text}  [{unit}]"
        lbl = QLabel(label_text)
        lbl.setStyleSheet(_SUBKEY_STYLE if depth > 0 else _KEY_STYLE)
        lbl.setFixedWidth(240 - depth * 20)
        lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        tip = self._descriptor.get("description", "")
        if tip:
            lbl.setToolTip(tip)
        hl.addWidget(lbl)

        # ── Value widget ───────────────────────────────────────────────────────
        self._widget = self._make_widget(value)
        if tip:
            self._widget.setToolTip(tip)
        hl.addWidget(self._widget)
        hl.addStretch()

    # ── widget factory ────────────────────────────────────────────────────────
    def _make_widget(self, value):
        dtype = self._dtype
        d     = self._descriptor

        if dtype == "bool":
            w = QCheckBox()
            w.setStyleSheet(_CHECK_STYLE)
            w.setChecked(bool(value))
            w.stateChanged.connect(self.changed)
            return w

        if dtype == "choice":
            w = QComboBox()
            w.setStyleSheet(_COMBO_STYLE)
            for c in d.get("choices", []):
                w.addItem(str(c))
            idx = w.findText(str(value))
            if idx >= 0:
                w.setCurrentIndex(idx)
            w.currentIndexChanged.connect(self.changed)
            return w

        if dtype == "int":
            w = QSpinBox()
            w.setStyleSheet(_SPIN_STYLE)
            w.setRange(int(d.get("min", -2**30)), int(d.get("max", 2**30)))
            w.setSingleStep(int(d.get("step", 1)))
            w.setValue(int(value))
            w.setFixedWidth(130)
            w.valueChanged.connect(self.changed)
            return w

        if dtype == "float":
            w = QDoubleSpinBox()
            w.setStyleSheet(_SPIN_STYLE)
            w.setRange(float(d.get("min", -1e12)), float(d.get("max", 1e12)))
            w.setSingleStep(float(d.get("step", 0.1)))
            w.setDecimals(int(d.get("decimals", 3)))
            w.setValue(float(value))
            w.setFixedWidth(130)
            w.valueChanged.connect(self.changed)
            return w

        # default: string / unrecognised type
        w = QLineEdit(str(value))
        w.setStyleSheet(INPUT_STYLE)
        w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        w.textChanged.connect(self.changed)
        return w

    # ── public API ────────────────────────────────────────────────────────────
    @property
    def key(self):
        return self._key

    @property
    def live_key(self) -> str | None:
        """Dotted config path to broadcast on save, or None."""
        return self._descriptor.get("live")

    def current_value(self):
        w = self._widget
        if isinstance(w, QCheckBox):    return w.isChecked()
        if isinstance(w, QSpinBox):     return w.value()
        if isinstance(w, QDoubleSpinBox): return w.value()
        if isinstance(w, QComboBox):    return w.currentText()
        # QLineEdit — try to coerce back to original type
        raw = w.text()
        try:
            if isinstance(self._original, bool):
                return raw.strip().lower() in ("true", "1", "yes")
            if isinstance(self._original, int):   return int(raw.strip())
            if isinstance(self._original, float): return float(raw.strip())
        except (ValueError, TypeError):
            pass
        return raw

    def is_modified(self) -> bool:
        return self.current_value() != self._original

    def reset(self):
        self._set_widget_value(self._original)

    def accept(self, new_value):
        self._original = new_value
        self._set_widget_value(new_value)

    def _set_widget_value(self, value):
        w = self._widget
        # Block signals so we don't trigger spurious changed emissions
        w.blockSignals(True)
        if isinstance(w, QCheckBox):      w.setChecked(bool(value))
        elif isinstance(w, QSpinBox):     w.setValue(int(value))
        elif isinstance(w, QDoubleSpinBox): w.setValue(float(value))
        elif isinstance(w, QComboBox):
            idx = w.findText(str(value))
            if idx >= 0: w.setCurrentIndex(idx)
        else:
            w.setText(str(value))
        w.blockSignals(False)


def _infer_type(value) -> str:
    if isinstance(value, bool):  return "bool"
    if isinstance(value, int):   return "int"
    if isinstance(value, float): return "float"
    return "string"


# ── Section group ─────────────────────────────────────────────────────────────
class _SectionWidget(QGroupBox):
    """One top-level section, rendered as a titled group box."""

    changed = Signal()

    def __init__(self, section_key: str, section_value, schema: dict,
                 parent=None):
        title = section_key.replace("_", " ").title()
        super().__init__(title, parent)
        self.setStyleSheet(GRP_STYLE)
        self._key     = section_key
        self._schema  = schema or {}
        self._editors: list[_ValueEditor] = []

        vl = QVBoxLayout(self)
        vl.setContentsMargins(8, 20, 8, 8)
        vl.setSpacing(2)
        self._populate(vl, section_value, self._schema, depth=0)

    def _populate(self, layout, value, schema, depth):
        if not isinstance(value, dict):
            desc = schema if isinstance(schema, dict) and "type" in schema else {}
            ed   = _ValueEditor(self._key, value, desc, depth)
            ed.changed.connect(self.changed)
            self._editors.append(ed)
            layout.addWidget(ed)
            return

        for k, v in value.items():
            sub_schema = schema.get(k, {}) if isinstance(schema, dict) else {}
            if isinstance(v, dict):
                sep = QLabel(k.replace("_", " ").title())
                sep.setStyleSheet(
                    f"color:{PAL['accent']}; font-size:8pt; font-weight:bold; "
                    f"margin-top:4px; margin-left:{depth*20}px; "
                    "background:transparent; border:none;"
                )
                layout.addWidget(sep)
                line = QFrame(); line.setFrameShape(QFrame.HLine)
                line.setStyleSheet("color:#2a3a5e;")
                layout.addWidget(line)
                self._populate(layout, v, sub_schema, depth + 1)
            else:
                ed = _ValueEditor(k, v, sub_schema, depth)
                ed.changed.connect(self.changed)
                self._editors.append(ed)
                layout.addWidget(ed)

    # ── collection helpers ────────────────────────────────────────────────────
    def collect(self, original_value):
        if not isinstance(original_value, dict):
            return self._editors[0].current_value() if self._editors else original_value
        return self._collect_dict(original_value)

    def _collect_dict(self, original: dict) -> dict:
        editor_map = {ed.key: ed for ed in self._editors}

        def _recurse(d):
            out = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    out[k] = _recurse(v)
                elif k in editor_map:
                    out[k] = editor_map[k].current_value()
                else:
                    out[k] = v
            return out

        return _recurse(original)

    def live_changes(self, original_value) -> list[tuple[str, object]]:
        """Return [(live_key, new_value)] for editors that are modified and have a live key."""
        out = []
        for ed in self._editors:
            if ed.live_key and ed.is_modified():
                out.append((ed.live_key, ed.current_value()))
        return out

    def is_modified(self):
        return any(ed.is_modified() for ed in self._editors)

    def reset(self):
        for ed in self._editors:
            ed.reset()

    def accept_values(self, new_section_value):
        editor_map = {ed.key: ed for ed in self._editors}

        def _recurse(d):
            for k, v in d.items():
                if isinstance(v, dict):
                    _recurse(v)
                elif k in editor_map:
                    editor_map[k].accept(v)

        if isinstance(new_section_value, dict):
            _recurse(new_section_value)


# ── Main tab ──────────────────────────────────────────────────────────────────
class ConfigurationTab(QWidget):
    """
    Schema-aware configuration editor.

    Emits config_changed(dotted_key: str, value: object) after each successful
    save, for every key whose schema entry includes a "live" field.
    """

    config_changed = Signal(str, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._config_path  = _CONFIG_PATH
        self._schema_path  = _SCHEMA_PATH
        self._data:   dict = {}
        self._schema: dict = {}
        self._sections: dict[str, _SectionWidget] = {}

        self.setStyleSheet(f"background:{PAL['bg']};")
        root_vl = QVBoxLayout(self)
        root_vl.setContentsMargins(6, 6, 6, 6)
        root_vl.setSpacing(6)

        # ── Toolbar ───────────────────────────────────────────────────────────
        tb = QHBoxLayout(); tb.setSpacing(8)
        path_lbl = QLabel(str(self._config_path))
        path_lbl.setStyleSheet(f"color:{PAL['subtext']}; font-size:8pt; font-family:monospace;")
        tb.addWidget(path_lbl, 1)

        self._save_btn   = QPushButton("💾  Save")
        self._reload_btn = QPushButton("🔄  Reload")
        self._reset_btn  = QPushButton("↩  Reset")
        for b in (self._save_btn, self._reload_btn, self._reset_btn):
            b.setStyleSheet(BTN_STYLE)
            b.setFixedWidth(100)
            tb.addWidget(b)

        self._status_lbl = QLabel("")
        self._status_lbl.setStyleSheet(_ST_OK)
        self._status_lbl.setMinimumWidth(280)
        tb.addWidget(self._status_lbl)
        root_vl.addLayout(tb)

        div = QFrame(); div.setFrameShape(QFrame.HLine)
        div.setStyleSheet("color:#2a3a5e;")
        root_vl.addWidget(div)

        # ── Splitter ──────────────────────────────────────────────────────────
        splitter = QSplitter(Qt.Horizontal)
        splitter.setStyleSheet(SPLITTER_STYLE)

        # Left: scrollable editor
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet(f"QScrollArea {{ background:{PAL['bg']}; border:none; }}")
        self._sections_container = QWidget()
        self._sections_container.setStyleSheet(f"background:{PAL['bg']};")
        self._sections_vl = QVBoxLayout(self._sections_container)
        self._sections_vl.setSpacing(8)
        self._sections_vl.setContentsMargins(4, 4, 4, 4)
        self._sections_vl.addStretch()
        scroll.setWidget(self._sections_container)
        splitter.addWidget(scroll)

        # Right: JSON preview
        preview_wrap = QWidget()
        preview_wrap.setStyleSheet(f"background:{PAL['surface']};")
        pw_vl = QVBoxLayout(preview_wrap)
        pw_vl.setContentsMargins(4, 4, 4, 4); pw_vl.setSpacing(4)
        hdr = QLabel("JSON Preview"); hdr.setStyleSheet(_HEADER_STYLE)
        pw_vl.addWidget(hdr)
        self._preview = QTextEdit()
        self._preview.setReadOnly(True)
        self._preview.setFont(QFont("Monospace", 8))
        self._preview.setStyleSheet(
            f"QTextEdit {{ background:{PAL['bg']}; color:{PAL['text']}; "
            f"border:1px solid #2a3a5e; border-radius:4px; }}"
        )
        pw_vl.addWidget(self._preview)
        splitter.addWidget(preview_wrap)
        splitter.setSizes([720, 380])
        root_vl.addWidget(splitter, 1)

        # ── Wiring ────────────────────────────────────────────────────────────
        self._save_btn.clicked.connect(self._save)
        self._reload_btn.clicked.connect(self._reload)
        self._reset_btn.clicked.connect(self._reset)

        self._preview_timer = QTimer(self)
        self._preview_timer.setInterval(400)
        self._preview_timer.timeout.connect(self._update_preview)
        self._preview_timer.start()

        self._load()

    # ── Load ──────────────────────────────────────────────────────────────────
    def _load(self):
        self._schema = {}
        if _SCHEMA_PATH.exists():
            try:
                self._schema = _load_json(_SCHEMA_PATH)
                # strip meta-keys
                for k in _SKIP:
                    self._schema.pop(k, None)
            except Exception as exc:
                self._set_status(f"Schema parse error: {exc}", "warn")

        if not _CONFIG_PATH.exists():
            self._create_default()
        try:
            self._data = _load_json(_CONFIG_PATH)
            self._set_status(f"Loaded: {_CONFIG_PATH.name}", "ok")
        except Exception as exc:
            self._set_status(f"Parse error: {exc}", "err")
            self._data = {}

        self._rebuild_sections()
        self._update_preview()

    def _create_default(self):
        """Write defaults derived from the schema (or a hard-coded fallback)."""
        if self._schema:
            default = {}
            for sec_k, sec_v in self._schema.items():
                if not isinstance(sec_v, dict):
                    continue
                sec = {}
                for field_k, desc in sec_v.items():
                    if isinstance(desc, dict) and "default" in desc:
                        sec[field_k] = desc["default"]
                if sec:
                    default[sec_k] = sec
        else:
            default = {
                "general":          {"facility": "ALS", "beamline": "BL601",
                                     "endstation": "HiRRIXS",
                                     "operator_email": "operator@lbl.gov"},
                "epics":            {"ca_addr_list": "", "ca_auto_addr": True,
                                     "ca_max_array_bytes": 10000000,
                                     "pva_provider": "pva"},
                "data_acquisition": {"default_output_dir": "~/data",
                                     "default_prefix": "scan",
                                     "auto_increment": True,
                                     "default_format": "HDF5"},
                "ui":               {"strip_chart_history_s": 300,
                                     "strip_chart_update_ms": 1000,
                                     "image_rate_limit_hz": 5.0,
                                     "overlay_opacity_rest": 0.35,
                                     "overlay_opacity_hover": 1.0},
                "blop":             {"max_iterations": 50, "init_samples": 8,
                                     "random_seed": 42, "verbose": True},
            }
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(default, indent=2), encoding="utf-8")
        self._set_status("Created default configuration.json", "warn")

    def _rebuild_sections(self):
        while self._sections_vl.count() > 1:
            item = self._sections_vl.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._sections.clear()

        for sec_key, sec_val in self._data.items():
            sec_schema = self._schema.get(sec_key, {})
            sw = _SectionWidget(sec_key, sec_val, sec_schema)
            sw.changed.connect(self._on_any_changed)
            self._sections[sec_key] = sw
            self._sections_vl.insertWidget(self._sections_vl.count() - 1, sw)

    # ── Save ──────────────────────────────────────────────────────────────────
    def _save(self):
        # Collect live-change list before accepting new baseline
        live_pairs: list[tuple[str, object]] = []
        updated: dict = {}

        for sec_key, sw in self._sections.items():
            orig_val = self._data.get(sec_key, {})
            live_pairs.extend(sw.live_changes(orig_val))
            updated[sec_key] = sw.collect(orig_val)

        try:
            serialised = json.dumps(updated, indent=2)
            json.loads(serialised)
        except (TypeError, ValueError) as exc:
            self._set_status(f"Serialisation error: {exc}", "err")
            return

        if _CONFIG_PATH.exists():
            shutil.copy2(_CONFIG_PATH, _CONFIG_PATH.with_suffix(".json.bak"))

        tmp = _CONFIG_PATH.with_suffix(".json.tmp")
        try:
            tmp.write_text(serialised, encoding="utf-8")
            tmp.replace(_CONFIG_PATH)
        except OSError as exc:
            self._set_status(f"Write error: {exc}", "err")
            return

        # Accept new baseline
        self._data = updated
        for sec_key, sw in self._sections.items():
            sw.accept_values(self._data.get(sec_key, {}))

        self._set_status("Saved ✓", "ok")
        self._update_preview()

        # Broadcast live changes
        for dotted_key, new_value in live_pairs:
            self.config_changed.emit(dotted_key, new_value)

    # ── Reload / Reset ────────────────────────────────────────────────────────
    def _reload(self):
        reply = QMessageBox.question(
            self, "Reload",
            "Discard unsaved changes and reload from disk?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            self._load()

    def _reset(self):
        reply = QMessageBox.question(
            self, "Reset",
            "Reset all fields to last-loaded values?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            for sw in self._sections.values():
                sw.reset()
            self._set_status("Reset to last loaded values", "warn")

    # ── Preview ───────────────────────────────────────────────────────────────
    def _update_preview(self):
        preview = {}
        for sec_key, sw in self._sections.items():
            preview[sec_key] = sw.collect(self._data.get(sec_key, {}))
        try:
            text = json.dumps(preview, indent=2)
        except (TypeError, ValueError):
            text = "(unable to serialise current values)"
        self._preview.setPlainText(text)

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _on_any_changed(self):
        dirty = any(sw.is_modified() for sw in self._sections.values())
        self._set_status("Unsaved changes" if dirty else "", "warn" if dirty else "ok")

    def _set_status(self, msg: str, level: str = "ok"):
        self._status_lbl.setStyleSheet(
            {"ok": _ST_OK, "err": _ST_ERR, "warn": _ST_WARN}.get(level, _ST_OK)
        )
        self._status_lbl.setText(msg)

    # ── Convenience: read a value from the loaded config ─────────────────────
    def get(self, dotted_key: str, default=None):
        """Read a value from the currently-loaded config by dotted path."""
        v = _get_nested(self._data, dotted_key)
        return v if v is not None else default
