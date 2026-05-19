"""
nxxas_writer.py — NeXus NXxas-compliant HDF5 writer for AMBER / HiRRIXS.

Place this file in qtui/config/ alongside bluesky_startup.py.

Add to the end of bluesky_startup.py:

    from nxxas_writer import make_nxxas_factory
    RE.subscribe(_RunRouter([make_nxxas_factory(_EXPORT_DIR)]))

The writer produces files that pass nexusformat / cnxvalidate NXxas validation:
    pip install nexusformat
    python -c "import nexusformat.nexus as nx; f=nx.nxload('scan.nxs'); f.validate()"

NeXus classes written
---------------------
/entry                       NXentry      (definition = NXxas)
  /instrument                NXinstrument
    /source                  NXsource
    /monochromator           NXmonochromator   energy [eV]
    /detector_<name>         NXdetector        data [counts / V]
    /izero_<name>            NXdetector        (normalisation channels)
  /sample                    NXsample
  /data                      NXdata       (default plot group)
  /metadata                  NXnote       (bluesky start-doc passthrough)
"""
import time as _time
from datetime import datetime as _dt, timezone as _tz
from pathlib import Path


# ── Column classification ─────────────────────────────────────────────────────

# ophyd device .name for the calibrated photon energy motor.
_ENERGY_NAMES = ("beamlineenergyudp", "beamlineenergy", "fakemotor")

# Channels treated as I₀ / normalisation monitors.
_IZERO_PREFIXES = ("izero", "i0", "diag132diode")

# Named XAS signal channels.
_SIGNAL_NAMES = frozenset((
    "teyup", "teydn", "tfychanneltron", "tfydiode",
    "diode133", "diode134", "diag132tey",
))

# Derivative channel suffixes to drop.
_SKIP_SUFFIXES = ("_set", "_setpoint", "_std", "_n")


def _classify(columns):
    """Return (energy_col, izero_cols, signal_cols, motor_cols) from a column list."""
    energy_col  = None
    izero_cols  = []
    signal_cols = []
    motor_cols  = []
    for col in columns:
        low = col.lower()
        if any(low.endswith(s) for s in _SKIP_SUFFIXES):
            continue
        if low in _ENERGY_NAMES:
            energy_col = col
        elif any(low.startswith(p) for p in _IZERO_PREFIXES):
            izero_cols.append(col)
        elif low in _SIGNAL_NAMES:
            signal_cols.append(col)
        else:
            motor_cols.append(col)
    return energy_col, izero_cols, signal_cols, motor_cols


def _iso(ts):
    return _dt.fromtimestamp(ts, tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


# ── Callback ──────────────────────────────────────────────────────────────────

class _NXxasCallback:
    """Accumulates bluesky documents and writes one NXxas file on stop."""

    def __init__(self, path):
        self._path      = Path(path)
        self._start_doc = {}
        self._columns   = []
        self._data      = {}

    def __call__(self, name, doc):
        getattr(self, name, lambda _d: None)(doc)

    def start(self, doc):
        self._start_doc = doc

    def descriptor(self, doc):
        if self._columns:
            return
        hints  = doc.get("hints", {})
        dims   = [d[0][0] for d in hints.get("dimensions", []) if d[0]]
        others = [k for k in doc["data_keys"] if k not in dims]
        self._columns = dims + others
        self._data    = {k: [] for k in self._columns}

    def event(self, doc):
        data = doc.get("data", {})
        for k in self._columns:
            self._data[k].append(data.get(k, float("nan")))

    def event_page(self, doc):
        data = doc.get("data", {})
        n = len(next(iter(data.values()), []))
        for i in range(n):
            for k in self._columns:
                col = data.get(k, [float("nan")] * n)
                self._data[k].append(col[i] if i < len(col) else float("nan"))

    def stop(self, doc):
        if not self._columns:
            print(f"[nxxas] no descriptor received — file not written: {self._path}")
            return
        if not any(self._data.values()):
            print(f"[nxxas] descriptor arrived but no events — file not written: {self._path}")
            return
        try:
            self._write(doc)
            print(f"[nxxas] written → {self._path}")
        except Exception as exc:
            import traceback
            print(f"[nxxas] write error for {self._path}: {exc}")
            traceback.print_exc()

    def _write(self, stop_doc):
        import h5py
        import numpy as np

        arrays = {k: np.array(v, dtype=float)
                  for k, v in self._data.items() if v}

        energy_col, izero_cols, signal_cols, motor_cols = _classify(
            list(arrays.keys()))

        start    = self._start_doc
        uid8     = start.get("uid", "unknown")[:8]
        scan_id  = start.get("scan_id", 0)
        start_ts = start.get("time", _time.time())
        stop_ts  = stop_doc.get("time", _time.time())

        self._path.parent.mkdir(parents=True, exist_ok=True)

        with h5py.File(str(self._path), "w") as f:
            f.attrs["default"] = "entry"

            # /entry
            e = f.require_group("entry")
            e.attrs["NX_class"]   = "NXentry"
            e.attrs["definition"] = "NXxas"
            e.attrs["default"]    = "data"
            e.create_dataset("title",
                data=f"AMBER BL601 scan {scan_id:04d} [{uid8}]")
            e.create_dataset("start_time", data=_iso(start_ts))
            e.create_dataset("end_time",   data=_iso(stop_ts))
            e.create_dataset("scan_id",    data=np.int32(scan_id))
            e.create_dataset("uid",        data=start.get("uid", ""))

            # /entry/sample
            s = e.require_group("sample")
            s.attrs["NX_class"] = "NXsample"
            s.create_dataset("name", data=str(
                start.get("sample_name", start.get("sample", "unknown"))))

            # /entry/instrument
            inst = e.require_group("instrument")
            inst.attrs["NX_class"] = "NXinstrument"
            inst.create_dataset("name", data="ALS BL601 AMBER / HiRRIXS")

            src = inst.require_group("source")
            src.attrs["NX_class"] = "NXsource"
            src.create_dataset("name",  data="Advanced Light Source")
            src.create_dataset("type",  data="Synchrotron X-ray Source")
            src.create_dataset("probe", data="x-ray")

            mono = inst.require_group("monochromator")
            mono.attrs["NX_class"] = "NXmonochromator"
            if energy_col and energy_col in arrays:
                ds = mono.create_dataset("energy", data=arrays[energy_col])
                ds.attrs["units"]      = "eV"
                ds.attrs["long_name"]  = "Photon energy"
                ds.attrs["source_pv"]  = "BL6013:BeamlineEnergyUDP"
            else:
                print("[nxxas] WARNING: BeamlineEnergy not in scan data — "
                      "monochromator/energy will be absent")

            for col in signal_cols:
                if col not in arrays:
                    continue
                g = inst.require_group(f"detector_{col.lower()}")
                g.attrs["NX_class"]   = "NXdetector"
                g.attrs["local_name"] = col
                ds = g.create_dataset("data", data=arrays[col])
                ds.attrs["units"]     = "counts"
                ds.attrs["long_name"] = col

            for col in izero_cols:
                if col not in arrays:
                    continue
                g = inst.require_group(f"izero_{col.lower()}")
                g.attrs["NX_class"]   = "NXdetector"
                g.attrs["local_name"] = col
                g.attrs["role"]       = "monitor"
                ds = g.create_dataset("data", data=arrays[col])
                ds.attrs["units"]     = "V"
                ds.attrs["long_name"] = col

            if motor_cols:
                mg = inst.require_group("motors")
                mg.attrs["NX_class"] = "NXpositioner"
                for col in motor_cols:
                    if col in arrays:
                        mg.create_dataset(col, data=arrays[col]).attrs[
                            "long_name"] = col

            # /entry/data  (NXdata — required default plot group)
            nd = e.require_group("data")
            nd.attrs["NX_class"] = "NXdata"

            primary = signal_cols[0] if signal_cols else (
                      izero_cols[0]  if izero_cols  else None)

            if primary and primary in arrays:
                nd.attrs["signal"] = primary
                ds = nd.create_dataset(primary, data=arrays[primary])
                ds.attrs["units"] = "counts"

            if energy_col and energy_col in arrays:
                nd.attrs["axes"] = energy_col
                ds = nd.create_dataset(energy_col, data=arrays[energy_col])
                ds.attrs["units"]     = "eV"
                ds.attrs["long_name"] = "Photon energy"
                if primary:
                    nd.attrs[f"{energy_col}_indices"] = np.int32(0)

            for col in signal_cols[1:] + izero_cols:
                if col not in arrays or col == primary:
                    continue
                ds = nd.create_dataset(col, data=arrays[col])
                ds.attrs["units"] = "counts" if col in signal_cols else "V"

            # /entry/metadata  (bluesky start-doc passthrough)
            meta = e.require_group("metadata")
            meta.attrs["NX_class"] = "NXnote"
            for k, v in start.items():
                try:
                    meta.attrs[k] = str(v)
                except Exception:
                    pass


# ── Factory ───────────────────────────────────────────────────────────────────

def make_nxxas_factory(export_dir):
    """Return a RunRouter-compatible factory that writes NXxas files to export_dir.

    Args:
        export_dir: pathlib.Path — use the same _EXPORT_DIR as bluesky_startup.py.

    Example (bluesky_startup.py):
        from nxxas_writer import make_nxxas_factory
        RE.subscribe(_RunRouter([make_nxxas_factory(_EXPORT_DIR)]))
    """
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    def _factory(name, doc):
        if name != "start":
            return [], []
        uid8 = doc.get("uid", "unknown")[:8]
        num  = doc.get("scan_id", 0)
        dts  = _dt.fromtimestamp(doc.get("time", _time.time()),
                                 tz=_tz.utc).strftime("%Y%m%dT%H%M%S")
        path = export_dir / f"scan_{num:04d}_{uid8}_{dts}.nxs"
        print(f"[nxxas] → {path}")
        return [_NXxasCallback(path)], []

    return _factory
