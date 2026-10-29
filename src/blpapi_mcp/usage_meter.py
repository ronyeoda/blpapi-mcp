"""Bloomberg usage meter: one line per tool call, in the jarvis meter format (door "mcp").

The writer is jarvis's ``marketdb/bbg_meter.py`` (its copy in ``~/.marketdb/bin``), loaded by path (stdlib only), so the
lines land next to the xbbg ones in ``~/.marketdb/bbg_usage`` and reach the VPS's
Bloomberg usage page with them. ``JARVIS_BBG_METER_PY`` points at another copy;
when the file is missing the meter is off and the tools run as before.

Rows are caught in ``_csv`` (every data tool returns through it) via a context
variable, so points per (ticker, field) are counted from what Bloomberg returned.
"""
from __future__ import annotations

import contextvars
import functools
import importlib.util
import inspect
import os
import time
from typing import Any, Optional

# jarvis scripts\bbg-usage\install_meter.ps1 keeps this copy current.
_DEFAULT = os.path.join(os.path.expanduser("~"), ".marketdb", "bin", "bbg_meter.py")
_KEYS = {"ticker", "security", "date", "time", "datetime", "error", "id"}
_rows: contextvars.ContextVar = contextvars.ContextVar("blpapi_mcp_rows", default=None)
_meter_mod: Any = None
_loaded = False


def _meter() -> Any:
    global _meter_mod, _loaded
    if _loaded:
        return _meter_mod
    _loaded = True
    path = os.environ.get("JARVIS_BBG_METER_PY", _DEFAULT)
    try:
        if os.path.isfile(path):
            spec = importlib.util.spec_from_file_location("jarvis_bbg_meter_mcp", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _meter_mod = mod
    except Exception:  # noqa: BLE001
        _meter_mod = None
    return _meter_mod


def capture(rows: list) -> None:
    holder = _rows.get()
    if holder is not None:
        holder.extend(rows)


def _series(rows: list, default_ticker: Optional[str]) -> list:
    acc: dict = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        t = r.get("ticker") or r.get("security") or default_ticker
        if not t:
            continue
        for k, v in r.items():
            if k in _KEYS or v in (None, ""):
                continue
            acc[(t, k)] = acc.get((t, k), 0) + 1
    return [[t, f, n] for (t, f), n in acc.items()]


def metered(fn):
    """Wrap one async tool. Signature and name are kept (FastMCP builds the schema from them)."""
    sig = inspect.signature(fn)

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        meter = _meter()
        if meter is None:
            return await fn(*args, **kwargs)
        holder: list = []
        token = _rows.set(holder)
        started = time.perf_counter()
        err: Optional[BaseException] = None
        try:
            return await fn(*args, **kwargs)
        except BaseException as e:  # noqa: BLE001 - recorded, then re-raised
            err = e
            raise
        finally:
            _rows.reset(token)
            try:
                a = sig.bind_partial(*args, **kwargs).arguments
                tickers = list(a.get("tickers") or ([a["ticker"]] if a.get("ticker") else []))
                flds = list(a.get("flds") or [])
                kind = fn.__name__
                series = _series(holder, tickers[0] if len(tickers) == 1 else None)
                requested = None
                if kind == "bdp":
                    requested = len(tickers) * max(len(flds), 1)
                elif kind == "bdh":
                    wd = meter.weekdays(a.get("start_date"), a.get("end_date", "today"))
                    requested = None if wd is None else len(tickers) * max(len(flds), 1) * wd
                meter.record({
                    "door": "mcp", "caller": "blpapi-mcp", "kind": kind,
                    "tickers": len(tickers), "fields": len(flds),
                    "start": a.get("start_date") or a.get("date"), "end": a.get("end_date"),
                    "requested": requested, "points": sum(s[2] for s in series), "rows": len(holder),
                    "cached": 0, "errors": 1 if err is not None else 0, "ok": err is None,
                    "error": str(err)[:200] if err is not None else None,
                    "ms": round((time.perf_counter() - started) * 1000, 1),
                    "series": series or [[t, f, None] for t in tickers for f in (flds or [""])],
                })
            except Exception:  # noqa: BLE001 - a meter never fails the tool
                pass

    return wrapper


def meter_tools(mcp) -> None:
    """Make every later ``@mcp.tool(...)`` registration wrap its function in ``metered``."""
    register = mcp.tool

    def tool(*args, **kwargs):
        deco = register(*args, **kwargs)
        return lambda fn: deco(metered(fn))

    mcp.tool = tool
