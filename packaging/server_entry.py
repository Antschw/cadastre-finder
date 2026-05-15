"""Point d'entrée uvicorn pour le bundle PyInstaller.

Le bloc frozen doit s'exécuter AVANT tout import de duckdb ou du projet,
car duckdb.connect est patché pour injecter extension_directory.
"""
import sys
import os
from pathlib import Path


def _patch_duckdb_for_bundle() -> None:
    """Redirige les connexions DuckDB vers l'extension spatiale bundlée."""
    import duckdb

    ext_dir = str(Path(sys._MEIPASS) / "duckdb_ext")  # type: ignore[attr-defined]
    _orig_connect = duckdb.connect

    def _patched_connect(database=":memory:", read_only=False, config=None, **kwargs):
        cfg = dict(config or {})
        cfg.setdefault("extension_directory", ext_dir)
        return _orig_connect(database, read_only=read_only, config=cfg, **kwargs)

    duckdb.connect = _patched_connect  # type: ignore[method-assign]


def _ensure_spatial_extension() -> None:
    """Si l'extension spatiale n'est pas bundlée, tente un téléchargement via INSTALL spatial."""
    import duckdb

    ext_dir = Path(sys._MEIPASS) / "duckdb_ext"  # type: ignore[attr-defined]
    found = list(ext_dir.glob("*/*/spatial.duckdb_extension"))
    if not found:
        print("[server_entry] Extension spatiale non bundlée — tentative de téléchargement...", flush=True)
        try:
            con = duckdb.connect(":memory:")
            con.execute("INSTALL spatial; LOAD spatial;")
            con.close()
            print("[server_entry] Extension spatiale installée avec succès.", flush=True)
        except Exception as e:
            print(f"[server_entry] Avertissement : extension spatiale indisponible : {e}", flush=True)


if getattr(sys, "frozen", False):
    _patch_duckdb_for_bundle()
    _ensure_spatial_extension()

import uvicorn  # noqa: E402 — doit être après le patch

if __name__ == "__main__":
    uvicorn.run(
        "cadastre_finder.api.main:app",
        host="127.0.0.1",
        port=8000,
        log_level="warning",
    )
