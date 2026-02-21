from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .models import Manifest


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _find_manifest_json_in_well_dir(well_dir: Path) -> Optional[Path]:
    """
    Prefer common names first, then fall back to the first *.json file.
    """
    preferred = [
        "manifest.json",
        "manifest.normalized.json",
        "normalized_manifest.json",
        "manifest_normalized.json",
        "index.json",
    ]
    for name in preferred:
        p = well_dir / name
        if p.exists() and p.is_file():
            return p

    # fallback: first json file
    json_files = sorted([p for p in well_dir.glob("*.json") if p.is_file()])
    return json_files[0] if json_files else None


def _normalize_loaded_key(folder_name: str, manifest: Manifest) -> str:
    """
    Use folder name as well_key if it matches expected pattern, otherwise derive from well_id.
    """
    if folder_name and folder_name.strip():
        return folder_name.strip()
    return manifest.well_id.replace("/", "_").strip()


def load_manifests_from_wells_dir(wells_dir: str) -> Dict[str, Manifest]:
    """
    Loads per-well manifest JSON.

    Supports two layouts:
      A) wells_dir contains subfolders per well:
         wells/15_9-F-9A/manifest.json
         wells/15_9-F-1/manifest.json
         ...
      B) wells_dir contains json files directly:
         wells/15_9-F-9A.json
         wells/15_9-F-1.json
         ...
    """
    base = Path(wells_dir)
    if not base.exists():
        raise FileNotFoundError(f"wells_dir not found: {wells_dir}")
    if not base.is_dir():
        raise NotADirectoryError(f"wells_dir is not a directory: {wells_dir}")

    manifests: Dict[str, Manifest] = {}

    # Case A: subfolders per well
    subdirs = sorted([p for p in base.iterdir() if p.is_dir()])
    if subdirs:
        for well_dir in subdirs:
            manifest_path = _find_manifest_json_in_well_dir(well_dir)
            if not manifest_path:
                # Skip silently; you can change to raise if you want strictness
                continue

            data = _read_json(manifest_path)
            m = Manifest.model_validate(data)
            well_key = _normalize_loaded_key(well_dir.name, m)
            manifests[well_key] = m

        if manifests:
            return manifests

    # Case B: JSON files directly in wells_dir
    json_files = sorted([p for p in base.glob("*.json") if p.is_file()])
    for jf in json_files:
        data = _read_json(jf)
        m = Manifest.model_validate(data)
        well_key = jf.stem  # filename without .json
        manifests[well_key] = m

    if not manifests:
        # give a useful diagnostic
        hint = (
            f"No manifests loaded from '{wells_dir}'.\n"
            f"Expected either:\n"
            f"  wells/<WELL_KEY>/manifest.json (or any *.json inside each well folder)\n"
            f"OR\n"
            f"  wells/<WELL_KEY>.json\n"
        )
        raise RuntimeError(hint)

    return manifests


# Backwards-compatible alias (your CLI was importing this earlier)
def load_all_manifests(wells_dir: str) -> Dict[str, Manifest]:
    return load_manifests_from_wells_dir(wells_dir)