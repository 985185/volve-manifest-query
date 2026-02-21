# src/volve_query/api.py

from __future__ import annotations

import csv
import io
import os
from typing import Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from .loader import load_manifests_from_wells_dir
from .models import (
    FileEntry,
    HealthResponse,
    Manifest,
    SummaryListResponse,
    SearchResponse,
    WellSummary,
    WellsResponse,
)

app = FastAPI(
    title="Volve Manifest Query API",
    version="0.0.1",
)

# In-memory store
_MANIFESTS: Dict[str, Manifest] = {}
_SOURCE_DIR: str = "wells"


def _split_tags(raw) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x).strip()]
    s = str(raw).strip()
    if not s:
        return []
    # accept "A|B|C" or "A,B"
    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
    elif "," in s:
        parts = [p.strip() for p in s.split(",")]
    else:
        parts = [s]
    return [p for p in parts if p]


def init_app(wells_dir: str) -> None:
    global _MANIFESTS, _SOURCE_DIR
    _SOURCE_DIR = wells_dir
    _MANIFESTS = load_manifests_from_wells_dir(wells_dir)


@app.on_event("startup")
def _startup() -> None:
    wells_dir = os.getenv("VOLVE_WELLS_DIR", "wells")
    init_app(wells_dir)


def _ensure_loaded() -> None:
    if not _MANIFESTS:
        raise HTTPException(status_code=500, detail="Manifests not loaded")


def _bucket_counts(m: Manifest) -> Dict[str, int]:
    return {b: len(nodes) for b, nodes in (m.buckets or {}).items()}


def _foreign_ref_count(m: Manifest) -> int:
    # If loader provides foreign_ref list, use it, else compute from nodes.
    if m.foreign_ref is not None:
        return len(m.foreign_ref)
    count = 0
    for nodes in (m.buckets or {}).values():
        for n in nodes:
            if getattr(n, "foreign_ref_wells", None):
                count += len(n.foreign_ref_wells)
    return count


def _iter_entries(
    q: str,
    well_key: Optional[str],
    bucket: Optional[str],
    include_dirs: bool,
) -> Iterable[FileEntry]:
    q_l = (q or "").lower().strip()

    for wk, m in _MANIFESTS.items():
        if well_key and wk != well_key:
            continue

        for bname, nodes in (m.buckets or {}).items():
            if bucket and bname != bucket:
                continue

            for n in nodes:
                if not include_dirs and n.type == "directory":
                    continue

                filename = getattr(n, "name", "") or ""
                path = getattr(n, "path", "") or ""
                tags_list = _split_tags(getattr(n, "tags", None))

                hay = " ".join(
                    [
                        wk,
                        m.well_id,
                        bname,
                        filename,
                        path,
                        " ".join(tags_list),
                    ]
                ).lower()

                if q_l and q_l not in hay:
                    continue

                yield FileEntry(
                    type=n.type,
                    well_key=wk,
                    well_id=m.well_id,
                    bucket=bname,
                    path=path,
                    filename=filename,
                    tags=tags_list,
                )


def _dedupe_results(results: List[FileEntry], key: str) -> List[FileEntry]:
    """
    key = "path" (best) or "filename"
    """
    seen = set()
    out: List[FileEntry] = []
    for r in results:
        k = r.path if key == "path" else f"{r.well_key}::{r.filename}"
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        wells_loaded=len(_MANIFESTS),
        manifests_loaded=len(_MANIFESTS),
        source_dir=_SOURCE_DIR,
    )


@app.get("/wells", response_model=WellsResponse)
def list_wells() -> WellsResponse:
    _ensure_loaded()
    wells = sorted(_MANIFESTS.keys())
    return WellsResponse(count=len(wells), wells=wells)


@app.get("/wells/{well_key}/manifest", response_model=dict)
def get_manifest(well_key: str) -> dict:
    _ensure_loaded()
    m = _MANIFESTS.get(well_key)
    if not m:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    # Return raw-ish normalized manifest JSON (debug/downstream)
    return {
        "well_id": m.well_id,
        "buckets": {b: [n.model_dump() for n in nodes] for b, nodes in (m.buckets or {}).items()},
        "bucket_counts": _bucket_counts(m),
        "foreign_ref": m.foreign_ref or [],
    }


@app.get("/wells/{well_key}/summary", response_model=WellSummary)
def well_summary(well_key: str) -> WellSummary:
    _ensure_loaded()
    m = _MANIFESTS.get(well_key)
    if not m:
        raise HTTPException(status_code=404, detail="Unknown well_key")

    counts = _bucket_counts(m)
    total_files = sum(counts.values())
    return WellSummary(
        well_key=well_key,
        well_id=m.well_id,
        bucket_counts=counts,
        total_files=total_files,
        foreign_reference_count=_foreign_ref_count(m),
    )


@app.get("/summary", response_model=SummaryListResponse)
def summary(limit: int = Query(100, ge=1, le=5000), offset: int = Query(0, ge=0)) -> SummaryListResponse:
    _ensure_loaded()
    wells = sorted(_MANIFESTS.keys())
    total = len(wells)

    page_keys = wells[offset : offset + limit]
    summaries: List[WellSummary] = []
    for wk in page_keys:
        m = _MANIFESTS[wk]
        counts = _bucket_counts(m)
        summaries.append(
            WellSummary(
                well_key=wk,
                well_id=m.well_id,
                bucket_counts=counts,
                total_files=sum(counts.values()),
                foreign_reference_count=_foreign_ref_count(m),
            )
        )

    return SummaryListResponse(
        total=total,
        offset=offset,
        limit=limit,
        count=len(summaries),
        wells=summaries,
    )


@app.get("/wells/{well_key}/buckets", response_model=dict)
def well_buckets(well_key: str) -> dict:
    _ensure_loaded()
    m = _MANIFESTS.get(well_key)
    if not m:
        raise HTTPException(status_code=404, detail="Unknown well_key")

    buckets = sorted(list((m.buckets or {}).keys()))
    return {"well_key": well_key, "buckets": buckets}


@app.get("/wells/{well_key}/buckets/{bucket_name}", response_model=dict)
def bucket_files(
    well_key: str,
    bucket_name: str,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> dict:
    _ensure_loaded()
    m = _MANIFESTS.get(well_key)
    if not m:
        raise HTTPException(status_code=404, detail="Unknown well_key")

    nodes = (m.buckets or {}).get(bucket_name)
    if nodes is None:
        raise HTTPException(status_code=404, detail="Unknown bucket for this well")

    total = len(nodes)
    page = nodes[offset : offset + limit]

    files = []
    for n in page:
        files.append(
            {
                "type": n.type,
                "well_key": well_key,
                "well_id": m.well_id,
                "bucket": bucket_name,
                "path": n.path,
                "filename": n.name,
                "tags": _split_tags(n.tags),
            }
        )

    return {
        "well_key": well_key,
        "bucket": bucket_name,
        "total": total,
        "offset": offset,
        "limit": limit,
        "files": files,
    }


@app.get("/wells/{well_key}/foreign-references", response_model=dict)
def foreign_references(well_key: str) -> dict:
    _ensure_loaded()
    m = _MANIFESTS.get(well_key)
    if not m:
        raise HTTPException(status_code=404, detail="Unknown well_key")

    refs = m.foreign_ref or []
    return {"well_key": well_key, "count": len(refs), "references": refs}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1),
    well_key: Optional[str] = Query(None),
    bucket: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    include_dirs: bool = Query(False, description="Include directory entries in results"),
    dedupe: bool = Query(True, description="Dedupe results (recommended)"),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> SearchResponse:
    _ensure_loaded()

    all_results = list(_iter_entries(q=q, well_key=well_key, bucket=bucket, include_dirs=include_dirs))
    if dedupe:
        all_results = _dedupe_results(all_results, key=dedupe_key)

    total = len(all_results)
    page = all_results[offset : offset + limit]

    return SearchResponse(
        query=q,
        filters={"well_key": well_key, "bucket": bucket},
        total=total,
        offset=offset,
        limit=limit,
        results=page,
    )


@app.get("/wells/{well_key}/search", response_model=SearchResponse)
def search_in_well(
    well_key: str,
    q: str = Query(..., min_length=1),
    bucket: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> SearchResponse:
    _ensure_loaded()
    if well_key not in _MANIFESTS:
        raise HTTPException(status_code=404, detail="Unknown well_key")

    all_results = list(_iter_entries(q=q, well_key=well_key, bucket=bucket, include_dirs=include_dirs))
    if dedupe:
        all_results = _dedupe_results(all_results, key=dedupe_key)

    total = len(all_results)
    page = all_results[offset : offset + limit]

    return SearchResponse(
        query=q,
        filters={"well_key": well_key, "bucket": bucket},
        total=total,
        offset=offset,
        limit=limit,
        results=page,
    )


def _as_csv_rows(entries: List[FileEntry]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["well_key", "well_id", "bucket", "filename", "path", "tags"])
    for e in entries:
        writer.writerow([e.well_key, e.well_id, e.bucket, e.filename, e.path, "|".join(e.tags)])
    return buf.getvalue()


@app.get("/search.csv", response_class=PlainTextResponse)
def search_csv(
    q: str = Query(..., min_length=1),
    well_key: Optional[str] = Query(None),
    bucket: Optional[str] = Query(None),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> PlainTextResponse:
    _ensure_loaded()
    results = list(_iter_entries(q=q, well_key=well_key, bucket=bucket, include_dirs=include_dirs))
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    csv_text = _as_csv_rows(results)
    return PlainTextResponse(content=csv_text, media_type="text/csv")


@app.get("/wells/{well_key}/search.csv", response_class=PlainTextResponse)
def search_in_well_csv(
    well_key: str,
    q: str = Query(..., min_length=1),
    bucket: Optional[str] = Query(None),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> PlainTextResponse:
    _ensure_loaded()
    if well_key not in _MANIFESTS:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    results = list(_iter_entries(q=q, well_key=well_key, bucket=bucket, include_dirs=include_dirs))
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    csv_text = _as_csv_rows(results)
    return PlainTextResponse(content=csv_text, media_type="text/csv")