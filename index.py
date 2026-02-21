from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from .models import Manifest


def _split_tags(tags: Any) -> List[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    if isinstance(tags, str):
        parts = [p.strip() for p in tags.replace(",", "|").split("|")]
        return [p for p in parts if p]
    return [str(tags).strip()]


def _filename_from_path(path: str) -> str:
    try:
        return PurePosixPath(path).name
    except Exception:
        return path.split("/")[-1]


@dataclass(frozen=True)
class SearchHit:
    type: str
    well_key: str
    well_id: str
    bucket: str
    path: str
    filename: str
    tags: List[str]


class ManifestIndex:
    """
    Deterministic in-memory index built from manifest.json files.
    No database. No external deps.
    """

    def __init__(self, manifests: Dict[str, Manifest]):
        self.manifests = manifests
        self._wells_sorted = sorted(manifests.keys())
        self._flat: List[SearchHit] = []
        self._build()

    def _build(self) -> None:
        flat: List[SearchHit] = []
        for well_key, manifest in self.manifests.items():
            # Fix: use .well_id attribute directly (no resolved_well_id() method)
            well_id = manifest.well_id or well_key.replace("_", "/")
            for bucket_name, entries in (manifest.buckets or {}).items():
                for e in entries or []:
                    # Fix: ManifestNode is a Pydantic model — use attribute access, not .get()
                    path = e.path
                    if not path:
                        continue
                    etype = e.type
                    filename = e.name or _filename_from_path(path)
                    tags = _split_tags(e.tags)
                    flat.append(
                        SearchHit(
                            type=etype,
                            well_key=well_key,
                            well_id=well_id,
                            bucket=bucket_name,
                            path=path,
                            filename=filename,
                            tags=tags,
                        )
                    )
        self._flat = flat

    def wells(self) -> List[str]:
        return self._wells_sorted

    def summary(self, well_key: str) -> Dict[str, Any]:
        m = self.manifests[well_key]
        # Fix: use .well_id directly
        well_id = m.well_id or well_key.replace("_", "/")
        bucket_counts = m.bucket_counts or {k: len(v) for k, v in (m.buckets or {}).items()}
        total_files = int(sum(bucket_counts.values()))
        foreign_count = int(len(m.foreign_ref or []))
        return {
            "well_key": well_key,
            "well_id": well_id,
            "bucket_counts": bucket_counts,
            "total_files": total_files,
            "foreign_reference_count": foreign_count,
        }

    def buckets(self, well_key: str) -> List[str]:
        m = self.manifests[well_key]
        return sorted(list((m.buckets or {}).keys()))

    def bucket_files(self, well_key: str, bucket: str) -> List[SearchHit]:
        return [h for h in self._flat if h.well_key == well_key and h.bucket == bucket]

    def manifest_raw(self, well_key: str) -> Dict[str, Any]:
        return self.manifests[well_key].model_dump(mode="json")

    def foreign_refs(self, well_key: str) -> List[Any]:
        return list(self.manifests[well_key].foreign_ref or [])

    def search(
        self,
        q: str,
        well_key: Optional[str] = None,
        bucket: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[int, List[SearchHit]]:
        qn = q.strip().lower()
        if not qn:
            return 0, []

        def hit(h: SearchHit) -> bool:
            if well_key and h.well_key != well_key:
                return False
            if bucket and h.bucket != bucket:
                return False

            if qn in h.filename.lower():
                return True
            if qn in h.path.lower():
                return True
            if qn in h.bucket.lower():
                return True
            if qn in h.well_key.lower():
                return True
            if qn in h.well_id.lower():
                return True
            if any(qn in t.lower() for t in h.tags):
                return True

            # Normalize separators so 15_9-F-9A can match 15/9-F-9A
            q_slash = qn.replace("_", "/")
            if q_slash != qn and (q_slash in h.well_id.lower() or q_slash in h.path.lower()):
                return True

            return False

        matches = [h for h in self._flat if hit(h)]
        total = len(matches)
        page = matches[offset : offset + limit]
        return total, page
