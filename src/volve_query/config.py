from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    # Default wells directory is repo-local ./wells
    wells_dir: Path = Path("wells").resolve()
    # Max limit guardrails
    default_limit: int = 100
    max_limit: int = 5000