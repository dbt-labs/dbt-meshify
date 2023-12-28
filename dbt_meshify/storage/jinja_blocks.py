from dataclasses import dataclass
from pathlib import Path


@dataclass
class JinjaBlock:
    """
    A common data structure for tracking blocks of text that represent Jinja blocks.
    """

    path: Path
    start: int
    end: int
    content: str
