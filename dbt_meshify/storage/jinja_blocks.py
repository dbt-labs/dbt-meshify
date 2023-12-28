import re
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple


@dataclass
class JinjaBlock:
    """
    A common data structure for tracking blocks of text that represent Jinja blocks.
    """

    path: Path
    block_type: str
    name: str
    start: int
    end: int
    content: str

    @staticmethod
    def find_block_range(file_content: str, block_type: str, name: str) -> Tuple[int, int]:
        """Find the line number that a block started."""
        start_line = None
        end_line = None

        for match in re.finditer(
            r"{%\s+" + block_type + r"\s+" + name + r"\s+%}", file_content, re.MULTILINE
        ):
            start = match.span()[0]  # .span() gives tuple (start, end)
            start_line = file_content[:start].count("\n")
            break

        if start_line is None:
            raise Exception(f"Unable to find a {block_type} block with the name {name}.")

        for match in re.finditer(r"{%\s+end" + block_type + r"\s+%}", file_content, re.MULTILINE):
            start = match.span()[0]  # .span() gives tuple (start, end)
            new_end_line = file_content[:start].count("\n")

            if new_end_line >= start_line:
                end_line = new_end_line
                break

        if end_line is None:
            raise Exception(f"Unable to find a the closing end{block_type} block for {name}.")

        return start_line, end_line

    @staticmethod
    def isolate_content_from_line_range(file_content: str, start: int, end: int) -> str:
        """Given content, a start line number, and an end line number, return the content of a Jinja block."""
        print(file_content.split("\n")[start + 1 :])
        return "/n".join(file_content.split("\n")[start + 1 : end])

    @classmethod
    def from_file(cls, path: Path, block_type: str, name: str) -> "JinjaBlock":
        """Find a specific Jinja block within a file, based on the block type and the name."""

        file_content = open(path).read()
        start, end = cls.find_block_range(file_content, block_type, name)
        content = cls.isolate_content_from_line_range(
            file_content=file_content, start=start, end=end
        )

        return cls(
            path=path, block_type=block_type, name=name, start=start, end=end, content=content
        )
