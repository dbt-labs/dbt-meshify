import re
from dataclasses import dataclass
from pathlib import Path
from typing import Set, Tuple


@dataclass
class JinjaBlock:
    """
    A data structure for tracking Jinja blocks of text. Includes the start and end character positions, and the content of the block
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
            r"{%-?\s+" + block_type + r"\s+" + name + r"([(a-zA-Z0-9=,_ )]*)\s-?%}",
            file_content,
            re.MULTILINE,
        ):
            start = match.span()[0]  # .span() gives tuple (start, end)
            start_line = start  # file_content[:start].count("\n")
            break

        if start_line is None:
            raise Exception(f"Unable to find a {block_type} block with the name {name}.")

        for match in re.finditer(
            r"{%-?\s+end" + block_type + r"\s+-?%}", file_content, re.MULTILINE
        ):
            end = match.span()[1]  # .span() gives tuple (start, end)
            new_end_line = end  # file_content[:start].count("\n")

            if new_end_line >= start_line:
                end_line = new_end_line
                break

        if end_line is None:
            raise Exception(f"Unable to find a the closing end{block_type} block for {name}.")

        return start_line, end_line

    @staticmethod
    def isolate_content(file_content: str, start: int, end: int) -> str:
        """Given content, a start position, and an end position, return the content of a Jinja block."""
        return file_content[start:end]

    @classmethod
    def from_file(cls, path: Path, block_type: str, name: str) -> "JinjaBlock":
        """Find a specific Jinja block within a file, based on the block type and the name."""

        file_content = path.read_text()
        start, end = cls.find_block_range(file_content, block_type, name)
        content = cls.isolate_content(file_content=file_content, start=start, end=end)

        return cls(
            path=path, block_type=block_type, name=name, start=start, end=end, content=content
        )


def find_doc_reference(content: str) -> Set[str]:
    """Find all doc block references within a string."""
    matches = re.findall(r"{{\sdoc\(\'?\"?([a-zA-Z0-9_\-\.]+)\'?\"?\)\s}}", content)

    return set(matches)
