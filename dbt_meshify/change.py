import dataclasses
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Protocol


class Operation(str, Enum):
    """An operation describes the type of work being performed."""

    Add = "add"
    Update = "update"
    Remove = "remove"
    Copy = "copy"
    Move = "move"


class EntityType(str, Enum):
    """An EntityType represents the type of entity being operated on in a Change"""

    Model = "model"
    Analysis = "analysis"
    Test = "test"
    Snapshot = "snapshot"
    Operation = "operation"
    Seed = "seed"
    RPCCall = "rpc"
    SqlOperation = "sql_operation"
    Documentation = "doc"
    Source = "source"
    Macro = "macro"
    Exposure = "exposure"
    Metric = "metric"
    Group = "group"
    SemanticModel = "semantic_model"
    Project = "project"
    Code = "code"

    def pluralize(self) -> str:
        if self is self.Analysis:
            return "analyses"
        return f"{self.value}s"


class Change(Protocol):
    """A change represents a unit of work that should be performed within a dbt project."""

    operation: Operation
    entity_type: EntityType
    identifier: str
    path: Path


@dataclasses.dataclass
class BaseChange:
    operation: Operation
    entity_type: EntityType
    identifier: str
    path: Path


@dataclasses.dataclass
class ResourceChange(BaseChange):
    """A ResourceChange represents a unit of work that should be performed on a Resource in a dbt project."""

    data: Dict


@dataclasses.dataclass
class FileChange(BaseChange):
    """A ResourceChange represents a unit of work that should be performed on a File in a dbt project."""

    data: Optional[str] = None
    source: Optional[Path] = None


class ChangeSet:
    """A collection of Changes that will be performed"""

    def __init__(self) -> None:
        self.changes: List[Change] = []
        self.step = -1

    def add(self, change: Change) -> None:
        """Add a change to the ChangeSet."""
        self.changes.append(change)

    def extend(self, changes: Iterable[Change]) -> None:
        """Extend a ChangeSet with an iterable of Changes."""
        self.changes.extend(changes)

    def __iter__(self) -> "ChangeSet":
        return self

    def __next__(self) -> Change:
        self.step += 1
        if self.step < len(self.changes):
            return self.changes[self.step]
        self.step = -1
        raise StopIteration
