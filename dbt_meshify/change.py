import dataclasses
from enum import Enum
from typing import Dict, Set


class Operation(str, Enum):
    """An operation describes the type of work being performed."""

    create = "create"
    update = "update"
    move = "move"
    delete = "delete"


class EntityType(str, Enum):
    """An EntityType represents the type of entity being operated on in a Change"""

    project = "project"
    resource = "resource"
    file = "file"


@dataclasses.dataclass
class Change:
    """A change represents a unit of work that should be performed on a dbt project."""

    operation: Operation
    entity_type: EntityType
    identifier: str
    config: Dict
    data: Dict


class ChangeSet:
    """A collection of Changes that will be performed"""

    def __init__(self, changes: Set[Change]) -> None:
        self.changes = changes
