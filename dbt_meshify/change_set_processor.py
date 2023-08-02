from pathlib import Path
from typing import Iterable

from loguru import logger

from dbt_meshify.change import Change, ChangeSet, EntityType
from dbt_meshify.storage.file_content_editors import (
    ProjectFileEditor,
    RawFileEditor,
    ResourceFileEditor,
)

FILE_EDITORS = {
    # TODO: Update with actual file editors
    EntityType.Project: ProjectFileEditor,
    EntityType.Code: RawFileEditor,
}


class ChangeSetProcessor:
    def __init__(self, dry_run: bool = False) -> None:
        self.__dry_run = dry_run

    def write(self, change: Change) -> None:
        """Commit a Change to the file system."""
        file_editor = FILE_EDITORS.get(change.entity_type, ResourceFileEditor)(Path("."))

        # TODO: Implement exception handling
        logger.info(
            f"{change.operation.value.capitalize()} {change.entity_type.value} {change.identifier} in {change.path}"
        )
        file_editor.__getattribute__(change.operation)(change)

    def process(self, change_sets: Iterable[ChangeSet]) -> None:
        """
        Process an iterable of ChangeSets. This is the mechanism by which file modifications
        are orchestrated.
        """

        for change_set in change_sets:
            for change in change_set:
                if self.__dry_run:
                    logger.info(
                        f"{change.operation.value.capitalize()} {change.entity_type.value} {change.path}"
                    )
                    continue

                self.write(change)
