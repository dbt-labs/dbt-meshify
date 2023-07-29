from pathlib import Path
from typing import Iterable

from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType
from dbt_meshify.storage.file_content_editors import (
    CodeFileEditor,
    ProjectFileEditor,
    ResourceFileEditor,
)

FILE_EDITORS = {
    # TODO: Update with actual file editors
    EntityType.Project: ProjectFileEditor,
    EntityType.Code: CodeFileEditor,
}


class ChangeSetProcessor:
    def process(self, change_sets: Iterable[ChangeSet]) -> None:
        """
        Process an iterable of ChangeSets. This is the mechanism by which file modifications
        are orchestrated.
        """

        for change_set in change_sets:
            for change in change_set:
                # TODO: Remove project path once it's not necessary
                file_editor = FILE_EDITORS.get(change.entity_type, ResourceFileEditor)(Path("."))

                # TODO: Implement exception handling
                logger.info(f"{change.operation} {change.entity_type.value} {change.path}")
                file_editor.__getattribute__(change.operation)(change)
