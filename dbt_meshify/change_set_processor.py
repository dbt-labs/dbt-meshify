import os
from pathlib import Path
from typing import Iterable

from loguru import logger

from dbt_meshify.change import Change, ChangeSet, EntityType, Operation
from dbt_meshify.storage.file_content_editors import RawFileEditor, ResourceFileEditor

FILE_EDITORS = {
    EntityType.Code: RawFileEditor,
}

prepositions = {
    Operation.Add: "to",
    Operation.Move: "to",
    Operation.Copy: "to",
    Operation.Update: "in",
    Operation.Remove: "from",
}


class ChangeSetProcessorException(BaseException):
    def __init__(self, change: ChangeSet, exception: BaseException) -> None:
        self.change = change
        self.exception = exception
        super().__init__(f"Error processing change {self.change}")


class ChangeSetProcessor:
    def __init__(self, dry_run: bool = False) -> None:
        self.__dry_run = dry_run

    def write(self, change: Change) -> None:
        """Commit a Change to the file system."""
        file_editor = FILE_EDITORS.get(change.entity_type, ResourceFileEditor)(Path("."))

        logger.info(
            f"{change.operation.value.capitalize()} {change.entity_type.value} {change.identifier} in {change.path}"
        )
        file_editor.__getattribute__(change.operation)(change)

    def process(self, change_sets: Iterable[ChangeSet]) -> None:
        """
        Process an iterable of ChangeSets. This is the mechanism by which file modifications
        are orchestrated.
        """

        if self.__dry_run:
            print("\nProposed steps:\n")
            step_number = 1

        for change_set in change_sets:
            for change in change_set:
                logger.debug(change)
                logger.info(change)

                if self.__dry_run:
                    print(
                        f"{step_number}: {change.operation.value.capitalize()} {change.entity_type.value} "
                        f"`{change.identifier}` {prepositions[change.operation]} {change.path.relative_to(os.getcwd())}"
                    )
                    step_number += 1
                    continue
                try:
                    self.write(change)
                except Exception as e:
                    raise ChangeSetProcessorException(change=change, exception=e)
