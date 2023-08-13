from itertools import chain
from typing import Iterable

from loguru import logger

from dbt_meshify.change import Change, ChangeSet, EntityType
from dbt_meshify.storage.file_content_editors import RawFileEditor, ResourceFileEditor


class ChangeSetProcessorException(BaseException):
    def __init__(self, change: Change, exception: BaseException) -> None:
        self.change = change
        self.exception = exception
        super().__init__(f"Error processing change {self.change}")


class ChangeSetProcessor:
    """
    A ChangeSetProcessor iterates through ChangeSets and executes each Change
    """

    def __init__(self, dry_run: bool = False) -> None:
        self.__dry_run = dry_run

    def write(self, change: Change) -> None:
        """Commit a Change to the file system."""
        file_editor = (
            RawFileEditor() if change.entity_type == EntityType.Code else ResourceFileEditor()
        )

        file_editor.__getattribute__(change.operation)(change)

    def process(self, change_sets: Iterable[ChangeSet]) -> None:
        """
        Process an iterable of ChangeSets. This is the mechanism by which file modifications
        are orchestrated.
        """

        if self.__dry_run:
            logger.warning("Dry-run mode active. dbt-meshify will not modify any files.")

        for step, change in enumerate(chain.from_iterable(change_sets)):
            logger.info(f"{step + 1}: {str(change)}")

            if not self.__dry_run:
                continue

            try:
                self.write(change)
                logger.success(f"{step + 1}: {str(change)}")
            except Exception as e:
                raise ChangeSetProcessorException(change=change, exception=e)
