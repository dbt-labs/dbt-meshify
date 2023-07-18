import click
from loguru import logger


class FileEditorException(BaseException):
    """Exceptions relating to errors in file generation and loading."""


class ModelFileNotFoundError(BaseException):
    """Unable to identify a local file that a defines Model. This indicates that the Manifest may not be valid."""


class FatalMeshifyException(click.ClickException):
    """An unrecoverable error in Meshify."""

    def __init__(self, message):
        super().__init__(message)

    def show(self):
        logger.error(self.message)
        if self.__cause__ is not None:
            logger.exception(self.__cause__)
