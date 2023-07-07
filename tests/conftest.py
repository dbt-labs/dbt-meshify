import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    """
    code taken from loguru docs site
    https://loguru.readthedocs.io/en/stable/resources/migration.html#replacing-caplog-fixture-from-pytest-library
    """
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)
