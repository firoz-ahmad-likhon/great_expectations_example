import pytest
import os
from pathlib import Path
import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

from init import GXInitiator


@pytest.fixture(autouse=True)
def gxi(tmp_path: Path) -> type[GXInitiator]:
    """Fixture to initialize a GX context for integration tests.

    This fixture sets up the necessary directories and initializes
    the Great Expectations context using `GXInitiator`.

    Args:
        tmp_path (Path): A temporary directory provided by pytest.

    Returns:
        The `GXInitiator` class.

    """
    # Setup: Point PROJECT_DIR and GX_DIR to a temporary directory
    GXInitiator.PROJECT_DIR = tmp_path / "quality"
    GXInitiator.GX_DIR = GXInitiator.PROJECT_DIR / "gx"

    # Initialize Great Expectations
    GXInitiator.initialize("init")

    return GXInitiator


@pytest.fixture(scope="class")
def context() -> AbstractDataContext:
    """Fixture to provide an AbstractDataContext for integration tests.

    This fixture initializes a file-based Great Expectations context.

    Returns: A Great Expectations data context.
    """
    project_root = os.path.join('./', "quality")
    return gx.get_context(mode="file", project_root_dir=project_root)


@pytest.fixture(scope="session")
def dimensions() -> list[str]:
    """List of dimensions for suites, validation definitions.

    returns: A list of Great Expectations suites.
    """
    return ["distribution", "missingness", "schema", "volume"]


@pytest.fixture(scope="session")
def checkpoints() -> list[str]:
    """List of available checkpoints.

    returns: A list of Great Expectations checkpoints.
    """
    return ["statistical_checkpoint", "completeness_checkpoint"]
