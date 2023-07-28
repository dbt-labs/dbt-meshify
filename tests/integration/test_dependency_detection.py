from pathlib import Path

import pytest as pytest

from dbt_meshify.dbt import Dbt
from dbt_meshify.dbt_projects import DbtProject
from dbt_meshify.linker import Linker, ProjectDependency, ProjectDependencyType


class TestLinkerSourceDependencies:
    """Test how Linker computes dependencies via the source hack."""

    @pytest.fixture
    def src_proj_a(self) -> DbtProject:
        """Load the `src_proj_a` project."""
        return DbtProject.from_directory(
            Path("test-projects/source-hack/src_proj_a/").resolve(), read_catalog=False
        )

    @pytest.fixture
    def src_proj_b(self) -> DbtProject:
        """Load the `src_proj_b` project."""
        return DbtProject.from_directory(
            Path("test-projects/source-hack/src_proj_b/").resolve(), read_catalog=False
        )

    @pytest.fixture
    def dest_proj_a(self) -> DbtProject:
        """Load the `dest_proj_a` project."""
        path = Path("test-projects/source-hack/dest_proj_a/").resolve()

        # Run `dbt deps` for this project so upstream projects are loaded.
        dbt = Dbt()
        dbt.invoke(path, ["deps"])

        return DbtProject.from_directory(path, read_catalog=False)

    @pytest.fixture
    def dest_proj_b(self) -> DbtProject:
        """Load the `dest_proj_b` project."""
        return DbtProject.from_directory(
            Path("test-projects/source-hack/dest_proj_b/").resolve(), read_catalog=False
        )

    def test_linker_detects_source_dependencies(self, src_proj_a, src_proj_b):
        """Verify that Linker detects source-hacked projects."""

        linker = Linker()
        dependencies = linker.dependencies(src_proj_b, src_proj_a)

        assert dependencies == {
            ProjectDependency(
                upstream="model.src_proj_a.shared_model",
                downstream="source.src_proj_b.src_proj_a.shared_model",
                type=ProjectDependencyType.Source,
            )
        }

    def test_linker_detects_source_dependencies_bidirectionally(self, src_proj_a, src_proj_b):
        """Verify that Linker detects source-hacked projects when provided out of order."""

        linker = Linker()
        dependencies = linker.dependencies(src_proj_a, src_proj_b)

        assert dependencies == {
            ProjectDependency(
                upstream="model.src_proj_a.shared_model",
                downstream="source.src_proj_b.src_proj_a.shared_model",
                type=ProjectDependencyType.Source,
            )
        }

    def test_linker_detects_package_import_dependencies(self, src_proj_a, dest_proj_a):
        """Verify that Linker detects package import dependencies."""

        linker = Linker()
        dependencies = linker.dependencies(src_proj_a, dest_proj_a)

        assert dependencies == {
            ProjectDependency(
                upstream="model.src_proj_a.shared_model",
                downstream="model.src_proj_a.shared_model",
                type=ProjectDependencyType.Package,
            ),
            ProjectDependency(
                upstream="model.src_proj_a.new_model",
                downstream="model.src_proj_a.new_model",
                type=ProjectDependencyType.Package,
            ),
        }

    # This doesn't exist yet as of 1.5.0. We'll test it out once it's a thing.
    # def test_linker_detects_cross_project_reference_dependencies(self, src_proj_a, dest_proj_b):
    #     """Verify that Linker detects cross-project reference dependencies."""
    #
    #     linker = Linker()
    #     dependencies = linker.dependencies(src_proj_a, dest_proj_b)
    #
    #     assert dependencies == {
    #         ProjectDependency(
    #             upstream='source.src_proj_b.src_proj_a.shared_model',
    #             downstream='model.src_proj_a.shared_model',
    #             type=ProjectDependencyType.Reference
    #         )
    #     }
