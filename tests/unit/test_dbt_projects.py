import shutil
from pathlib import Path

from dbt_meshify.dbt_projects import DbtProject

split_project_path = "test-projects/split/split_proj"


class TestDbtProject:
    def test_missing_packages(self):
        package_path = Path(split_project_path) / "dbt_packages"
        if package_path.exists():
            shutil.rmtree(package_path)
        project = DbtProject.from_directory(split_project_path, read_catalog=True)
        assert project.manifest is not None
