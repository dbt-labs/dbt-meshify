import shutil
from pathlib import Path


def setup_test_project(src_project_path, dest_project_path):
    src_path = Path(src_project_path)
    dest_path = Path(dest_project_path)
    shutil.copytree(src_path, dest_path)


def teardown_test_project(dest_project_path):
    dest_path = Path(dest_project_path)
    shutil.rmtree(dest_path)
