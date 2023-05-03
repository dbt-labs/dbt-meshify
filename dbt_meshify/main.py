from pathlib import Path
import click

from dbt_meshify.dbt_meshify import DbtMeshModelConstructor

from .dbt_projects import DbtProject, DbtSubProject, DbtProjectHolder


@click.group()
def cli():
    pass


@cli.command(name="connect")
def connect():
    holder = DbtProjectHolder()

    while True:
        path_string = input("Enter the relative path to a dbt project (enter 'done' to finish): ")
        if path_string == "done":
            break

        path = Path(path_string).expanduser().resolve()
        project = DbtProject.from_directory(path)
        holder.register_project(project)

    print(holder.project_map())


@cli.command(name="split")
def split():
    path_string = input("Enter the relative path to a dbt project you'd like to split: ")

    holder = DbtProjectHolder()

    path = Path(path_string).expanduser().resolve()
    project = DbtProject.from_directory(path)
    holder.register_project(project)

    while True:
        subproject_name = input("Enter the name for your subproject ('done' to finish): ")
        if subproject_name == "done":
            break
        subproject_selector = input(
            f"Enter the selector that represents the subproject {subproject_name}: "
        )

        subproject: DbtSubProject = project.split(
            project_name=subproject_name, select=subproject_selector
        )
        holder.register_project(subproject)

    print(holder.project_map())


@cli.command(name="contract")
@click.option("--select", "-s")
@click.option("--exclude", "-e")
@click.option("--project-path", default=".")
def contract(select, exclude, project_path):
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(
        project.select_resources(select=select, exclude=exclude, output_key="unique_id")
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        model_catalog = project.get_catalog_entry(model_unique_id)
        meshify_constructor = DbtMeshModelConstructor(
            model_node=model_node, model_catalog=model_catalog, project_path=project_path
        )
        meshify_constructor.add_model_contract()

@cli.command(name="version")
@click.option("--select", "-s")
@click.option("--exclude", "-e")
@click.option("--project-path", default=".")
@click.option("--prerelease", "--pre", default=False, is_flag=True)
@click.option("--defined-in", default=None)
def version(select, exclude, project_path, prerelease, defined_in):
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(
        project.select_resources(select=select, exclude=exclude, output_key="unique_id")
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        if model_node.version == model_node.latest_version:
            meshify_constructor = DbtMeshModelConstructor(
                project_path=project_path, model_node=model_node
            )
            meshify_constructor.add_model_version(
                prerelease=prerelease, defined_in=defined_in
            )