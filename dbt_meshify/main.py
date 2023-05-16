from pathlib import Path
import click

from .dbt_projects import DbtProject, DbtSubProject, DbtProjectHolder

# define common parameters
project_path = click.option(
    "--project-path",
    type=click.Path(exists=True),
    default=".",
    help="The path to the dbt project to operate on. Defaults to the current directory."
)

exclude = click.option(
    "--exclude",
    "-e",
    default=None,
    help="The dbt selection syntax specifying the resources to exclude in the operation"
)

select = click.option(
    "--select",
    "-s",
    default=None,
    help="The dbt selection syntax specifying the resources to include in the operation"
)

selector = click.option(
    "--selector",
    default=None,
    help="The name of the YML selector specifying the resources to include in the operation"
)

# define cli group 
@click.group()
def cli():
    pass

@cli.command(name="add-contract")
@exclude
@project_path
@select
@selector
def add_contract(select, exclude, project_path, selector):
    """
    Adds a contract to all selected models.
    """
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(project.select_resources(select=select, exclude=exclude, output_key="unique_id"))
    models = filter(lambda x: x.startswith('model'), resources)
    for model_unique_id in models:
        project.add_model_contract(model_unique_id)

@cli.command(name="add-version")
@exclude
@project_path
@select
@selector
def add_version(select, exclude, project_path, selector):
    """
    Increments a model version on all selected models. Increments the version of the model if a version exists. 
    """
    pass

@cli.command(name="create-group")
@exclude
@project_path
@select
@selector
def create_group(select, exclude, project_path, selector):
    """
    Add selected resources to a group
    """
    pass

@cli.command(name="connect")
@click.argument("projects-dir", type=click.Path(exists=True), default=".")
def connect(projects_dir):
    """
    !!! info 
        This command is not yet implemented

    Connects multiple dbt projects together by adding all necessary dbt Mesh constructs
    """
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
@exclude
@project_path
@select
@selector
def split():
    """
    !!! info 
        This command is not yet implemented

    Splits dbt projects apart by adding all necessary dbt Mesh constructs based on the selection syntax. 

    """
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