import click
from .dbt_projects import LocalProjectHolder, LocalDbtProject

@click.group()
def cli():
    pass


@cli.command(name="merge")
def merge():

    holder = LocalProjectHolder()
    while True:
        path = input("Enter the relative path to a dbt project (enter 'done' to finish): ")
        if path == "done":
            break
        holder.add_relative_project_path(path)
    
    print(holder.project_map())

@cli.command(name="split")
def split():
    path = input("Enter the relative path to a dbt project you'd like to split: ")
    project = LocalDbtProject(path)
    while True:
        subproject_selector = input("Enter the selector that represents the subproject you'd like to create (enter 'done' to finish): ")
        if subproject_selector == "done":
            break
        project.add_subproject_selector(subproject_selector)

    print(project.subprojects())
