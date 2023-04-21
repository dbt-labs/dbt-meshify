import click
from .dbt_projects import LocalProjectHolder, LocalDbtProject

@click.group()
def cli():
    pass

@cli.command(name="connect")
def connect():

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
        subproject_name = input("Enter the name for your subproject ('done' to finish): ")
        if subproject_name == "done":
            break
        subproject_selector = input(f"Enter the selector that represents the subproject {subproject_name}: ")
        project.add_subproject({
            "name": subproject_name,
            "selector": subproject_selector
            })
    project.update_subprojects_with_resources()

    print(project.subprojects)
