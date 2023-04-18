import click
from .dbt_projects import LocalProjectHolder

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
    holder = LocalProjectHolder()
    while True:
        path = input("Enter the relative path to a dbt project (enter 'done' to finish): ")
        if path == "done":
            break
        holder.add_relative_project_path(path)