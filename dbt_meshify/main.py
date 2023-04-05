import json 
import click

@click.group()
def cli():
    pass

@cli.command(name="merge")
def merge():
    """This will do something"""
    print("hey dave i solved it")