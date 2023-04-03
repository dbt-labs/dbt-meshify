import json 
import click

@click.group()
def cli():
    print("grace")

@cli.group()
def dbt_meshify():
    """Replicate job or env config from the source account to the destination account"""
    pass

@dbt_meshify.command(name="merge")
def merge_from_src():
    """Check for missing environments either in Source or Destination"""
    print("merge from source")

def main():
    print("Hello!")


if __name__ == '__main__':
    cli()