import json 
import click
import os
import subprocess

REPO_A = os.getenv("REPO_A", "test-projects/source-hack/src_proj_a")

@click.group()
def cli():
    pass

@cli.command(name="merge")
def merge():
    """This will navigate to repo_a, run dbt compile, load the manifest.json, and print the nodes"""

    print("First navigate to " + REPO_A)
    os.chdir(REPO_A)
    subprocess.run(["dbt", "compile"])
    repo_a_manifest = open(REPO_A + '/target/manifest.json')
    manifest_a = json.load(repo_a_manifest)

    print(manifest_a['nodes'])