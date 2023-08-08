import os
import sys
from pathlib import Path
from typing import List, Optional

import click
import yaml
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.graph.unparsed import Owner
from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType, ResourceChange
from dbt_meshify.change_set_processor import ChangeSetProcessor
from dbt_meshify.storage.dbt_project_creator import DbtSubprojectCreator
from dbt_meshify.utilities.contractor import Contractor
from dbt_meshify.utilities.versioner import ModelVersioner

from .cli import (
    TupleCompatibleCommand,
    create_path,
    exclude,
    group_yml_path,
    owner,
    owner_email,
    owner_name,
    owner_properties,
    project_path,
    read_catalog,
    select,
    selector,
)
from .dbt_projects import DbtProject, DbtProjectHolder
from .exceptions import FatalMeshifyException

log_format = "<white>{time:HH:mm:ss}</white> | <level>{level}</level> | <level>{message}</level>"
logger.remove()  # Remove the default sink added by Loguru
logger.add(sys.stdout, format=log_format)


# define cli group
@click.group()
@click.option("--dry-run", is_flag=True)
def cli(dry_run: bool):
    pass


@cli.result_callback()
def handle_change_sets(change_sets=List[ChangeSet], dry_run=False):
    """Handle any resulting ChangeSets."""

    try:
        change_set_processor = ChangeSetProcessor(dry_run=dry_run)
        change_set_processor.process(change_sets)
    except Exception as e:
        raise FatalMeshifyException(
            f"Error evaluating the calculated change set for this operation. Change Sets: {change_sets}"
        ) from e


@cli.group()
def operation():
    """
    Set of subcommands for performing mesh operations on dbt projects
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
        project = DbtProject.from_directory(path, read_catalog)
        holder.register_project(project)

    print(holder.project_map())


@cli.command(
    cls=TupleCompatibleCommand,
    name="split",
)
@create_path
@click.argument("project_name")
@exclude
@project_path
@read_catalog
@select
@selector
@click.pass_context
def split(
    ctx, project_name, select, exclude, project_path, selector, create_path, read_catalog
) -> List[ChangeSet]:
    """
    Splits out a new subproject from a dbt project by adding all necessary dbt Mesh constructs to the
    resources based on the selected resources.
    """
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(
        path,
        read_catalog,
    )

    if create_path:
        create_path = Path(create_path).expanduser().resolve()
        create_path.parent.mkdir(parents=True, exist_ok=True)

    subproject = project.split(
        project_name=project_name,
        select=select,
        exclude=exclude,
        selector=selector,
        target_directory=create_path,
    )
    logger.info(f"Selected {len(subproject.resources)} resources: {subproject.resources}")

    subproject_creator = DbtSubprojectCreator(subproject=subproject, target_directory=create_path)
    logger.info(f"Creating subproject {subproject.name}...")
    try:
        change_set = subproject_creator.initialize()

        logger.success(f"Successfully created change set for subproject {subproject.name}")
        return [change_set]
    except Exception:
        raise FatalMeshifyException(f"Error creating subproject {subproject.name}")


@operation.command(name="add-contract")
@exclude
@project_path
@read_catalog
@select
@selector
def add_contract(
    select, exclude, project_path, selector, read_catalog, public_only=False
) -> List[ChangeSet]:
    """
    Adds a contract to all selected models.
    """

    path = Path(project_path).expanduser().resolve()
    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)
    resources = list(
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )

    logger.info(f"Selected {len(resources)} resources: {resources}")
    models = filter(lambda x: x.startswith("model"), resources)
    if public_only:
        models = filter(
            lambda x: project.get_manifest_node(x).access == "public", models  # type: ignore
        )

    logger.info("Adding contracts to models in selected resources...")

    change_set = ChangeSet()
    contractor = Contractor(project=project)

    try:
        for model_unique_id in models:
            model_node = project.get_manifest_node(model_unique_id)

            if not isinstance(model_node, ModelNode):
                continue

            change = contractor.generate_contract(model_node)
            change_set.add(change)

    except Exception:
        raise FatalMeshifyException(f"Error generating contract for model: {model_unique_id}")

    return [change_set]


@operation.command(name="add-version")
@exclude
@project_path
@read_catalog
@select
@selector
@click.option("--prerelease", "--pre", default=False, is_flag=True)
@click.option("--defined-in", default=None)
def add_version(
    select,
    exclude,
    project_path,
    selector,
    prerelease: bool,
    defined_in: Optional[Path],
    read_catalog,
) -> List[ChangeSet]:
    """
    Adds/increments model versions for all selected models.
    """
    path = Path(project_path).expanduser().resolve()

    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)
    resources = list(
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )
    models = filter(lambda x: x.startswith("model"), resources)
    logger.info(f"Selected {len(resources)} resources: {resources}")
    logger.info("Adding version to models in selected resources...")
    try:
        versioner = ModelVersioner(project=project)
        change_set = ChangeSet()
        for model_unique_id in models:
            model_node = project.get_manifest_node(model_unique_id)

            if not isinstance(model_node, ModelNode):
                continue

            if model_node.version != model_node.latest_version:
                continue

            changes: ChangeSet = versioner.generate_version(
                model=model_node, prerelease=prerelease, defined_in=defined_in
            )
            change_set.extend(changes)
        return [change_set]

    except Exception as e:
        raise FatalMeshifyException(f"Error adding version to model: {model_unique_id}") from e


@operation.command(
    name="create-group",
    cls=TupleCompatibleCommand,
)
@click.argument("name")
@exclude
@group_yml_path
@owner
@owner_email
@owner_name
@owner_properties
@project_path
@read_catalog
@select
@selector
def create_group(
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    read_catalog: bool,
    owner_name: Optional[str] = None,
    owner_email: Optional[str] = None,
    owner_properties: Optional[str] = None,
    exclude: Optional[str] = None,
    selector: Optional[str] = None,
) -> List[ChangeSet]:
    """
    Create a group and add selected resources to the group.
    """
    from dbt_meshify.utilities.grouper import ResourceGrouper

    path = Path(project_path).expanduser().resolve()
    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)

    if group_yml_path is None:
        group_yml_path = (path / Path("models/_groups.yml")).resolve()
    else:
        group_yml_path = Path(group_yml_path).resolve()
    logger.info(f"Creating new model group in file {group_yml_path.name}")

    if not str(os.path.commonpath([group_yml_path, path])) == str(path):
        raise FatalMeshifyException(
            "The provided group-yml-path is not contained within the provided dbt project."
        )

    group_owner: Owner = Owner(
        name=owner_name, email=owner_email, _extra=yaml.safe_load(owner_properties or "{}")
    )

    grouper = ResourceGrouper(project)
    try:
        changes: ChangeSet = grouper.add_group(
            name=name,
            owner=group_owner,
            select=select,
            exclude=exclude,
            selector=selector,
            path=group_yml_path,
            project_path=path,
        )
        return [changes]

    except Exception as e:
        logger.exception(e)
        raise FatalMeshifyException(f"Error creating group: {name}")


@cli.command(name="group", cls=TupleCompatibleCommand)
@click.argument("name")
@exclude
@group_yml_path
@owner
@owner_email
@owner_name
@owner_properties
@project_path
@read_catalog
@select
@selector
@click.pass_context
def group(
    ctx,
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    read_catalog: bool,
    owner_name: Optional[str] = None,
    owner_email: Optional[str] = None,
    owner_properties: Optional[str] = None,
    exclude: Optional[str] = None,
    selector: Optional[str] = None,
) -> List[ChangeSet]:
    """
    Creates a new dbt group based on the selection syntax
    Detects the edges of the group, makes their access public, and adds contracts to them
    """

    group_changes: List[ChangeSet] = ctx.forward(create_group)

    # Here's where things get a little weird. We only want to add contracts to projects
    # that have a public access configuration on the boundary of our group. But, we don't
    # actually want to update our manifest yet. This puts us between a rock and a hard place.
    # to work around this, we can "trust" that create_group will always return a change for
    # each public model, and rewrite our selection criteria to only select the models that
    # will be having a public access config.

    contract_changes = ctx.invoke(  # noqa: F841
        add_contract,
        select=" ".join(
            [
                change.identifier
                for change in group_changes[0]
                if isinstance(change, ResourceChange)
                and change.entity_type == EntityType.Model
                and change.data["access"] == "public"
            ]
        ),
        project_path=project_path,
    )

    return group_changes + contract_changes
