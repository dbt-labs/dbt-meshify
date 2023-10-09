import os
import sys
from itertools import combinations
from pathlib import Path
from typing import List, Optional

import click
import yaml
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.graph.unparsed import Owner
from loguru import logger

from dbt_meshify.change import ChangeSet, EntityType, FileChange, ResourceChange
from dbt_meshify.change_set_processor import (
    ChangeSetProcessor,
    ChangeSetProcessorException,
)
from dbt_meshify.storage.dbt_project_editors import DbtSubprojectCreator
from dbt_meshify.utilities.contractor import Contractor
from dbt_meshify.utilities.linker import Linker
from dbt_meshify.utilities.versioner import ModelVersioner

from .cli import (
    TupleCompatibleCommand,
    create_path,
    exclude,
    exclude_projects,
    group_yml_path,
    latest,
    owner,
    owner_email,
    owner_name,
    owner_properties,
    project_path,
    project_paths,
    projects_dir,
    read_catalog,
    select,
    selector,
)
from .dbt_projects import DbtProject
from .exceptions import FatalMeshifyException

log_format = "<white>{time:HH:mm:ss}</white> | <level>{level}</level> | <level>{message}</level>"
operation_format = (
    "<white>{time:HH:mm:ss}</white> | [{extra[step]: >3}/{extra[steps]}] <level>{level: <8}</level> | "
    "<level>{message}</level>"
)

STARTING_LOG_LEVEL = logger.level("STARTING", no=24, color="<yellow>")
PLANNED_LOG_LEVEL = logger.level("PLANNED", no=23, color="<white>")
logger.remove()  # Remove the default sink added by Loguru


def logger_log_level_filter(record):
    """
    Filter for log records that have a level greater than or equal to the level stored in LOG_LEVEL, while filtering
    out all STARTED and SUCCESS levels.
    """
    return record["level"].no not in (
        PLANNED_LOG_LEVEL.no,
        STARTING_LOG_LEVEL.no,
        25,
    )


def logger_operation_level_filter(record):
    """
    Filter for log records that have a level of STARTED or SUCCESS.
    """
    return record["level"].no in (STARTING_LOG_LEVEL.no, PLANNED_LOG_LEVEL.no, 25)


# define cli group
@click.group()
@click.option("--dry-run", is_flag=True)
@click.option("--debug", is_flag=True)
def cli(dry_run: bool, debug: bool):
    log_level = "DEBUG" if debug else "INFO"

    logger.add(sys.stdout, format=log_format, filter=logger_log_level_filter, level=log_level)
    logger.add(sys.stdout, format=operation_format, filter=logger_operation_level_filter)


@cli.result_callback()
def handle_change_sets(change_sets=List[ChangeSet], dry_run=False, **kwargs):
    """Handle any resulting ChangeSets."""

    try:
        change_set_processor = ChangeSetProcessor(dry_run=dry_run)
        change_set_processor.process(change_sets)
    except ChangeSetProcessorException as e:
        logger.exception(e.exception)
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
@project_paths
@projects_dir
@exclude_projects
@read_catalog
def connect(
    project_paths: tuple, projects_dir: Path, exclude_projects: List[str], read_catalog: bool
) -> List[ChangeSet]:
    """
    Connects multiple dbt projects together by adding all necessary dbt Mesh constructs
    """
    if project_paths and projects_dir:
        raise click.BadOptionUsage(
            option_name="project_paths",
            message="Cannot specify both project_paths and projects_dir",
        )
    # 1. initialize all the projects supplied to the command
    # 2. compute the dependency graph between each combination of 2 projects in that set
    # 3. for each dependency, add the necessary dbt Mesh constructs to each project.
    #    This includes:
    #    - adding the dependency to the dependencies.yml file of the downstream project
    #    - adding contracts and public access to the upstream models
    #    - deleting the source definition of the upstream models in the downstream project
    #    - updating the `{{ source }}` macro in the downstream project to a {{ ref }} to the upstream project

    linker = Linker()
    if project_paths:
        dbt_projects = [
            DbtProject.from_directory(Path(project_path).resolve(), read_catalog)
            for project_path in project_paths
        ]

    if projects_dir:
        dbt_project_paths = [
            path.parent.resolve()
            for path in Path(projects_dir).glob("**/dbt_project.yml")
            if (
                "dbt_packages" not in path.parts
                and "site-packages" not in path.parts
                and path.is_file()
            )
        ]
        all_dbt_projects = [
            DbtProject.from_directory(project_path, read_catalog)
            for project_path in dbt_project_paths
        ]
        dbt_projects = [
            project for project in all_dbt_projects if project.name not in exclude_projects
        ]

    project_map = {project.name: project for project in dbt_projects}
    dbt_project_combinations = [combo for combo in combinations(dbt_projects, 2)]
    all_dependencies = set()
    for dbt_project_combo in dbt_project_combinations:
        dependencies = linker.dependencies(dbt_project_combo[0], dbt_project_combo[1])
        if len(dependencies) == 0:
            logger.info(
                f"No dependencies found between {dbt_project_combo[0].name} and {dbt_project_combo[1].name}"
            )
            continue

        noun = "dependency" if len(dependencies) == 1 else "dependencies"
        logger.info(
            f"Found {len(dependencies)} {noun} between {dbt_project_combo[0].name} and {dbt_project_combo[1].name}"
        )
        all_dependencies.update(dependencies)
    if len(all_dependencies) == 0:
        logger.warning("No dependencies found between any of the projects")
        return []

    noun = "dependency" if len(all_dependencies) == 1 else "dependencies"
    logger.info(f"Found {len(all_dependencies)} unique {noun} between all projects.")
    change_set = ChangeSet()
    for dependency in all_dependencies:
        logger.info(
            f"Resolving dependency between {dependency.upstream_resource} and {dependency.downstream_resource}"
        )
        try:
            changes = linker.resolve_dependency(
                dependency,
                project_map[dependency.upstream_project_name],
                project_map[dependency.downstream_project_name],
                change_set,
            )
            change_set.extend(changes)
        except Exception as e:
            logger.exception(e)
            raise FatalMeshifyException(f"Error resolving dependency : {dependency} {e}")

    return [change_set]


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

    if subproject.is_project_cycle:
        raise FatalMeshifyException(
            f"Cannot create subproject {project_name} from {project.name} because it would create a project "
            "dependency cycle. Try adding a `+` to your selection syntax to ensure all upstream resources are "
            "properly selected"
        )

    subproject_creator = DbtSubprojectCreator(project=subproject)
    logger.info(f"Creating subproject {subproject.name}...")
    try:
        change_set = subproject_creator.initialize()

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
@click.option("--defined-in", default=None)
def add_version(
    select,
    exclude,
    project_path,
    selector,
    defined_in: Optional[Path],
    read_catalog,
) -> List[ChangeSet]:
    """
    Adds model version boilerplate for all selected models.
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

    versioner = ModelVersioner(project=project)
    change_set = ChangeSet()
    for model_unique_id in models:
        try:
            model_node = project.get_manifest_node(model_unique_id)

            if not isinstance(model_node, ModelNode):
                continue

            if model_node.version != model_node.latest_version:
                continue

            changes: ChangeSet = versioner.add_version(model=model_node, defined_in=defined_in)
            change_set.extend(changes)

        except Exception as e:
            raise FatalMeshifyException(f"Error adding version to model: {model_unique_id}. {e}")

    return [change_set]


@operation.command(name="bump-version")
@exclude
@project_path
@read_catalog
@select
@selector
@latest
@click.option("--defined-in", default=None)
def bump_version(
    select,
    exclude,
    project_path,
    selector,
    latest: bool,
    defined_in: Optional[Path],
    read_catalog,
) -> List[ChangeSet]:
    """
    Create new model versions for all selected models.
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

    versioner = ModelVersioner(project=project)
    change_set = ChangeSet()
    for model_unique_id in models:
        try:
            model_node = project.get_manifest_node(model_unique_id)

            if not isinstance(model_node, ModelNode):
                continue

            if model_node.version != model_node.latest_version:
                continue

            changes: ChangeSet = versioner.bump_version(
                model=model_node, prerelease=not (latest), defined_in=defined_in
            )
            change_set.extend(changes)

        except Exception as e:
            raise FatalMeshifyException(f"Error adding version to model: {model_unique_id}. {e}")

    return [change_set]


@cli.command(name="version")
@exclude
@project_path
@read_catalog
@select
@selector
@latest
@click.option("--defined-in", default=None)
def version(
    select,
    exclude,
    project_path,
    selector,
    latest: bool,
    defined_in: Optional[Path],
    read_catalog,
) -> List[ChangeSet]:
    """
    Increment the models to the next version, and create in the initial version if it has not yet been defined.
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

    versioner = ModelVersioner(project=project)
    change_set = ChangeSet()
    for model_unique_id in models:
        try:
            model_node = project.get_manifest_node(model_unique_id)

            if not isinstance(model_node, ModelNode):
                continue

            if model_node.version != model_node.latest_version:
                continue

            if model_node.latest_version is not None:
                # We already have a version. Simply bump

                change_set.extend(
                    versioner.bump_version(
                        model=model_node,
                        defined_in=defined_in,
                        prerelease=not (latest),
                    )
                )

            else:
                # We don't have a version! This means we have to both add versions to the model, and then bump it!

                add_changes: ChangeSet = versioner.add_version(
                    model=model_node, defined_in=defined_in
                )

                model_add_change = add_changes[0]
                file_add_change = add_changes[1]

                if not isinstance(model_add_change, ResourceChange) or not isinstance(
                    file_add_change, FileChange
                ):
                    raise FatalMeshifyException(
                        f"Fault in change calculations for model {model_unique_id}"
                    )

                bump_change: ChangeSet = versioner.bump_version(
                    model=model_node,
                    defined_in=defined_in,
                    prerelease=not (latest),
                    model_override=model_add_change.data,
                )

                # Update the bump change for the model copy to reference the new model path
                file_bump_change = bump_change[1]
                if not isinstance(file_bump_change, FileChange):
                    raise FatalMeshifyException(
                        f"Fault in change calculations for model {model_unique_id}"
                    )
                file_bump_change.source = file_add_change.path
                bump_change[1] = file_bump_change

                # We don't need to update the model properties twice. Let's remove the first one form add_changes.
                del add_changes[0]

                change_set.extend(add_changes)
                change_set.extend(bump_change)

        except Exception as e:
            raise FatalMeshifyException(f"Error adding version to model: {model_unique_id}. {e}")

    return [change_set]


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
    Detects the edges of the group, makes their access protected, and adds contracts to them
    """

    group_changes: List[ChangeSet] = ctx.forward(create_group)

    # Here's where things get a little weird. We only want to add contracts to projects
    # that have a non-private access configuration on the boundary of our group. But, we don't
    # actually want to update our manifest yet. This puts us between a rock and a hard place.
    # to work around this, we can "trust" that create_group will always return a change for
    # each accessible model, and rewrite our selection criteria to only select the models that
    # will be having a non-private access config.

    contract_changes = ctx.invoke(  # noqa: F841
        add_contract,
        select=" ".join(
            [
                change.identifier
                for change in group_changes[0]
                if isinstance(change, ResourceChange)
                and change.entity_type == EntityType.Model
                and change.data["access"] != "private"
            ]
        ),
        project_path=project_path,
        read_catalog=read_catalog,
    )

    return group_changes + contract_changes
