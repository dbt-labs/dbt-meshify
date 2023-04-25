from typing import Set

from dbt.contracts.graph.manifest import Manifest


def prune_manifest(manifest: Manifest, resources: Set[str]) -> Manifest:
    """
    Given a dbt Manifest and a set of resource selectors, prune the Manifest such that
    only the selected resources remain.
    """

    # manifest.nodes = {unique_id: node for unique_id, node in manifest.nodes if unique_id in resources}

    return manifest
