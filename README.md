# dbt-meshify

EXPERIMENTAL

maintained with :heart: by dbt practitioners for dbt practitioners

[Click here for full package documentation](https://dbt-labs.github.io/dbt-meshify/)

## Overview

`dbt-meshify` is a dbt-core plugin that automates the management and creation of dbt-core model governance features introduced in dbt-core v1.5

These dbt-core features include:

1. **[Groups](https://docs.getdbt.com/docs/build/groups)** - group your models into logical sets.
2. **[Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)** - add model contracts to your models to ensure consistent data shape.
3. **[Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)** - control the `access` level of models within groups
4. **[Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)** - create and increment versions of particular models.

Additionally, `dbt-meshify` automates the code development required to split a monolithic dbt project into component projects, or connect multiple pre-existing dbt projects using cross-project `ref`.

## Installation

To install dbt-meshify, run:
```bash
pip install dbt-meshify
```

To upgrade dbt-meshify, run:
```bash
<<<<<<< HEAD
pip install --upgrade dbt-meshify
```
=======
# create a group of all models tagged with "finance"
# leaf nodes and nodes with cross-group dependencies will be `protected`
# public and protected nodes will also have contracts added to them
dbt-meshify group finance --owner-name "Monopoly Man" -s +tag:finance

# optionally use the add-version operation to add a new version to a model
dbt-meshify operation add-version -s fct_orders
```
>>>>>>> main
