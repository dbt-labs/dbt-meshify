# dbt-meshify

EXPERIMENTAL

maintained with :heart: by dbt practitioners for dbt practitioners

[Click here for full package documentation](https://dbt-labs.github.io/dbt-meshify/)

## Overview

`dbt-meshify` is a dbt-core plugin that automates the management and creation of dbt-core model governance features introduced in dbt-core v1.5

These dbt-core features include:

1. __[Groups](https://docs.getdbt.com/docs/build/groups)__ - group your models into logical sets.
2. __[Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)__ - add model contracts to your models to ensure consistent data shape.
3. __[Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)__ - control the `access` level of models within groups
4. __[Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)__ - create and increment versions of particular models.

## Installation

```bash
pip install dbt-meshify
```

## Basic Usage

```bash
# create a group of all models tagged with "finance"
# leaf nodes and nodes with cross-group dependencies will be `public`
# public nodes will also have contracts added to them
dbt-meshify group finance --owner-name "Monopoly Man" -s +tag:finance

# optionally use the add-version operation to add a new version to a model
dbt-meshify operation add-version -s fct_orders
```
