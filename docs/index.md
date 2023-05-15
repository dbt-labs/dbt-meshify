# dbt_project_meshify

`dbt-meshify` is a dbt-core plugin that automates the management and creation of dbt-core model governance features introduced in dbt-core v1.5. Each command in the package will leverage your dbt project metadata to create and/or edit the files in your project to properly configure the models in your project with these governance features. 

These dbt-core features include:

1. __[Groups](https://docs.getdbt.com/docs/build/groups)__ - group your models into logical sets.
2. __[Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)__ - add model contracts to your models to ensure consistent data shape.
3. __[Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)__ - control the `access` level of models within groups
4. __[Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)__ - create and increment versions of particular models.

This package leverages the dbt-core Python API to allow users to use standard dbt selection syntax for each of the commands in this package (unless otherwise noted). See details on each of the specific commands available on the [commands page](commands.md)

## Basic Usage

```bash
# create a group of all models tagged with "finance"
dbt-meshify add-group finance --owner name Monopoly Man -s +tag:finance

# create a contract to a model
dbt-meshify add-contract --select my_public_model

```
