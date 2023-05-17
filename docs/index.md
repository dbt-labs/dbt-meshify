# dbt_meshify

`dbt-meshify` is a dbt-core plugin that automates the management and creation of dbt-core model governance features introduced in dbt-core v1.5. Each command in the package will leverage your dbt project metadata to create and/or edit the files in your project to properly configure the models in your project with these governance features.

These dbt-core features include:

1. __[Groups](https://docs.getdbt.com/docs/build/groups)__ - group your models into logical sets.
2. __[Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)__ - add model contracts to your models to ensure consistent data shape.
3. __[Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)__ - control the `access` level of models within groups
4. __[Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)__ - create and increment versions of particular models.

This package leverages the dbt-core Python API to allow users to use standard dbt selection syntax for each of the commands in this package (unless otherwise noted). See details on each of the specific commands available on the [commands page](commands.md)

## Basic Usage

Each of the available [commands page](commands.md) allows you to add one (or many) of the above features to a set of models specified by the selection syntax in the command.

The goal of this package is to make it more straightforward to apply to your project so that splitting apart a monolithic project into component projects is a more automated, dbt-tonic experience.

The process of splitting a dbt monolith apart roughly requires you to:

1. Determine what parts of your project should be grouped together into subprojects
2. Determine the access-level for the members of that group
3. Add model contracts to the elements that are public and accessed my members outside the group specified in (1)
4. (Optional) Add model versions to the public models to allow for development without impacting downstream stakeholders.

Here's how that might look for the process of creating a separate `finance` subproject in your dbt monolith.

```bash
# create a group of all models tagged with "finance"
# this command automatically detects models that should be public, and updates access levels
dbt-meshify add-group finance --owner name Monopoly Man -s +tag:finance

# create a contract to the publicly accessed nodes in that group
dbt-meshify add-contract --select group:finance,access:public

# optionally, add versions to your fct_payments model
dbt-meshify add-version --select fct_payments

```
Future releases of this package may also include features that allow users to fully split off groups of models into entirely new dbt projects.