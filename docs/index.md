# dbt_meshify

`dbt-meshify` is a CLI tool that automates the creation of model governance and cross-project lineage features introduced in dbt-core v1.5 and v1.6. This package will leverage your dbt project metadata to create and/or edit the files in your project to properly configure the models in your project with these features.

These features include:

1. **[Groups](https://docs.getdbt.com/docs/build/groups)** - group your models into logical sets.
2. **[Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)** - add model contracts to your models to ensure consistent data shape.
3. **[Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)** - control the `access` level of models within groups
4. **[Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)** - create and increment versions of particular models.
5. **[Project dependencies](https://docs.getdbt.com/docs/collaborate/govern/project-dependencies)** - split a monolithic dbt project into component projects, or connect multiple pre-existing dbt projects using cross-project `ref`.

This package leverages the dbt-core Python API to allow users to use standard dbt selection syntax for each of the commands in this package (unless otherwise noted). See details on each of the specific commands available on the [commands page](commands.md).

## Getting Started

This package helps automate the code development required for adding the model governance and cross-project lineage features mentioned above.

The first question to ask yourself is "which of these features do I want to add to my project"? Do you want to add contracts, create a new group, split your monolithic dbt project in two? Your answer to this question will establish which `dbt-meshify` command is right for you!

This package consists of **component** and **global** commands - so you can decide how to best break apart your work.

The **component** commands allow you to do a single step at a time and begin with `dbt-meshify operation`. For example, if you wanted to add a new version to a model, you would run something like `dbt-meshify operation add-version --select fct_orders`. This command would:

1. add version configuration values to `fct_orders`

and that's it!

The **global** commands combine _multiple_ **component** commands to complete a larger set of work and begin with `dbt-meshify`. For example, if you wanted to define a group for a subset of your models, you would run something like `dbt-meshify group finance --owner-name "Monopoly Man" --select +tag:finance`. This command would:

1. define a new group named "finance" in your dbt project, setting the owner name to "Monopoly Man"
2. add all models tagged with "finance" to that new group
3. set `access` to protected for all "leaf" models (models with no downstream dependencies) and models with cross-group dependencies
4. add contracts to all protected nodes

all at once!

The next question to ask yourself is "which of my models do I want to add these features to?". This informs the selection syntax you provide to the `dbt-meshify` command of choice. `dbt-meshify` uses the same selection syntax as `dbt`, so you can use the `--select`, `--exclude` and `--selector` flags to select resources based on model names, tags, and so on!

Once you've decided:

1. which feature(s) you want to add to your dbt project
2. which subset of models you want to add those feature(s) to

you're ready to use `dbt-meshify`!

For further information, check out the available [commands](commands.md) or read through some [examples](examples.md).

## What dbt-meshify does not handle

There are a handful of known edge cases that this package does not automatically handle. In these cases, we recommend doing a manual check to make sure you've handled these appropriately:

| edge case                                                                                                                      | manual check                                                                                                                                                        |
| ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dbt-meshify split` copies over the entire contents of the `packages.yml` file from the original project to the new subproject | remove unnecessary packages from each project                                                                                                                       |
| `dbt-meshify split` makes a copy of all necessary macros from the original project to the new subproject                       | consider creating a private "macros only" project to install as a package into all of your other projects, instead of maintaining duplicate copies of shared macros |
