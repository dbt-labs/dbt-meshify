# Examples

For consistency and clarity of the following examples, we're going to use a simplified dbt project. In practice, the model governance features describe are _most_ beneficial for large dbt projects that are struggling to scale. 

We will give a basic example for each command, but to see the full list of additional flags you can add to a given command, check out the [commands page](commands.md). 

!!! note
    One helpful flag that you can add to all of the commands is `--read-catalog`, which will skip the `dbt docs generate` step and instead read the local `catalog.json` file - this will speed up the time it takes to run the `dbt-meshify` commands but relies on your local `catalog.json` file being up-to-date. 

Let's imagine a dbt project with the following models:
![dbt dag of models](https://github.com/dave-connors-3/barnold-corp/assets/53586774/3775c540-ddc1-4eae-8587-8a0a9fb48c79)

## Component commands

### Create a new group

Let's say you want to create a new group for your sales analytics models. 
![create a new group for sales analytics models](https://github.com/dave-connors-3/barnold-corp/assets/53586774/0f2b03a2-c5da-4e70-81c7-e83084ee9ba1)

You can run the following command:
```bash
dbt-meshify operation create-group sales_analytics --owner-name Ralphie --select +int_sales__unioned +int_returns__unioned transactions
```

This will create a new group named "sales_analytics" with the owner "Ralphie" and add all selected models to that group.

TO DO: once this command is working, add screenshots and descriptions of code changes

### Add/increment model versions

Let's say you want to add a new version to the customers model, which is currently unversioned.
![add a version to the customers model](https://github.com/dave-connors-3/barnold-corp/assets/53586774/e4097ca4-b6fa-4af4-b238-384a090573a7)

You can run the following command:
```bash
dbt-meshify operation add-version --select customers
```

This will add a version to the `customers` model:
- the `customers.sql` file will be renamed to `customers_v1.sql`
- the necessary version configurations will be created (or added to a pre-xisting `yml` file)
![yml file updated with version configs](https://github.com/dave-connors-3/barnold-corp/assets/53586774/c0b12ab7-904e-4590-84aa-7b602a91f53f)

### Add contract(s)

TO DO: add example here 

## Global commands

## Group together a subset of models

TO DO: add example here 

## Split out a new subproject

TO DO: add example here 

## Connect multiple dbt projects

TO DO: add example here 
