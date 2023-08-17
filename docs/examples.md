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

This will create a new group named "sales_analytics" with the owner "Ralphie" and add all selected models to that group with the appropriate `access` configuration:
- create a new group definition in a `_groups.yml` file
![yml file with group defition](https://github.com/dave-connors-3/barnold-corp/assets/53586774/b3fa812a-157f-41b3-842d-c67e59f77298)
- add all selected models to that group with the appropriate `access` config
    - all models that are only referenced by other models in their _same group_ will have `access: private`
    ![int_sales__unioned access set to private](https://github.com/dave-connors-3/barnold-corp/assets/53586774/481010bb-ceed-4feb-a46e-05c185fac4e4)
    - all other models (those that are referenced by models _outside their group_ or are leaf nodes) will have `access: public`
    ![transactions access set to public](https://github.com/dave-connors-3/barnold-corp/assets/53586774/4c8665ac-d14c-424d-81e3-51c0bf12c701)

### Add/increment model versions

Let's say you want to add a new version to the customers model, which is currently un-versioned.
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

Let's say you want to add a new contract to the `stores` model, which is currently un-contracted.
![add a contract to the stores model](https://github.com/dave-connors-3/barnold-corp/assets/53586774/9eb48ce4-d6c2-4c79-a09f-0ff85cfccdcc)

You can run the following command:
```bash
dbt-meshify operation add-contract --select stores
```

This will add an enforced contract to the `stores` model:
- add a `contract` config and set `enforced: true`
![yml file updated with added contract config](https://github.com/dave-connors-3/barnold-corp/assets/53586774/bf1ba4e2-76a1-4a65-a0a9-7614487b7d6f)
- add every column's `name` and `data_type` if not already defined
![yml file updated with added column names and data_types](https://github.com/dave-connors-3/barnold-corp/assets/53586774/1d989396-2b07-48c5-bcf6-de7eaf02b928)

## Global commands

## Group together a subset of models

TO DO: add example here 

## Split out a new subproject

TO DO: add example here 

## Connect multiple dbt projects

TO DO: add example here 
