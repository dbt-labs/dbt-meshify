# Examples

For consistency and clarity of the following examples, we're going to use a simplified dbt project. In practice, the model governance features describe are _most_ beneficial for large dbt projects that are struggling to scale. 

We will give a basic example for each command, but to see the full list of additional flags you can add to a given command, check out the [commands page](commands.md). 

!!! note
    One helpful flag that you can add to all of the commands is `--read-catalog`, which will skip the `dbt docs generate` step and instead read the local `catalog.json` file - this will speed up the time it takes to run the `dbt-meshify` commands but relies on your local `catalog.json` file being up-to-date. Alternatively, you can configure this via the `DBT_MESHIFY_READ_CATALOG` environment variable.

Let's imagine a dbt project with the following models:
![dbt dag of models](https://github.com/dave-connors-3/barnold-corp/assets/53586774/3775c540-ddc1-4eae-8587-8a0a9fb48c79)

You can checkout the source code for this example [here](https://github.com/dave-connors-3/mega-corp-big-co-inc/tree/dbt-meshify-docs).

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
    - all other models (those that are referenced by models _outside their group_ or are "leaf" models) will have `access: protected`
    ![transactions access set to protected](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/ad612ca7-2415-429f-aed8-108c4f16f9db)

### Add/increment model versions

Let's say you want to add a new version to the customers model, which is currently un-versioned. Versions can provide a smoother upgrade pathway when introducing breaking changes to models that have downstream dependencies.
![add a version to the customers model](https://github.com/dave-connors-3/barnold-corp/assets/53586774/e4097ca4-b6fa-4af4-b238-384a090573a7)

You can run the following command:
```bash
dbt-meshify operation add-version --select customers
```

This will add a version to the `customers` model:
- the `customers.sql` file will be renamed to `customers_v1.sql`
- the necessary version configurations will be created (or added to a pre-existing `yml` file)
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

Let's say you want to group together your sales analytics models - create a new group and add contracts to appropriate models simultaneously.
![group together sales analytics models](https://github.com/dave-connors-3/barnold-corp/assets/53586774/b192bf70-e854-46f6-be40-915eb48adbb3)

You can run the following command:
```bash
dbt-meshify group sales_analytics --owner-name Ralphie --select +int_sales__unioned +int_returns__unioned transactions
```

This will create a new group named "sales_analytics" with the owner "Ralphie", add all selected models to that group with the appropriate `access` configuration, _and add contracts to the models at the boundary between this group and the rest of the project__:
- create a new group definition in a `_groups.yml` file
![yml file with group defition](https://github.com/dave-connors-3/barnold-corp/assets/53586774/b3fa812a-157f-41b3-842d-c67e59f77298)
- add all selected models to that group with the appropriate `access` config
    - all models that are only referenced by other models in their _same group_ will have `access: private`
    ![int_sales__unioned access set to private](https://github.com/dave-connors-3/barnold-corp/assets/53586774/481010bb-ceed-4feb-a46e-05c185fac4e4)
    - all other models (those that are referenced by models _outside their group_ or are "leaf" models) will have `access: protected`
    ![transactions access set to protected](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/ad612ca7-2415-429f-aed8-108c4f16f9db)
- for all `protected` models:
    - add a `contract` config and set `enforced: true`
    ![yml file updated with added contract config for transactions model](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/747a7a25-d352-4913-95ed-4c6f72721bbb)
    - add every column's `name` and `data_type` if not already defined
    ![yml file updated with added column names and data_types for transactions model](https://github.com/dave-connors-3/barnold-corp/assets/53586774/f6402db9-95f0-4dc3-bc17-5966e79811a4)

## Split out a new subproject

Let's say you want to split our your sales analytics models into a new subproject.
![split sales analytics models into a new subproject](https://github.com/dave-connors-3/barnold-corp/assets/53586774/402a5637-800e-4945-b2e0-5271f2bf2c25)

You can run the following command:
```bash
dbt-meshify split sales_analytics --select +int_sales__unioned +int_returns__unioned transactions
```

This will create a new subproject that contains the selected sales analytics models, configure the "edge" models to be `public` and contracted, and replace all dependencies in the downstream project on the upstreams's models with cross-project `ref`s:
- create a new subproject that contains the selected sales analytics models
![selected models moved to a subproject](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/e638d83e-eb24-4f1e-852d-2c058bfedb4f)
- add a `dependencies.yml` to the _downstream_ project (in our case, our new subproject is downstream of the original project because the `transactions` model depends on some of the models that remain in the original project - `stores` and `customers`)
![add dependencies.yml](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/65e47b65-30ca-475f-bfa7-fffb26d85e11)
- add `access: public` to all "leaf" models (models with no downstream dependencies) and models in the upstream project that are referenced by models in the downstream project 
![customers access set to public](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/9e110ca4-40c5-4013-ab89-773b59638320)
- for all `public` models:
    - add a `contract` config and set `enforced: true`
    ![yml file updated with added contract config for stores model](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/800fc871-ce56-4e80-b746-8bd84aa05574)
    - add every column's `name` and `data_type` if not already defined
    ![yml file updated with added column names and data_types for stores model](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/48d41e17-0ad3-4a31-863b-1a8646d1d7c9)
- replace any dependencies in the downstream project on the upstream's models with a cross-project `ref`
![refs to customers and stores replaced with cross-project ref](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/33de63e1-0579-4ac0-9ff4-22099d701b99)

By default, the new subproject will be created in the current directory; however, you can use the `--create-path` flag to create it in any directory you like.

## Connect multiple dbt projects

Let's look at a slightly modified version of the example we've been working with. Instead of a single dbt project, let's imagine you're starting with two separate dbt projects connected via the "source hack":
- project A contains the following models
![project A's dag of models](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/75771c9c-1fa4-4cc5-b9b9-380f39091031)
- project B contains the following models
![project B's dag of models](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/a94657e5-c9bc-4b8b-ada5-63887bfd0ba3)

We call this type of multi-project configuration the "source hack" because there are models generated by project A (`stores` and `customers`) that are defined as sources in project B. 

Let's say we want to connect these two projects using model governance best practices and cross project `ref`s. 

You can run the following command:
```bash
dbt-meshify connect --project-paths path/to/project_a path/to/project_b
```

This will make the upstream project a dependency for the downstream project, configure the "edge" models to be `public` and contracted, and replace all dependencies in the downstream project on the upstreams's models with cross-project `ref`s:
- add a `dependencies.yml` to the _downstream_ project (in our case, project B is downstream of project A because the `transactions` model depends on some of the models that are generated by project A - `stores` and `customers`)
TO DO: ADD SCREENSHOT ONCE BUG IS FIXED
![dependencies.yml]()
- add `access: public` to all models in the upstream project that are referenced by models in the downstream project
![customers access set to public](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/9e110ca4-40c5-4013-ab89-773b59638320)
- for all `public` models:
    - add a `contract` config and set `enforced: true`
    ![yml file updated with added contract config for stores model](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/800fc871-ce56-4e80-b746-8bd84aa05574)
    - add every column's `name` and `data_type` if not already defined
    ![yml file updated with added column names and data_types for stores model](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/48d41e17-0ad3-4a31-863b-1a8646d1d7c9)
- replace any dependencies in the downstream project on the upstream's models with a cross-project `ref`
![customers and stores sources replaced with cross-project ref](https://github.com/dave-connors-3/mega-corp-big-co-inc/assets/53586774/24d72b99-fbf1-489d-bda8-ccaea267981b)
- remove unnecessary sources
TO DO: ADD SCREENSHOT
![unnecessary sources have been deleted]()