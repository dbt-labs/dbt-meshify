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

Additionally, `dbt-meshify` automates the code development required to split a monolithic dbt project into component projects, or connect multiple pre-existing dbt projects using cross-project `ref`.

## Installation

To install dbt-meshify, run:
```bash
pip install dbt-meshify
```

To upgrade dbt-meshify, run:
```bash
pip install --upgrade dbt-meshify
```