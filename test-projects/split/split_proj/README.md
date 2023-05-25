### splittable project

This project has a bit more oomph than the other test projects, and can be used to test commands related to teh `Mesh` class and related methos. 

#### setting up

the duckdb database for this project will be committed, but in the case that we need to reset this db, there are several "raw" sources in the `jaffle_shop` directory that the staging layer reads from. To load these, uncomment the `seed-paths` config and the `seeds` config block in the `dbt_project.yml` file, run a `dbt seed`, then revert the commented blocks. 