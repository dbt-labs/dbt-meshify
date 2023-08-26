with

upstream as (
    select * from {{ ref('shared_model') }}
),

upstream1 as (
    select * from {{ ref('new_model') }}
)

select 1 as id
