with

upstream as (
    select * from {{ ref('shared_model') }}
)

select * from upstream
where colleague = 'grace'
