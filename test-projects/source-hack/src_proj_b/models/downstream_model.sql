with

upstream as (
    select * from {{ source('src_proj_a', 'shared_model') }}
)

select * from upstream
where colleague = 'grace'