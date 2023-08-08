
with

source as (

    select * from {{ source('ecom', 'raw_items') }}

),

renamed as (

    select

        ----------  ids
        id as order_item_id,
        order_id,

        ---------- properties
        sku as product_id
        -- {{ cents_to_dollars('any') }}

    from source

)

select * from renamed
