with

source as (

    select * from {{ source('networkrail_data', 'movements') }}

)

, renamed_recasted as (

    select  
        event_type,
        actual_timestamp as actual_timestamp_utc,
        event_source,
        train_id,
        variation_status,
        toc_id
    from source

)

, final as (

    select 
        event_type, 
        actual_timestamp_utc, 
        event_source, 
        train_id, 
        variation_status,
        toc_id 
    from renamed_recasted
    

)

select * from final