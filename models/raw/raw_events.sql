{{
  config(
    materialized='table'
  )
}}

-- Raw events model: Load event data from JSON file
-- This model loads all event data as strings initially
-- Validation will be applied in a separate transform step

select 
    event_type::varchar as event_type,
    user_id::varchar as user_id,
    episode_id::varchar as episode_id,
    timestamp::varchar as timestamp,
    duration::varchar as duration,
    current_timestamp::timestamp as load_at
from read_json_auto('{{ var("events_json_path") }}')
