{{
  config(
    materialized='table'
  )
}}

-- Raw events model: Load event data from JSON file
-- This model loads all event data as strings initially and adds a validation_errors column
-- validation_errors will store an array of validation error messages in JSON format
-- Examples: null (valid), ["missing_user_id"], ["missing_user_id", "invalid_timestamp"]
-- Validation will be applied in a separate transform step

select 
    event_type::varchar as event_type,
    user_id::varchar as user_id,
    episode_id::varchar as episode_id,
    timestamp::varchar as timestamp,
    duration::varchar as duration,
    null::json as validation_errors  -- Will be populated in validation step (JSON array format)
from read_json_auto('{{ var("events_json_path") }}')
