{{
  config(
    materialized='incremental'
  )
}}

-- Raw events model: apply transform: validation step
-- This model applies data quality checks to the raw events data and populates the validation_errors column

-- We need to populate the validation_errors column for new records since the last run, determined by load_at
-- with either an empty array (if no errors) or an array of error codes (if there are errors)


select
    event_type,
    user_id,
    episode_id,
    timestamp,
    duration,
    load_at,
    array_filter(array[
        case when user_id is null then 'missing_user_id' end,
        case when user_id is not null and trim(user_id) = '' then 'empty_user_id' end,
        case when episode_id is null then 'missing_episode_id' end,
        case when episode_id is not null and trim(episode_id) = '' then 'empty_episode_id' end,
        case when timestamp is null then 'missing_timestamp' end,
        case when timestamp is not null and try_cast(timestamp as timestamp) is null then 'invalid_timestamp' end,
        case when timestamp is not null and try_cast(timestamp as timestamp) < '2000-01-01'::timestamp then 'timestamp_too_early' end,
        case when timestamp is not null and try_cast(timestamp as timestamp) > current_timestamp then 'timestamp_in_future' end,
        case when event_type not in ('play', 'pause', 'seek', 'complete') then 'invalid_event_type' end,
        case when event_type in ('play', 'complete') and (duration is null or try_cast(duration as integer) is null or try_cast(duration as integer) <= 0) then 'invalid_duration_for_play_complete' end,
        case when event_type not in ('play', 'complete') and duration is not null then 'non_null_duration_for_other_event' end
    ], x -> x is not null)  -- remove nulls from array
    as validation_errors
from {{ ref('raw_events') }}

{% if is_incremental() %}
where load_at > (select max(load_at) from {{ this }})
{% endif %}