{{ config(
    materialized='incremental',
    unique_key='interaction_id',
    on_schema_change='fail'
) }}

with cleansed_events as (
  select *
  from {{ ref('cleansed_events') }}
  {% if is_incremental() %}
    -- Only process new events based on timestamp
    -- For first implementation, we'll use a simple approach
    where timestamp > (select max(timestamp) from {{ this }})
  {% endif %}
),

numbered_events as (
  select
    -- Generate auto-incrementing interaction_id
    row_number() over (order by timestamp, user_id, episode_id, event_type) as interaction_id,
    user_id,
    episode_id,
    event_type,
    timestamp,
    duration
  from cleansed_events
)

select
  interaction_id,
  user_id,
  episode_id,
  event_type,
  timestamp,
  duration
from numbered_events
