
with valid_events as (
  select *
  from {{ ref('raw_events_validated') }}
  where array_length(validation_errors) = 0
),

deduplicated_events as (
  select
    event_type,
    user_id,
    episode_id,
    timestamp,
    duration,
    load_at,
    filename,
    row_number() over (
      partition by user_id, episode_id, event_type, timestamp
      order by load_at desc
    ) as row_num
  from valid_events
)

select
  -- Convert event_type to the expected enum values (already validated in previous step)
  event_type::varchar as event_type,
  
  -- Ensure user_id and episode_id are not null (already validated)
  user_id::varchar as user_id,
  episode_id::varchar as episode_id,
  
  -- Convert timestamp from string to timestamp type
  timestamp::timestamp as timestamp,
  
  -- Convert duration to integer, set to null for non-play/complete events
  case 
    when event_type in ('play', 'complete') then duration::integer
    else null
  end as duration,
  
  -- Include load_at and filename for auditing
  load_at,
  filename

from deduplicated_events
where row_num = 1
order by load_at