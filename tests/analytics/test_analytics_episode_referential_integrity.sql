-- Test that all fact table episode_ids exist in dim_episodes
-- This validates referential integrity

select distinct f.episode_id
from {{ ref('fact_user_interactions') }} f
left join {{ ref('dim_episodes') }} e on f.episode_id = e.episode_id
where e.episode_id is null
