-- Test that all fact table user_ids exist in dim_users
-- This validates referential integrity

select distinct f.user_id
from {{ ref('fact_user_interactions') }} f
left join {{ ref('dim_users') }} u on f.user_id = u.user_id
where u.user_id is null
