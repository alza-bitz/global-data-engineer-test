# First Slice Analysis: Top 10 Most Completed Episodes

## Overview
This analysis answers the question: **"What are the top 10 most completed episodes in the past 7 days?"**

## Usage

### Default Analysis (using 2024-01-07 as end date)
```bash
dbt run --select question_1_top_completed_episodes
```

### Custom Date Analysis
```bash
# Analyze completions in the 7 days ending 2024-01-05
dbt run --select question_1_top_completed_episodes --vars '{"analysis_end_date": "2024-01-05"}'
```

### Query Results
```sql
SELECT * FROM main.question_1_top_completed_episodes;
```

## Parameters
- `analysis_end_date`: The end date for the 7-day analysis window (defaults to '2024-01-07')
- Analysis covers the period: `[analysis_end_date - 7 days, analysis_end_date)`

## Output Schema
- `episode_id`: Unique identifier for the episode
- `title`: Episode title
- `podcast_id`: Podcast identifier
- `completion_count`: Number of times the episode was completed in the analysis period
- `release_date`: When the episode was originally released
- `duration_seconds`: Episode duration in seconds

## Data Sources
- `fact_user_interactions`: User interaction events (filtered for 'complete' events)
- `dim_episodes`: Episode metadata
