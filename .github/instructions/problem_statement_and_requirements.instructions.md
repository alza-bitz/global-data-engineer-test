---
applyTo: '**'
---

## Scenario
You are part of the data engineering team at a company that provides a podcast streaming app with millions of users. Your task is to build a data pipeline that processes interaction logs to enable analytics around user engagement and episode performance. 

## Provided Files
You receive raw event data in the form of JSON logs with key fields including: 
- event_type: "play", "pause", "seek", "complete" 
- user_id 
- episode_id 
- timestamp 
- duration: only for "play" and "complete" events

You also have reference files: 
- users (user_id, signup_date, country)
- episodes (episode_id, podcast_id, episode_title, release_date, duration_seconds)

### Example Data
- [event_logs.json](../../data_example/event_logs_head.json) (sample of raw event logs)
- [users.csv](../../data_example/users.csv) (user reference data)
- [episodes.csv](../../data_example/episodes.csv) (episode reference data)

## Part 1: Data Modelling
Design a relational schema that: 
- Efficiently stores the user interaction data 
- Supports queries like “top episodes completed” or “avg session duration per user” 

Deliverable: 
- ERD or DDL statements (SQL or dbt schema.yml / models.sql structure) 

## Part 2: Data Pipeline
Design a batch pipeline to process the user event logs, transform them, and load them into a database to enable analysis. The pipeline should: 
- Read the raw json log files 
- Clean and normalise the events: 
  - Parse event_type (play, pause, seek, complete) 
  - Handle missing or malformed timestamps 
  - Filter out events with no user or episode ID 
- Include some data quality validation (e.g. null checks, timestamp range checks) 
- Populate the target schema you defined in Part 1

Deliverables: 
- Code to ingest the raw data (python) 
- Code that produces a cleaned, enriched event model 
  - Use dbt models if familiar, otherwise python scripts or notebooks 
- DAG (Airflow, Prefect, or pseudo-code) to run this pipeline on a schedule. 

## Part 3: Analysis
Write SQL for: 
- The top 10 most completed episodes in the past 7 days 
- Average listen-through rate (completion duration/episode duration) by country 
- Number of distinct users who listened to 3+ different episodes in one day 

## Submission Requirements
Git repo or zip with: 
- Code (python scripts and/or dbt models) 
- SQL queries 
- README with 
  - instructions to run your solution
  - notes on any assumptions or technical constraints that have influenced your solution e.g. use of something like duckdb rather than snowflake to allow local development 
- Optional: ERD diagram or pipeline diagram 