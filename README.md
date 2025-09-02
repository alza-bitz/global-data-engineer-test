# Podcast Analytics Data Pipeline

## Summary

This project implements an end-to-end data pipeline for processing podcast streaming interaction logs to enable analytics around user engagement and episode performance. The solution addresses the requirements outlined in the [problem statement](.github/instructions/problem_statement_and_requirements.instructions.md) and follows the architecture described in the [solution design](.github/instructions/solution_design.instructions.md). It uses the [ELT](https://wikipedia.org/wiki/Extract,_load,_transform) (Extract, Load, Transform) pattern with the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) (Bronze, Silver, Gold layers) implemented through dbt models.

The pipeline processes raw JSON event logs from podcast streaming interactions, validates and cleanses the data, and transforms it into an analytics-ready star schema to answer key business questions about podcast performance and user behavior.

## Features

### Data Pipeline Architecture
- **Medallion Architecture**: Bronze (raw), Silver (cleansed), Gold (analytics) data layers
- **ELT Pattern**: Extract and Load raw data, then Transform using dbt
- **Incremental Processing**: Handles new data efficiently with incremental models
- **Data Quality Validation**: Comprehensive validation rules with error tracking
- **Star Schema**: Optimized analytics model with fact and dimension tables

### Analytics Capabilities
- **Episode Performance**: Top completed episodes analysis
- **User Engagement**: Listen-through rates by country
- **User Behavior**: Multi-episode listening patterns
- **Temporal Analysis**: Time-based filtering for recent activity

### Data Sources
- **Event Logs**: JSON files containing user interaction events (play, pause, seek, complete)
- **User Reference Data**: CSV with user demographics and signup information
- **Episode Reference Data**: CSV with episode metadata and podcast information

### Supported Databases
- **DuckDB**: Primary target for local development and testing
- **Snowflake**: Production target (configured but not yet implemented)

## Prerequisites

### Required Software
- Python 3.8+
- dbt-core
- dbt-duckdb
- DuckDB
- Git

### Development Environment
This project is designed to run in a dev container with all dependencies pre-installed. The dev container includes:
- Python 3.12
- dbt-core and dbt-duckdb
- DuckDB CLI
- Testing frameworks (pytest, hypothesis)

### Required Python Packages
All dependencies are listed in `requirements.txt`:
- dbt-core
- dbt-duckdb
- duckdb
- pytest
- hypothesis

## Usage

### Initial Setup
1. Ensure you're in the project root directory
2. Install dependencies (if not using dev container):
   ```bash
   pip install -r requirements.txt
   ```

### Running the Complete Pipeline
Execute all pipeline steps in order:

1. **Load seed data (reference tables)**:
   ```bash
   dbt seed
   ```

2. **Run all models** (raw → validated → cleansed → analytics):
   ```bash
   dbt run
   ```

3. **Run data quality tests**:
   ```bash
   dbt test
   ```

### Running Specific Pipeline Stages

#### Bronze Layer (Raw Data)
```bash
# Load raw event data
dbt run --select raw_events

# Validate raw data
dbt run --select raw_events_validated
```

#### Silver Layer (Cleansed Data)
```bash
# Clean and normalize events
dbt run --select cleansed_events
```

#### Gold Layer (Analytics)
```bash
# Build analytics models
dbt run --select analytics
```

### Running Analysis Questions

#### Question 1: Top 10 Most Completed Episodes (Past 7 Days)
```bash
# Default analysis (using 2024-01-07 as end date)
dbt run --select question_1_top_completed_episodes

# Custom date analysis
dbt run --select question_1_top_completed_episodes --vars '{"analysis_end_date": "2024-01-05"}'

# View results
dbt show --select question_1_top_completed_episodes
```

#### Querying Results Directly
```sql
-- Connect to DuckDB and query results
SELECT * FROM main.question_1_top_completed_episodes;
```

### Data Refresh and Incremental Processing

#### Full Refresh (Rebuild All Models)
```bash
dbt run --full-refresh
```

#### Process Only New Data
```bash
dbt run  # Incremental models will automatically process only new data
```

### Adding New Event Data
1. Place new JSON event files in the `data/` directory
2. Run the pipeline to process new events:
   ```bash
   dbt run --select raw_events+  # Run raw_events and all downstream models
   ```

## Development

### Running Integration Tests
The project includes comprehensive integration tests using pytest and hypothesis for data generation:

```bash
# Run all integration tests
python -m pytest integration/ -v

# Run specific test
python -m pytest integration/test_extract_and_load.py -v
```

### Test Data Generation
Integration tests use hypothesis to generate realistic test data:
- Event logs in NDJSON format
- Temporary test databases
- Isolated test environments

### dbt Development Commands

#### Compile Models (Check SQL Syntax)
```bash
dbt compile
```

#### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

#### Check Data Lineage
```bash
dbt ls --select +question_1_top_completed_episodes  # Show upstream dependencies
dbt ls --select question_1_top_completed_episodes+  # Show downstream dependencies
```

### Database Management

#### Connect to DuckDB
```bash
duckdb podcast_analytics.duckdb
```

#### Inspect Data
```sql
-- Check raw data
SELECT COUNT(*) FROM main.raw_events;

-- Check validation results
SELECT validation_errors, COUNT(*) 
FROM main.raw_events_validated 
GROUP BY validation_errors;

-- Check analytics data
SELECT COUNT(*) FROM main.fact_user_interactions;
```

## Technical Architecture

For detailed technical information about the data models, quality framework, and architectural decisions, see the [Technical Architecture Documentation](docs/technical-architecture.md).

## Acknowledgements

This project was developed as a take-home technical assessment for a Senior Data Engineer position, implementing the requirements specified in the [problem statement](.github/instructions/problem_statement_and_requirements.instructions.md). The solution demonstrates:

- Modern data engineering patterns (ELT, Medallion Architecture)
- dbt best practices for data transformation and testing
- Comprehensive data quality validation
- Production-ready code structure and documentation
- Test-driven development with integration testing

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2025 Alex Coyle
