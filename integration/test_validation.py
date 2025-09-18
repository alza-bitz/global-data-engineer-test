"""
Integration tests for the validation step of the podcast analytics pipeline.

These tests verify that the validated_events model correctly validates data 
and populates validation_errors according to the solution design requirements.
"""

import json
import os
import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
import duckdb
from hypothesis import given, settings, strategies as st

from .common import event_data, strategy_with_files, run_dbt_model, run_dbt_tests, query_database


@given(strategy_with_files(num_files=1))
@settings(max_examples=3, deadline=None)
def test_valid_events_have_empty_validation_errors(strategy):
    """
    Test the first validation integration test scenario:
    Given: All generated valid events loaded via the raw_events model
    When: The validated model is run
    Then: 
    - Assert that all validated dbt tests pass
    - Assert that validation_errors is an empty list for all rows
    """
    
    # Extract data from the strategy
    events = strategy['events']
    data_dir = strategy['data_dir']
    loading_dir = strategy['loading_dir']
    staging_paths = strategy['staging_paths']
    db_path = os.path.join(data_dir, 'podcast_analytics.duckdb')
    
    try:
        # Given: Load events using the raw_events model (part of test setup)
        # Copy the file from staging to loading directory
        shutil.copy2(staging_paths[0], loading_dir)
        
        # Run the raw_events model to populate the raw events table
        dbt_vars = {
            "events_json_path": os.path.join(loading_dir, "*.json"),
            "test_db_path": db_path
        }
        run_dbt_model("raw_events", dbt_vars)
        
        # When: Run the validation model
        run_dbt_model("validated_events", dbt_vars)
        
        # Then: Verify the results
        
        # 1. Assert that all validated dbt tests pass
        # Exclude tests that depend on downstream models (cleansed, analytics, etc.)
        run_dbt_tests("validated_events", dbt_vars, exclude="cleansed_events+")
                
        # 2. Assert that validation_errors is an empty array for all rows
        validation_errors_query = """
            SELECT validation_errors, 
                   array_length(validation_errors) as error_count,
                   event_type, user_id, episode_id, timestamp, duration
            FROM main_validated.validated_events
        """
        validation_results = query_database(validation_errors_query, db_path)
        
        for row in validation_results:
            validation_errors, error_count, event_type, user_id, episode_id, timestamp, duration = row
            assert error_count == 0, f"Expected no validation errors for valid event {event_type}, {user_id}, {episode_id}, but got: {validation_errors}"
            assert validation_errors == [], f"Expected empty validation_errors list for valid event, but got: {validation_errors}"
        
        # 4. Verify that all events have the expected structure (non-null required fields)
        structure_query = """
            SELECT COUNT(*) 
            FROM main_validated.validated_events 
            WHERE filename IS NOT NULL 
            AND load_at IS NOT NULL
        """
        structure_result = query_database(structure_query, db_path)
        assert structure_result[0][0] == len(events), f"Expected {len(events)} events with proper structure, got {structure_result[0][0]}"
        
    finally:
        # Clean up the temporary directory created for this example
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)