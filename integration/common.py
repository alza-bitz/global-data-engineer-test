"""
Common utilities and data generation strategies for integration tests.

This module provides shared functionality used across multiple integration tests
to avoid code duplication and maintain consistency.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any, List

import duckdb
from hypothesis import strategies as st
from dbt.cli.main import dbtRunner


# Test data generation strategies
@st.composite
def event_data(draw):
    """Generate valid event data using Hypothesis."""
    event_type = draw(st.sampled_from(["play", "pause", "seek", "complete"]))
    user_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))))
    episode_id = draw(st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"))))
    
    # Fix timezone issue with Hypothesis datetimes
    timestamp = draw(st.datetimes(
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2025, 8, 31)
    )).replace(tzinfo=timezone.utc).isoformat()
    
    # Duration is only present for play and complete events
    duration = None
    if event_type in ["play", "complete"]:
        duration = draw(st.integers(min_value=1, max_value=7200))  # Max 2 hours in seconds
    
    event = {
        "event_type": event_type,
        "user_id": user_id,
        "episode_id": episode_id,
        "timestamp": timestamp
    }
    
    if duration is not None:
        event["duration"] = duration
    
    return event


@st.composite
def strategy_with_files(draw, num_files: int = 1):
    """Generate events across multiple files with staging and loading subdirectories.
    
    Args:
        num_files: Number of separate JSON files to create
        
    Returns:
        dict with:
        - events: List of all events across all files
        - data_dir: Main temporary directory (contains staging and loading subdirs)
        - staging_dir: Staging subdirectory under data_dir
        - loading_dir: Loading subdirectory under data_dir
        - staging_paths: List of file paths in staging directory
    """
    
    # Create main temporary directory with staging and loading subdirectories
    data_dir = tempfile.mkdtemp()
    staging_dir = os.path.join(data_dir, "staging")
    loading_dir = os.path.join(data_dir, "loading")
    os.makedirs(staging_dir)
    os.makedirs(loading_dir)
    
    # Create the test data files in staging directory
    events = []
    staging_paths = []
    for i in range(num_files):
        staging_file = os.path.join(staging_dir, f"event_logs_{i:02d}.json")
        file_events = draw(st.lists(event_data(), min_size=1, max_size=10))
        with open(staging_file, "w") as f:
            for event in file_events:
                f.write(json.dumps(event) + "\n")
        events.extend(file_events)
        staging_paths.append(staging_file)
    
    # Return comprehensive test data info
    return {
        'events': events,
        'data_dir': data_dir,
        'staging_dir': staging_dir,
        'loading_dir': loading_dir,
        'staging_paths': staging_paths
    }


def run_dbt_model(model_name: str, vars_dict: Dict[str, Any] = None):
    """Run a specific dbt model with test configuration."""
    dbt_runner = dbtRunner()
    
    args = [
        "run",
        "--target", "test",
        "--select", model_name
    ]
    
    if vars_dict:
        # Format variables as valid YAML for dbt CLI
        import json
        vars_str = json.dumps(vars_dict)
        args.extend(["--vars", vars_str])
    
    result = dbt_runner.invoke(args)
    if not result.success:
        raise Exception(f"dbt run failed: {result.exception}")


def run_dbt_tests(model_name: str, vars_dict: Dict[str, Any] = None, exclude: str = None):
    """Run dbt tests for a specific model. Raises exception if any tests fail.
    
    Args:
        model_name: The full model name (e.g., "validated_events")
        vars_dict: Variables to pass to dbt
        exclude: Optional dbt selection syntax to exclude tests
    """
    dbt_runner = dbtRunner()
    
    args = [
        "test",
        "--target", "test",
        "--select", model_name
    ]
    
    if exclude:
        args.extend(["--exclude", exclude])
    
    if vars_dict:
        # Format variables as valid YAML for dbt CLI
        import json
        vars_str = json.dumps(vars_dict)
        args.extend(["--vars", vars_str])
    
    result = dbt_runner.invoke(args)
    if not result.success:
        raise Exception(f"dbt tests failed for {model_name}: {result.exception}")


def query_database(query: str, db_path: str):
    """Execute a query against the test database."""
    conn = duckdb.connect(db_path)
    try:
        result = conn.execute(query).fetchall()
        return result
    finally:
        conn.close()