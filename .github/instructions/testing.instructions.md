---
applyTo: '**'
---

# Integration Tests
- Must be written in python.
- Must be placed in the `integration` directory.
- Use pytest as the testing framework.
- Use hypothesis to generate data for test data files.
- Use the python dbt library for interacting with the dbt project and executing dbt commands.
- Use the duckdb python library for interacting with the duckdb database and making data assertions.
- Use a separate dbt profile, e.g. "test"
- Create a temporary directory for the test database and test data files inside the hypothesis strategy.
- The path of the database and test data files can be passed to dbt run using --vars.
- Use a try-finally block in each test to ensure deletion of the temporary directory at the end of each test.
- Use the built-in 'runTests' tool to execute the integration tests.