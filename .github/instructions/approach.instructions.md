---
applyTo: '**'
---

We will work together to implement the solution end-to-end by "slice", where the goal of each slice is to answer one of the analysis questions in part 3.

So for each slice, we shall implement enough of the ELT pipeline steps needed to answer the analytics question for that slice, in order:

1. Extract and load
2. Transform: validation
3. Transform: cleanse
4. Transform: analytics
5. Analysis questions as SQL queries

So we are iterating at two levels, the outer level by analytics question and the inner level by ELT pipeline step.

In each slice/step combination, we will work together to implement the code and tests needed, and I will provide feedback on any code you generate before we move on to the next step or slice.

We will complete each slice firstly for DuckDB and then secondly for Snowflake, before moving on to the next slice.