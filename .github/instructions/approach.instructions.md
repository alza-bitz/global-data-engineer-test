---
applyTo: '**'
---

We will work together to implement the solution end-to-end to answer each of the analysis questions in part 3 by "slice".

So, for each slice defined as follows:

- First slice: The top 10 most completed episodes in the past 7 days
- Second slice: Average listen-through rate (completion duration/episode duration) by country
- Third slice: Number of distinct users who listened to 3+ different episodes in one day

We shall implement enough of the ELT pipeline steps needed to answer the analytics question for that slice, in order:
1. Extract and load
2. Transform: validation
3. Transform: cleanse
4. Transform: analytics
5. Analysis questions as SQL queries

So we are iterating at two levels, the outer level by analytics question slice and the inner level by ELT pipeline step.

In each slice/step combination, we will work together to implement the code and tests needed, and I will provide feedback on the code you generate before we move on to the next step or slice.

We will complete each slice first for DuckDB and then for Snowflake, before moving on to the next slice.